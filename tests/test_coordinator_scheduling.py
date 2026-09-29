import sqlite3
import threading
from unittest.mock import Mock, patch

from graph_drawings import config
from graph_drawings.coordinator import CoordinatorState
from graph_drawings.status import init_schema, insert_job, set_status
from graph_drawings.worker import start_heartbeat_loop


def state():
    obj = CoordinatorState.__new__(CoordinatorState)
    obj.db = sqlite3.connect(':memory:')
    obj.db.row_factory = sqlite3.Row
    init_schema(obj.db)
    obj.lock = threading.RLock()
    return obj


def test_completion_with_pending_leased_done_and_other_stages():
    obj = state()
    insert_job(obj.db, stage_index=4, report_hash='a', work_hash='a', compact_blob=b'a')
    assert not obj.stage_complete(4)
    assert obj.stage_complete(3)
    assert not obj.global_done()
    obj.db.execute("UPDATE jobs SET status='leased', lease_expires_at=9999999999")
    assert not obj.stage_complete(4)
    assert not obj.global_done()
    obj.db.execute("UPDATE jobs SET status='done'")
    assert obj.stage_complete(4)
    assert obj.global_done()
    set_status(obj.db, 'reducing_stage', '5')
    assert not obj.global_done()
    obj.db.close()


def test_reduction_jobs_and_expired_leases_prevent_completion():
    obj = state()
    insert_job(obj.db, stage_index=4, report_hash='a', work_hash='a', compact_blob=b'a')
    obj.db.execute("UPDATE jobs SET status='leased', lease_expires_at=0")
    assert not obj.global_done()
    assert obj.db.execute('SELECT status FROM jobs').fetchone()[0] == 'pending'
    obj.db.execute("UPDATE jobs SET status='done'")
    obj.db.execute("INSERT INTO reduce_jobs(reduce_job_key,stage_index,bucket_hash,bucket_dir,status,created_at,updated_at) VALUES ('r',5,'b','b','pending',0,0)")
    assert not obj.reduce_stage_complete(5)
    assert not obj.global_done()
    obj.db.execute("UPDATE reduce_jobs SET status='leased', lease_expires_at=9999999999")
    assert not obj.reduce_stage_complete(5)
    obj.db.execute("UPDATE reduce_jobs SET status='done'")
    assert obj.reduce_stage_complete(5)
    assert obj.global_done()
    obj.db.close()


def test_close_batch_advances_only_after_last_outstanding_job():
    obj = state()
    for name in ('a','b'):
        insert_job(obj.db, stage_index=4, report_hash=name, work_hash=name, compact_blob=b'x')
    with patch.object(obj, 'reduce_and_advance') as advance:
        obj.close_jobs({'jobs':[{'job_key':'4:a','stage_index':4}]})
        advance.assert_not_called()
        obj.close_jobs({'jobs':[{'job_key':'4:b','stage_index':4}]})
        advance.assert_called_once_with(4)
    obj.db.close()


def test_periodic_heartbeat_waits_and_renews_current_buffer():
    stop = Mock()
    stop.wait.side_effect = [False, True]
    shutdown = Mock()
    client = Mock()
    client.heartbeat.return_value = {}
    keys = ['4:a','4:b']
    with patch('graph_drawings.worker.threading.Event', side_effect=[stop,shutdown]), \
         patch('graph_drawings.worker.threading.Thread') as thread:
        start_heartbeat_loop(client, '4:a', {}, keys)
        thread.call_args.kwargs['target']()
    assert config.WORKER_HEARTBEAT_SECONDS == 60
    assert config.WORKER_HEARTBEAT_SECONDS * 3 < config.LEASE_SECONDS
    assert all(call.args == (60,) for call in stop.wait.call_args_list)
    client.heartbeat.assert_called_once_with('4:a', {}, current_job_keys=keys)
