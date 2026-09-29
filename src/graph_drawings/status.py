"""SQLite schema and status helpers for compact graph-drawings runs."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_key TEXT PRIMARY KEY,
    stage_index INTEGER NOT NULL,
    report_hash TEXT NOT NULL,
    work_hash TEXT NOT NULL,
    compact_blob BLOB NOT NULL,
    status TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_stage_created
ON jobs(status, stage_index, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_status_lease
ON jobs(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_jobs_stage_work
ON jobs(stage_index, work_hash);

CREATE TABLE IF NOT EXISTS canonical_drawings (
    stage_index INTEGER NOT NULL,
    report_hash TEXT NOT NULL,
    work_hash TEXT NOT NULL,
    compact_blob BLOB,
    source_job_key TEXT,
    source_worker TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    bucket_hash TEXT,
    shard_path TEXT,
    shard_ordinal INTEGER,
    PRIMARY KEY(stage_index, report_hash)
);

CREATE INDEX IF NOT EXISTS idx_canonical_drawings_stage
ON canonical_drawings(stage_index);

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    last_seen_at REAL NOT NULL,
    current_job_key TEXT,
    jobs_done INTEGER NOT NULL DEFAULT 0,
    candidates_seen INTEGER NOT NULL DEFAULT 0,
    candidates_accepted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    event_type TEXT NOT NULL,
    worker_id TEXT,
    job_key TEXT,
    message TEXT,
    payload_json TEXT
);

CREATE TABLE IF NOT EXISTS operation_timings (
    operation TEXT PRIMARY KEY,
    request_count INTEGER NOT NULL DEFAULT 0,
    total_seconds REAL NOT NULL DEFAULT 0.0,
    max_seconds REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS run_status (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shard_manifests (
    shard_path TEXT PRIMARY KEY,
    stage_index INTEGER NOT NULL,
    bucket_hash TEXT NOT NULL,
    kind TEXT NOT NULL,
    source_job_key TEXT,
    source_worker TEXT,
    record_count INTEGER NOT NULL,
    byte_count INTEGER NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shard_manifests_stage_kind
ON shard_manifests(stage_index, kind);
CREATE INDEX IF NOT EXISTS idx_shard_manifests_stage_bucket
ON shard_manifests(stage_index, bucket_hash);

CREATE TABLE IF NOT EXISTS reduce_jobs (
    reduce_job_key TEXT PRIMARY KEY,
    stage_index INTEGER NOT NULL,
    bucket_hash TEXT NOT NULL,
    bucket_dir TEXT NOT NULL,
    status TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reduce_jobs_status_stage_created
ON reduce_jobs(status, stage_index, created_at);
CREATE INDEX IF NOT EXISTS idx_reduce_jobs_status_lease
ON reduce_jobs(status, lease_expires_at);

CREATE TABLE IF NOT EXISTS worker_controls (
    worker_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    reason TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
"""


def connect_db(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path, check_same_thread=False, timeout=60)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=60000")
    return db


def init_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def ensure_runtime_schema(db: sqlite3.Connection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS worker_controls (
            worker_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            reason TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    db.commit()


def set_status(db: sqlite3.Connection, key: str, value: str) -> None:
    db.execute(
        """
        INSERT INTO run_status(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )
    db.commit()


def get_status(db: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = db.execute("SELECT value FROM run_status WHERE key=?", (key,)).fetchone()
    return default if row is None else row["value"]


def insert_job(
    db: sqlite3.Connection,
    *,
    stage_index: int,
    report_hash: str,
    work_hash: str,
    compact_blob: bytes,
    now: float | None = None,
) -> bool:
    now = time.time() if now is None else now
    job_key = f"{stage_index}:{work_hash}"
    cur = db.execute(
        """
        INSERT OR IGNORE INTO jobs(
            job_key, stage_index, report_hash, work_hash, compact_blob, status,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """,
        (job_key, stage_index, report_hash, work_hash, compact_blob, now, now),
    )
    return cur.rowcount == 1


def insert_reported_class(
    db: sqlite3.Connection,
    *,
    stage_index: int,
    report_hash: str,
    work_hash: str,
    compact_blob: bytes,
    source_job_key: str | None,
    source_worker: str | None,
    now: float | None = None,
) -> bool:
    now = time.time() if now is None else now
    row = db.execute(
        """
        SELECT compact_blob
        FROM canonical_drawings
        WHERE stage_index=? AND report_hash=?
        """,
        (stage_index, report_hash),
    ).fetchone()
    if row is None:
        db.execute(
            """
            INSERT INTO canonical_drawings(
                stage_index, report_hash, work_hash, compact_blob, source_job_key,
                source_worker, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (stage_index, report_hash, work_hash, compact_blob, source_job_key, source_worker, now, now),
        )
        return True
    if row["compact_blob"] is None or compact_blob < row["compact_blob"]:
        db.execute(
            """
            UPDATE canonical_drawings
            SET compact_blob=?, work_hash=?, source_job_key=?, source_worker=?, updated_at=?
            WHERE stage_index=? AND report_hash=?
            """,
            (compact_blob, work_hash, source_job_key, source_worker, now, stage_index, report_hash),
        )
    return False


def insert_compact_job(
    db: sqlite3.Connection,
    *,
    stage_index: int,
    report_hash: str,
    work_hash: str,
    compact_blob: bytes,
    now: float | None = None,
) -> bool:
    return insert_job(
        db,
        stage_index=stage_index,
        report_hash=report_hash,
        work_hash=work_hash,
        compact_blob=compact_blob,
        now=now,
    )


def insert_representative_index(
    db: sqlite3.Connection,
    *,
    stage_index: int,
    report_hash: str,
    work_hash: str,
    bucket_hash: str,
    shard_path: str,
    shard_ordinal: int,
    source_job_key: str | None = None,
    source_worker: str | None = None,
    now: float | None = None,
) -> bool:
    now = time.time() if now is None else now
    cur = db.execute(
        """
        INSERT OR IGNORE INTO canonical_drawings(
            stage_index, report_hash, work_hash, source_job_key,
            source_worker, created_at, updated_at, bucket_hash, shard_path, shard_ordinal
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            stage_index,
            report_hash,
            work_hash,
            source_job_key,
            source_worker,
            now,
            now,
            bucket_hash,
            shard_path,
            shard_ordinal,
        ),
    )
    return cur.rowcount == 1


def insert_shard_manifest(
    db: sqlite3.Connection,
    *,
    shard_path: str,
    stage_index: int,
    bucket_hash: str,
    kind: str,
    source_job_key: str | None,
    source_worker: str | None,
    record_count: int,
    byte_count: int,
    now: float | None = None,
) -> bool:
    now = time.time() if now is None else now
    cur = db.execute(
        """
        INSERT OR REPLACE INTO shard_manifests(
            shard_path, stage_index, bucket_hash, kind, source_job_key,
            source_worker, record_count, byte_count, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            shard_path,
            stage_index,
            bucket_hash,
            kind,
            source_job_key,
            source_worker,
            record_count,
            byte_count,
            now,
        ),
    )
    return cur.rowcount == 1


def insert_reduce_job(
    db: sqlite3.Connection,
    *,
    stage_index: int,
    bucket_hash: str,
    bucket_dir: str,
    now: float | None = None,
) -> bool:
    now = time.time() if now is None else now
    reduce_job_key = f"reduce:{stage_index}:{bucket_hash}:{bucket_dir}"
    cur = db.execute(
        """
        INSERT OR IGNORE INTO reduce_jobs(
            reduce_job_key, stage_index, bucket_hash, bucket_dir, status,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, 'pending', ?, ?)
        """,
        (reduce_job_key, stage_index, bucket_hash, bucket_dir, now, now),
    )
    return cur.rowcount == 1


def mark_job(db: sqlite3.Connection, job_key: str, status: str) -> None:
    db.execute(
        """
        UPDATE jobs
        SET status=?, lease_owner=NULL, lease_expires_at=NULL, updated_at=?
        WHERE job_key=?
        """,
        (status, time.time(), job_key),
    )


def mark_reduce_job(db: sqlite3.Connection, reduce_job_key: str, status: str) -> None:
    db.execute(
        """
        UPDATE reduce_jobs
        SET status=?, lease_owner=NULL, lease_expires_at=NULL, updated_at=?
        WHERE reduce_job_key=?
        """,
        (status, time.time(), reduce_job_key),
    )


def expire_leases(db: sqlite3.Connection, now: float | None = None) -> int:
    now = time.time() if now is None else now
    cur = db.execute(
        """
        UPDATE jobs
        SET status='pending', lease_owner=NULL, lease_expires_at=NULL, updated_at=?
        WHERE status='leased' AND lease_expires_at < ?
        """,
        (now, now),
    )
    return cur.rowcount


def expire_reduce_leases(db: sqlite3.Connection, now: float | None = None) -> int:
    now = time.time() if now is None else now
    cur = db.execute(
        """
        UPDATE reduce_jobs
        SET status='pending', lease_owner=NULL, lease_expires_at=NULL, updated_at=?
        WHERE status='leased' AND lease_expires_at < ?
        """,
        (now, now),
    )
    return cur.rowcount


def summarize_db(db: sqlite3.Connection, active_worker_seconds: int = 120, options: dict | None = None) -> dict:
    from . import config

    options = options or {}
    include_job_counts = options.get("include_job_counts", True)
    include_stage_counts = options.get("include_stage_counts", True)
    include_worker_heartbeats = options.get("include_worker_heartbeats", True)
    include_shards = options.get("include_shards", True)
    include_operation_timings = options.get("include_operation_timings", True)
    operation_timing_limit = int(options.get("operation_timing_limit", config.INSPECT_DB_OPERATION_TIMING_LIMIT))
    worker_heartbeat_limit = int(options.get("worker_heartbeat_limit", config.INSPECT_DB_WORKER_HEARTBEAT_LIMIT))
    worker_heartbeat_fields = list(options.get("worker_heartbeat_fields", config.INSPECT_DB_WORKER_HEARTBEAT_FIELDS))
    operation_timing_fields = list(options.get("operation_timing_fields", config.INSPECT_DB_OPERATION_TIMING_FIELDS))
    now = time.time()
    summary = {
        "active_stage": get_status(db, "active_stage", "0"),
        "reducing_stage": get_status(db, "reducing_stage", ""),
        "schema": "compact_sharded",
    }
    if include_job_counts:
        summary["extension_jobs"] = {
            row["status"]: row["count"]
            for row in db.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")
        }
        summary["reduce_jobs"] = {
            row["status"]: row["count"]
            for row in db.execute("SELECT status, COUNT(*) AS count FROM reduce_jobs GROUP BY status")
        }
        summary["jobs"] = {
            status: summary["extension_jobs"].get(status, 0) + summary["reduce_jobs"].get(status, 0)
            for status in set(summary["extension_jobs"]) | set(summary["reduce_jobs"])
        }
        summary["expired_leases"] = {
            "extension_jobs": db.execute(
                "SELECT COUNT(*) AS count FROM jobs WHERE status='leased' AND lease_expires_at < ?",
                (now,),
            ).fetchone()["count"],
            "reduce_jobs": db.execute(
                "SELECT COUNT(*) AS count FROM reduce_jobs WHERE status='leased' AND lease_expires_at < ?",
                (now,),
            ).fetchone()["count"],
        }
    if include_stage_counts:
        reported_by_stage = {
            row["stage_index"]: row["count"]
            for row in db.execute("SELECT stage_index, COUNT(*) AS count FROM canonical_drawings GROUP BY stage_index")
        }
        work_by_stage = {
            row["stage_index"]: row["count"]
            for row in db.execute("SELECT stage_index, COUNT(*) AS count FROM jobs GROUP BY stage_index")
        }
        reduce_by_stage = {
            row["stage_index"]: row["count"]
            for row in db.execute("SELECT stage_index, COUNT(*) AS count FROM reduce_jobs GROUP BY stage_index")
        }
        summary.update(
            {
                "canonical_drawings_by_stage": reported_by_stage,
                "reported_classes_by_stage": reported_by_stage,
                "work_contexts_by_stage": work_by_stage,
                "reduce_jobs_by_stage": reduce_by_stage,
            }
        )
    active_cutoff = now - active_worker_seconds
    summary["active_workers"] = db.execute(
        "SELECT COUNT(*) AS count FROM worker_heartbeats WHERE last_seen_at >= ?",
        (active_cutoff,),
    ).fetchone()["count"]
    if include_worker_heartbeats:
        worker_rows = [
            {
                "worker_id": row["worker_id"],
                "hostname": row["hostname"],
                "current_job_key": row["current_job_key"],
                "seconds_since_last_seen": round(now - row["last_seen_at"], 1),
                "extension_jobs_done": row["extension_jobs_done"],
                "reduce_jobs_done": row["reduce_jobs_done"],
                "jobs_done": row["jobs_done"],
                "candidates_seen": row["candidates_seen"],
                "candidates_accepted": row["candidates_accepted"],
                "control_action": row["control_action"],
            }
            for row in db.execute(
                """
                SELECT h.worker_id, h.hostname, h.last_seen_at, h.current_job_key,
                       h.jobs_done, h.candidates_seen, h.candidates_accepted,
                       c.action AS control_action,
                       (
                         SELECT COUNT(*) FROM events e
                         WHERE e.worker_id=h.worker_id AND e.event_type='done'
                       ) AS extension_jobs_done,
                       (
                         SELECT COUNT(*) FROM events e
                         WHERE e.worker_id=h.worker_id AND e.event_type='reduce_done'
                       ) AS reduce_jobs_done
                FROM worker_heartbeats h
                LEFT JOIN worker_controls c USING(worker_id)
                ORDER BY h.last_seen_at DESC
                LIMIT ?
                """,
                (worker_heartbeat_limit,),
            )
        ]
        summary["worker_heartbeats"] = [
            {field: row[field] for field in worker_heartbeat_fields if field in row}
            for row in worker_rows
        ]
    if include_shards:
        summary["shards"] = {}
        for row in db.execute(
            """
            SELECT kind, COUNT(*) AS count,
                   SUM(byte_count) AS bytes,
                   SUM(record_count) AS records,
                   MIN(record_count) AS min_records_per_shard,
                   MAX(record_count) AS max_records_per_shard,
                   AVG(record_count) AS avg_records_per_shard,
                   MIN(byte_count) AS min_bytes_per_shard,
                   MAX(byte_count) AS max_bytes_per_shard,
                   AVG(byte_count) AS avg_bytes_per_shard,
                   COUNT(DISTINCT source_job_key) AS source_jobs
            FROM shard_manifests
            GROUP BY kind
            """
        ):
            count = int(row["count"] or 0)
            records = int(row["records"] or 0)
            bytes_ = int(row["bytes"] or 0)
            source_jobs = int(row["source_jobs"] or 0)
            summary["shards"][row["kind"]] = {
                "count": count,
                "bytes": bytes_,
                "records": records,
                "records_per_shard": records / count if count else None,
                "bytes_per_shard": bytes_ / count if count else None,
                "bytes_per_record": bytes_ / records if records else None,
                "min_records_per_shard": row["min_records_per_shard"],
                "max_records_per_shard": row["max_records_per_shard"],
                "avg_records_per_shard": row["avg_records_per_shard"],
                "min_bytes_per_shard": row["min_bytes_per_shard"],
                "max_bytes_per_shard": row["max_bytes_per_shard"],
                "avg_bytes_per_shard": row["avg_bytes_per_shard"],
                "source_jobs": source_jobs,
                "shards_per_source_job": count / source_jobs if source_jobs else None,
            }
    if include_operation_timings:
        timing_rows = [
            dict(row)
            for row in db.execute(
                """
                SELECT operation, request_count, total_seconds, max_seconds
                FROM operation_timings
                ORDER BY total_seconds DESC
                LIMIT ?
                """,
                (operation_timing_limit,),
            )
        ]
        summary["operation_timings"] = [
            {field: row[field] for field in operation_timing_fields if field in row}
            for row in timing_rows
        ]
    return summary


def final_run_summary(db: sqlite3.Connection) -> dict:
    timings = [
        dict(row)
        for row in db.execute(
            """
            SELECT operation, request_count, total_seconds, max_seconds
            FROM operation_timings
            ORDER BY total_seconds DESC
            """
        )
    ]
    event_counts = {
        row["event_type"]: row["count"]
        for row in db.execute("SELECT event_type, COUNT(*) AS count FROM events GROUP BY event_type")
    }
    job_counts = {
        row["status"]: row["count"]
        for row in db.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")
    }
    reduce_job_counts = {
        row["status"]: row["count"]
        for row in db.execute("SELECT status, COUNT(*) AS count FROM reduce_jobs GROUP BY status")
    }
    reported_by_stage = {
        row["stage_index"]: row["count"]
        for row in db.execute("SELECT stage_index, COUNT(*) AS count FROM canonical_drawings GROUP BY stage_index")
    }
    shard_by_stage = [
        dict(row)
        for row in db.execute(
            """
            SELECT stage_index, kind, COUNT(*) AS shard_count,
                   COALESCE(SUM(record_count), 0) AS record_count,
                   COALESCE(SUM(byte_count), 0) AS byte_count
            FROM shard_manifests
            GROUP BY stage_index, kind
            ORDER BY stage_index, kind
            """
        )
    ]
    status = {
        row["key"]: row["value"]
        for row in db.execute("SELECT key, value FROM run_status")
    }
    return {
        "created_at": time.time(),
        "run_status": status,
        "reported_classes_by_stage": reported_by_stage,
        "job_counts_before_cleanup": job_counts,
        "reduce_job_counts_before_cleanup": reduce_job_counts,
        "event_counts_before_cleanup": event_counts,
        "worker_io_summary": worker_io_summary(db),
        "worker_io_by_stage": worker_io_by_stage(db),
        "operation_timings": timings,
        "shards_by_stage": shard_by_stage,
    }


def add_number(target: dict, key: str, value) -> None:
    target[key] = target.get(key, 0) + (value or 0)


def worker_io_summary(db: sqlite3.Connection) -> dict:
    out = {
        "expand": {},
        "reduce": {},
    }
    rows = db.execute(
        """
        SELECT event_type, payload_json
        FROM events
        WHERE event_type IN ('job_result', 'reduce_result') AND payload_json IS NOT NULL
        """
    ).fetchall()
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            continue
        profile = payload.get("profile") or {}
        timings = profile.get("timings") or {}
        if row["event_type"] == "job_result":
            slot = out["expand"]
            add_number(slot, "jobs", 1)
            add_number(slot, "candidates_seen", payload.get("candidates_seen", 0))
            add_number(slot, "candidates_submitted", payload.get("candidates_submitted", 0))
            add_number(slot, "shard_files_written", profile.get("output_shard_files", 0))
            add_number(slot, "compressed_bytes_written", profile.get("output_shard_compressed_bytes", 0))
            add_number(slot, "payload_bytes_written", profile.get("output_compact_payload_bytes", 0))
            add_number(slot, "write_seconds", timings.get("checkpoint_write_shards", 0.0))
            add_number(slot, "apply_block_seconds", timings.get("apply_block", 0.0))
        elif row["event_type"] == "reduce_result":
            slot = out["reduce"]
            add_number(slot, "jobs", 1)
            add_number(slot, "raw_records_read", payload.get("raw_records", 0))
            add_number(slot, "reduced_records_written", payload.get("reduced_records", 0))
            add_number(slot, "raw_shard_files_read", profile.get("raw_shard_files", 0))
            add_number(slot, "compressed_bytes_read", profile.get("raw_shard_compressed_bytes", 0))
            add_number(slot, "payload_bytes_read", profile.get("raw_payload_bytes", 0))
            add_number(slot, "compressed_bytes_written", payload.get("reduced_bytes", 0))
            add_number(slot, "read_seconds", timings.get("reduce_read_shards", 0.0))
            add_number(slot, "canonicalize_seconds", timings.get("reduce_canonicalize", 0.0))
            add_number(slot, "write_seconds", timings.get("reduce_write_shard", 0.0))
    for slot in out.values():
        if slot.get("write_seconds"):
            slot["compressed_write_mb_per_sec"] = (
                slot.get("compressed_bytes_written", 0) / 1_000_000 / slot["write_seconds"]
            )
            slot["payload_write_mb_per_sec"] = (
                slot.get("payload_bytes_written", 0) / 1_000_000 / slot["write_seconds"]
            )
        if slot.get("read_seconds"):
            slot["compressed_read_mb_per_sec"] = (
                slot.get("compressed_bytes_read", 0) / 1_000_000 / slot["read_seconds"]
            )
            slot["payload_read_mb_per_sec"] = (
                slot.get("payload_bytes_read", 0) / 1_000_000 / slot["read_seconds"]
            )
    return out


def worker_io_by_stage(db: sqlite3.Connection) -> list[dict]:
    """Aggregate task-boundary timing and resource data before event cleanup."""
    stages: dict[tuple[str, int], dict] = {}
    rows = db.execute(
        """
        SELECT timestamp, event_type, payload_json
        FROM events
        WHERE event_type IN ('job_result', 'reduce_result') AND payload_json IS NOT NULL
        ORDER BY timestamp
        """
    ).fetchall()
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            continue
        stage_index = int(payload.get("stage_index", -1))
        if stage_index < 0:
            continue
        kind = "expand" if row["event_type"] == "job_result" else "reduce"
        slot = stages.setdefault(
            (kind, stage_index),
            {
                "kind": kind,
                "stage_index": stage_index,
                "tasks": 0,
                "worker_elapsed_seconds_sum": 0.0,
                "worker_cpu_seconds_sum": 0.0,
                "worker_max_rss_kib": 0,
                "first_task_started_at": None,
                "last_task_finished_at": None,
            },
        )
        profile = payload.get("profile") or {}
        elapsed = float(payload.get("elapsed_seconds", profile.get("elapsed_seconds", 0.0)) or 0.0)
        finished_at = float(row["timestamp"])
        started_at = finished_at - elapsed
        slot["tasks"] += 1
        slot["worker_elapsed_seconds_sum"] += elapsed
        slot["worker_cpu_seconds_sum"] += float(profile.get("worker_cpu_seconds", 0.0) or 0.0)
        slot["worker_max_rss_kib"] = max(
            slot["worker_max_rss_kib"], int(profile.get("worker_max_rss_kib", 0) or 0)
        )
        slot["first_task_started_at"] = (
            started_at
            if slot["first_task_started_at"] is None
            else min(slot["first_task_started_at"], started_at)
        )
        slot["last_task_finished_at"] = (
            finished_at
            if slot["last_task_finished_at"] is None
            else max(slot["last_task_finished_at"], finished_at)
        )
        if kind == "expand":
            add_number(slot, "candidates_seen", payload.get("candidates_seen", 0))
            add_number(slot, "candidates_submitted", payload.get("candidates_submitted", 0))
            block = profile.get("block") or {}
            slot["block_kind"] = block.get("kind")
            slot["block_vertex"] = block.get("vertex")
            add_number(slot, "block_wall_seconds_sum", block.get("total_seconds", 0.0))
            step_totals = slot.setdefault("block_step_totals", {})
            for step in block.get("steps") or []:
                step_index = int(step.get("block_step_index", len(step_totals)))
                step_slot = step_totals.setdefault(
                    step_index,
                    {
                        "block_step_index": step_index,
                        "tasks": 0,
                        "frontier_in": 0,
                        "raw_outputs": 0,
                        "frontier_out": 0,
                        "exact_dedupe_removed": 0,
                    },
                )
                step_slot["tasks"] += 1
                for key in ("frontier_in", "raw_outputs", "frontier_out", "exact_dedupe_removed"):
                    step_slot[key] += int(step.get(key, 0) or 0)
        else:
            add_number(slot, "raw_records", payload.get("raw_records", 0))
            add_number(slot, "reduced_records", payload.get("reduced_records", 0))
    output = []
    for slot in stages.values():
        first = slot["first_task_started_at"]
        last = slot["last_task_finished_at"]
        slot["wall_span_seconds"] = last - first if first is not None and last is not None else 0.0
        if isinstance(slot.get("block_step_totals"), dict):
            slot["block_step_totals"] = [
                slot["block_step_totals"][index]
                for index in sorted(slot["block_step_totals"])
            ]
        output.append(slot)
    return sorted(output, key=lambda value: (value["stage_index"], value["kind"]))


def compact_completed_run(db: sqlite3.Connection, run_dir: Path, *, delete_transient_rows: bool = True) -> None:
    outputs = run_dir / "outputs"
    outputs.mkdir(parents=True, exist_ok=True)
    summary = final_run_summary(db)
    (outputs / "final_run_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if not delete_transient_rows:
        return
    db.execute("DELETE FROM jobs")
    db.execute("DELETE FROM reduce_jobs")
    db.execute("DELETE FROM worker_heartbeats")
    db.execute("DELETE FROM events")
    db.execute("UPDATE canonical_drawings SET source_job_key=NULL, source_worker=NULL")
    db.execute("UPDATE shard_manifests SET source_job_key=NULL, source_worker=NULL")
    set_status(db, "transient_rows_deleted", "1")
    set_status(db, "final_summary_path", "outputs/final_run_summary.json")
    db.execute("VACUUM")
    db.commit()


def record_timing(db: sqlite3.Connection, operation: str, elapsed: float) -> None:
    db.execute(
        """
        INSERT INTO operation_timings(operation, request_count, total_seconds, max_seconds)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(operation) DO UPDATE SET
            request_count = request_count + 1,
            total_seconds = total_seconds + excluded.total_seconds,
            max_seconds = MAX(max_seconds, excluded.max_seconds)
        """,
        (operation, elapsed, elapsed),
    )
