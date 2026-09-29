"""SQLite-backed TCP coordinator for local and Slurm workers."""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import socketserver
import sys
import threading
import time
from pathlib import Path

from . import config
from .io_utils import append_jsonl, clear_live_db_path, write_live_db_path
from .compact import compress_db_blob
from .reducer import incoming_bucket_dirs
from .shards import iter_flag_shard, iter_reduced_shard
from .status import (
    compact_completed_run,
    connect_db,
    ensure_runtime_schema,
    expire_reduce_leases,
    expire_leases,
    get_status,
    insert_compact_job,
    insert_reduce_job,
    insert_representative_index,
    insert_shard_manifest,
    mark_job,
    mark_reduce_job,
    record_timing,
    set_status,
    summarize_db,
)


class CoordinatorState:
    def __init__(self, run_id: str, total_steps: int, db_path: Path):
        self.run_id = run_id
        self.total_steps = total_steps
        self.run_dir = config.RUNS_ROOT / run_id
        self.db = connect_db(db_path)
        ensure_runtime_schema(self.db)
        self.lock = threading.RLock()
        self.last_progress = 0.0
        self.stop_requested = False
        write_live_db_path(run_id, db_path)
        if get_status(self.db, "active_stage") is None:
            set_status(self.db, "active_stage", "0")
        if get_status(self.db, "run_complete") is None:
            set_status(self.db, "run_complete", "0")

    def shutdown(self) -> None:
        self.db.close()
        clear_live_db_path(self.run_id)

    def global_done(self) -> bool:
        with self.lock:
            if get_status(self.db, "run_complete", "0") == "1":
                return True
            expire_leases(self.db)
            expire_reduce_leases(self.db)
            if get_status(self.db, "reducing_stage", ""):
                return False
            return not any(
                self.db.execute(f"SELECT 1 FROM {table} WHERE status IN ('pending', 'leased') LIMIT 1").fetchone()
                for table in ('jobs', 'reduce_jobs')
            )

    def maybe_log_progress(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_progress < config.PROGRESS_LOG_EVERY_SECONDS:
            return
        self.last_progress = now
        # Full queue/shard aggregates belong to explicit STATUS requests.
        summary = summarize_db(self.db, options={
            "include_job_counts": False, "include_stage_counts": False,
            "include_worker_heartbeats": False, "include_shards": False,
        })
        append_jsonl(self.run_dir / "logs" / "progress.jsonl", {"timestamp": now, **summary})

    def run_config(self) -> dict:
        return json.loads((self.run_dir / "run_config.json").read_text(encoding="utf-8"))

    def final_output_mode(self) -> str:
        return str(self.run_config().get("final_output_mode", "full_drawings"))

    def final_flags_output_path(self, mode: str) -> Path:
        if mode == "crossing_pair_flags":
            return self.run_dir / "outputs" / "crossing_pair_flags.txt"
        if mode == "four_graph_flags":
            return self.run_dir / "outputs" / "four_graph_flags.txt"
        raise ValueError(f"unknown final flag output mode: {mode}")

    def log_event(self, event_type: str, worker_id=None, job_key=None, message=None, payload=None) -> None:
        self.db.execute(
            """
            INSERT INTO events(timestamp, event_type, worker_id, job_key, message, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                event_type,
                worker_id,
                job_key,
                message,
                json.dumps(payload, sort_keys=True) if payload is not None else None,
            ),
        )

    def handle(self, request: dict) -> dict:
        with self.lock:
            start = time.perf_counter()
            try:
                typ = request.get("type")
                if typ == "GET_JOB":
                    response = self.get_job(request.get("worker_id", "unknown"))
                elif typ == "SUBMIT_SHARDS":
                    response = self.submit_shards(request)
                elif typ == "SUBMIT_REDUCTION":
                    response = self.submit_reduction(request)
                elif typ == "CLOSE_JOB":
                    response = self.close_job(request)
                elif typ == "CLOSE_JOBS":
                    response = self.close_jobs(request)
                elif typ == "HEARTBEAT":
                    response = self.heartbeat(request)
                elif typ == "STATUS":
                    response = {"ok": True, "summary": summarize_db(self.db)}
                elif typ == "DRAIN_WORKERS":
                    response = self.drain_workers(request)
                elif typ == "LIST_WORKERS":
                    response = {"ok": True, "workers": self.worker_rows()}
                elif typ == "SHUTDOWN":
                    self.stop_requested = True
                    response = {"ok": True}
                else:
                    response = {"ok": False, "error": f"unknown request type: {typ!r}"}
                self.maybe_log_progress()
                record_timing(self.db, f"coordinator_{typ or 'UNKNOWN'}", time.perf_counter() - start)
                self.db.commit()
                return response
            except Exception as exc:
                self.db.rollback()
                return {"ok": False, "error": repr(exc)}

    def get_job(self, worker_id: str) -> dict:
        expire_leases(self.db)
        expire_reduce_leases(self.db)
        if self.worker_should_drain(worker_id):
            return {"ok": True, "job": None, "shutdown": True, "global_done": False}
        if get_status(self.db, "run_complete", "0") == "1":
            return {"ok": True, "job": None, "global_done": True}
        reducing_stage = get_status(self.db, "reducing_stage", "")
        if reducing_stage:
            reduce_job = self.get_reduce_job(worker_id, int(reducing_stage))
            if reduce_job is not None:
                return {"ok": True, "job": reduce_job}
            return {"ok": True, "job": None, "global_done": self.global_done()}
        active_stage = int(get_status(self.db, "active_stage", "0") or "0")
        row = self.db.execute(
            """
            SELECT job_key, stage_index, report_hash, work_hash, compact_blob
            FROM jobs
            WHERE status='pending' AND stage_index=?
            ORDER BY stage_index ASC, created_at ASC
            LIMIT 1
            """,
            (active_stage,),
        ).fetchone()
        if row is None:
            return {"ok": True, "job": None, "global_done": self.global_done()}
        now = time.time()
        self.db.execute(
            """
            UPDATE jobs
            SET status='leased', lease_owner=?, lease_expires_at=?, updated_at=?
            WHERE job_key=? AND status='pending'
            """,
            (worker_id, now + config.LEASE_SECONDS, now, row["job_key"]),
        )
        job = dict(row)
        job["job_type"] = "expand"
        if job.get("compact_blob") is not None:
            job["compact_blob_b64"] = base64.b64encode(job.pop("compact_blob")).decode("ascii")
        return {"ok": True, "job": job}

    def get_reduce_job(self, worker_id: str, stage_index: int) -> dict | None:
        row = self.db.execute(
            """
            SELECT reduce_job_key, stage_index, bucket_hash, bucket_dir
            FROM reduce_jobs
            WHERE status='pending' AND stage_index=?
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (stage_index,),
        ).fetchone()
        if row is None:
            return None
        now = time.time()
        self.db.execute(
            """
            UPDATE reduce_jobs
            SET status='leased', lease_owner=?, lease_expires_at=?, updated_at=?
            WHERE reduce_job_key=? AND status='pending'
            """,
            (worker_id, now + config.LEASE_SECONDS, now, row["reduce_job_key"]),
        )
        job = dict(row)
        job["job_key"] = job["reduce_job_key"]
        job["job_type"] = "reduce"
        return job

    def stage_complete(self, stage_index: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM jobs WHERE status IN ('pending', 'leased') AND stage_index=? LIMIT 1",
            (stage_index,),
        ).fetchone() is None

    def reduce_stage_complete(self, stage_index: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM reduce_jobs WHERE status IN ('pending', 'leased') AND stage_index=? LIMIT 1",
            (stage_index,),
        ).fetchone() is None

    def reduce_and_advance(self, completed_stage: int) -> None:
        self.prepare_reduce_jobs(completed_stage + 1)

    def mark_run_complete(self) -> None:
        set_status(self.db, "run_complete", "1")
        if config.COMPACT_RUN_DB_AFTER_COMPLETION:
            compact_completed_run(
                self.db,
                self.run_dir,
                delete_transient_rows=config.DELETE_TRANSIENT_ROWS_AFTER_COMPLETION,
            )

    def prepare_reduce_jobs(self, reduce_stage_index: int) -> None:
        if reduce_stage_index > self.total_steps:
            self.mark_run_complete()
            return
        buckets = incoming_bucket_dirs(self.run_dir, reduce_stage_index)
        set_status(self.db, "reducing_stage", str(reduce_stage_index))
        inserted = 0
        for bucket_dir in buckets:
            if insert_reduce_job(
                self.db,
                stage_index=reduce_stage_index,
                bucket_hash=bucket_dir.name,
                bucket_dir=str(bucket_dir.relative_to(self.run_dir)),
            ):
                inserted += 1
        self.log_event(
            "reduction_jobs_created",
            payload={"stage_index": reduce_stage_index, "buckets": len(buckets), "inserted": inserted},
        )
        self.db.commit()
        if not buckets:
            self.finalize_reduction(reduce_stage_index)

    def finalize_reduction(self, stage_index: int) -> None:
        row = self.db.execute(
            """
            SELECT COUNT(*) AS buckets, COALESCE(SUM(record_count), 0) AS reduced_records,
                   COALESCE(SUM(byte_count), 0) AS reduced_bytes
            FROM shard_manifests
            WHERE stage_index=? AND kind='reduced'
            """,
            (stage_index,),
        ).fetchone()
        if config.DELETE_RAW_SHARDS_AFTER_REDUCE:
            incoming_root = self.run_dir / "shards" / f"stage-{stage_index}" / "incoming"
            if incoming_root.exists():
                shutil.rmtree(incoming_root)
        set_status(self.db, "reducing_stage", "")
        set_status(self.db, "last_reduced_stage", str(stage_index))
        reduced_records = int(row[1] or 0)
        run_finished = stage_index >= self.total_steps or reduced_records == 0
        if run_finished:
            set_status(self.db, "run_complete", "1")
            final_output_mode = self.final_output_mode()
            if stage_index >= self.total_steps and final_output_mode != "full_drawings":
                output_flags = 0
                out_path = self.final_flags_output_path(final_output_mode)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with out_path.open("w", encoding="utf-8") as f:
                    for shard in self.db.execute(
                        """
                        SELECT shard_path
                        FROM shard_manifests
                        WHERE stage_index=? AND kind='reduced'
                        ORDER BY shard_path
                        """,
                        (stage_index,),
                    ):
                        for flag in iter_flag_shard(self.run_dir / shard["shard_path"]):
                            f.write(flag + "\n")
                            output_flags += 1
                self.log_event(
                    "final_flags_written",
                    payload={
                        "stage_index": stage_index,
                        "mode": final_output_mode,
                        "path": str(out_path.relative_to(self.run_dir)),
                        "flags": output_flags,
                    },
                )
        else:
            set_status(self.db, "active_stage", str(stage_index))
        self.log_event(
            "stage_reduced",
            payload={
                "stage_index": stage_index,
                "buckets": int(row[0] or 0),
                "reduced_records": reduced_records,
                "reduced_bytes": int(row[2] or 0),
            },
        )
        if run_finished and config.COMPACT_RUN_DB_AFTER_COMPLETION:
            compact_completed_run(
                self.db,
                self.run_dir,
                delete_transient_rows=config.DELETE_TRANSIENT_ROWS_AFTER_COMPLETION,
            )
        self.db.commit()

    def close_job(self, request: dict) -> dict:
        job_type = request.get("job_type")
        job_key = request["job_key"]
        if job_type is None:
            job_type = "reduce" if str(job_key).startswith("reduce:") else "expand"
        if job_type == "reduce":
            row = self.db.execute("SELECT stage_index FROM reduce_jobs WHERE reduce_job_key=?", (job_key,)).fetchone()
            stage_index = int(row["stage_index"]) if row else int(request.get("stage_index", -1))
            mark_reduce_job(self.db, job_key, "done")
            self.log_event("reduce_done", worker_id=request.get("worker_id"), job_key=job_key)
            self.db.commit()
            if stage_index >= 0 and self.reduce_stage_complete(stage_index):
                self.finalize_reduction(stage_index)
            return {"ok": True, "global_done": self.global_done()}

        stage_index = int(request.get("stage_index", -1))
        if stage_index < 0:
            row = self.db.execute("SELECT stage_index FROM jobs WHERE job_key=?", (job_key,)).fetchone()
            stage_index = int(row["stage_index"]) if row else -1
        mark_job(self.db, job_key, "done")
        self.log_event("done", worker_id=request.get("worker_id"), job_key=job_key)
        self.db.commit()
        if stage_index >= 0 and self.stage_complete(stage_index):
            self.reduce_and_advance(stage_index)
        return {"ok": True, "global_done": self.global_done()}

    def close_jobs(self, request: dict) -> dict:
        jobs = request.get("jobs") or []
        if not jobs:
            return {"ok": True, "global_done": self.global_done()}
        changed_stages = set()
        for job in jobs:
            job_type = job.get("job_type")
            job_key = job["job_key"]
            if job_type is None:
                job_type = "reduce" if str(job_key).startswith("reduce:") else "expand"
            if job_type == "reduce":
                row = self.db.execute("SELECT stage_index FROM reduce_jobs WHERE reduce_job_key=?", (job_key,)).fetchone()
                stage_index = int(row["stage_index"]) if row else int(job.get("stage_index", -1))
                mark_reduce_job(self.db, job_key, "done")
                self.log_event("reduce_done", worker_id=request.get("worker_id"), job_key=job_key)
                if stage_index >= 0:
                    changed_stages.add(("reduce", stage_index))
            else:
                stage_index = int(job.get("stage_index", -1))
                if stage_index < 0:
                    row = self.db.execute("SELECT stage_index FROM jobs WHERE job_key=?", (job_key,)).fetchone()
                    stage_index = int(row["stage_index"]) if row else -1
                mark_job(self.db, job_key, "done")
                self.log_event("done", worker_id=request.get("worker_id"), job_key=job_key)
                if stage_index >= 0:
                    changed_stages.add(("expand", stage_index))
        self.db.commit()
        for kind, stage_index in sorted(changed_stages):
            if kind == "reduce" and self.reduce_stage_complete(stage_index):
                self.finalize_reduction(stage_index)
            elif kind == "expand" and self.stage_complete(stage_index):
                self.reduce_and_advance(stage_index)
        return {"ok": True, "global_done": self.global_done()}

    def submit_shards(self, request: dict) -> dict:
        profile = request.get("profile") or {}
        job_results = request.get("job_results") or []
        shard_count = 0
        record_count = 0
        byte_count = 0
        for shard in request.get("shards") or []:
            shard_count += 1
            record_count += int(shard.get("record_count", 0) or 0)
            byte_count += int(shard.get("byte_count", 0) or 0)
            insert_shard_manifest(
                self.db,
                shard_path=str(shard["shard_path"]),
                stage_index=int(shard["stage_index"]),
                bucket_hash=str(shard["bucket_hash"]),
                kind="incoming",
                source_job_key=request.get("job_key"),
                source_worker=request.get("worker_id"),
                record_count=int(shard.get("record_count", 0) or 0),
                byte_count=int(shard.get("byte_count", 0) or 0),
            )
        if job_results:
            for result in job_results:
                result_profile = result.get("profile") or {}
                record_timing(self.db, "worker_process_job", float(result.get("elapsed_seconds", 0.0)))
                self.record_worker_profile_timings(result_profile)
                self.log_event(
                    "job_result",
                    worker_id=request.get("worker_id"),
                    job_key=result["job_key"],
                    payload={
                        "stage_index": int(result.get("stage_index", -1)),
                        "candidates_seen": int(result.get("candidates_seen", 0) or 0),
                        "candidates_submitted": int(result.get("candidates_submitted", result.get("candidates_seen", 0)) or 0),
                        "candidates_kind": str(result_profile.get("candidate_kind", "drawing")),
                        "shards": int(result.get("shards", 0) or 0),
                        "shard_bytes": int(result.get("shard_bytes", 0) or 0),
                        "candidates_accepted": 0,
                        "reported_new": 0,
                        "work_new": 0,
                        "elapsed_seconds": float(result.get("elapsed_seconds", 0.0)),
                        "profile": result_profile,
                    },
                )
        else:
            record_timing(self.db, "worker_process_job", float(request.get("elapsed_seconds", 0.0)))
            self.record_worker_profile_timings(profile)
            candidates_seen = int(
                profile.get(
                    "checkpoint_routes_seen",
                    profile.get("checkpoint_candidates_before_local_reduce", record_count),
                )
                or 0
            )
            self.log_event(
                "job_result",
                worker_id=request.get("worker_id"),
                job_key=request["job_key"],
                payload={
                    "stage_index": int(request.get("stage_index", -1)),
                    "candidates_seen": candidates_seen,
                    "candidates_submitted": record_count,
                    "candidates_kind": str(profile.get("candidate_kind", "drawing")),
                    "shards": shard_count,
                    "shard_bytes": byte_count,
                    "candidates_accepted": 0,
                    "reported_new": 0,
                    "work_new": 0,
                    "elapsed_seconds": float(request.get("elapsed_seconds", 0.0)),
                    "profile": profile,
                },
            )
        self.db.commit()
        return {"ok": True, "accepted": record_count, "reported_new": 0, "work_new": 0}

    def submit_reduction(self, request: dict) -> dict:
        profile = request.get("profile") or {}
        summary = request.get("summary") or {}
        stage_index = int(summary.get("stage_index", request.get("stage_index", -1)))
        reduced_path = summary.get("reduced_shard_path")
        reduced_records = int(summary.get("reduced_records", 0) or 0)
        reduced_bytes = int(summary.get("reduced_bytes", 0) or 0)
        raw_records = int(summary.get("raw_records", 0) or 0)
        bucket_hash = str(summary.get("bucket_hash", ""))
        inserted_jobs = 0
        inserted_representatives = 0
        if reduced_path:
            insert_shard_manifest(
                self.db,
                shard_path=str(reduced_path),
                stage_index=stage_index,
                bucket_hash=bucket_hash,
                kind="reduced",
                source_job_key=request["job_key"],
                source_worker=request.get("worker_id"),
                record_count=reduced_records,
                byte_count=reduced_bytes,
            )
            if not (stage_index >= self.total_steps and self.final_output_mode() != "full_drawings"):
                for ordinal, record in enumerate(iter_reduced_shard(self.run_dir / reduced_path)):
                    if insert_representative_index(
                        self.db,
                        stage_index=stage_index,
                        report_hash=record.report_hash,
                        work_hash=record.work_hash,
                        bucket_hash=bucket_hash,
                        shard_path=str(reduced_path),
                        shard_ordinal=ordinal,
                        source_job_key=request["job_key"],
                        source_worker=request.get("worker_id"),
                    ):
                        inserted_representatives += 1
                    if stage_index < self.total_steps and insert_compact_job(
                        self.db,
                        stage_index=stage_index,
                        report_hash=record.report_hash,
                        work_hash=record.work_hash,
                        compact_blob=compress_db_blob(record.compact_payload),
                    ):
                        inserted_jobs += 1
        record_timing(self.db, "worker_process_reduce_job", float(request.get("elapsed_seconds", 0.0)))
        self.record_worker_profile_timings(profile)
        self.log_event(
            "reduce_result",
            worker_id=request.get("worker_id"),
            job_key=request["job_key"],
            payload={
                "stage_index": stage_index,
                "bucket_hash": bucket_hash,
                "raw_records": raw_records,
                "reduced_records": reduced_records,
                "reduced_bytes": reduced_bytes,
                "inserted_representatives": inserted_representatives,
                "inserted_jobs": inserted_jobs,
                "elapsed_seconds": float(request.get("elapsed_seconds", 0.0)),
                "profile": profile,
            },
        )
        self.db.commit()
        return {
            "ok": True,
            "raw_records": raw_records,
            "reduced_records": reduced_records,
            "inserted_jobs": inserted_jobs,
            "inserted_representatives": inserted_representatives,
        }

    def record_worker_profile_timings(self, profile: dict) -> None:
        for name, seconds in (profile.get("timings") or {}).items():
            record_timing(self.db, f"worker_{name}", float(seconds))
        for name, value in profile.items():
            if name.startswith(("report_key_", "work_key_")) and name.endswith("_seconds"):
                record_timing(self.db, f"worker_{name}", float(value or 0.0))
        block = profile.get("block") or {}
        if "total_seconds" in block:
            record_timing(self.db, "worker_block_total", float(block.get("total_seconds") or 0.0))
        for step in block.get("steps") or []:
            idx = int(step.get("block_step_index", -1))
            kind = str(step.get("kind", "unknown"))
            prefix = f"worker_block_step_{idx}_{kind}"
            for key in [
                "block_step_seconds",
                "enumerate_sequences_seconds",
                "make_extended_total_seconds",
                "drawing_indices_seconds",
                "face_choice_seconds",
                "path_construction_seconds",
                "route_preparation_seconds",
                "cartesian_product_seconds",
                "unique_presentation_seconds",
                "exact_dedupe_serialize_seconds",
                "checkpoint_bucket_seconds",
                "checkpoint_write_shards_seconds",
            ]:
                if key in step:
                    record_timing(self.db, f"{prefix}_{key}", float(step[key] or 0.0))

    def heartbeat(self, request: dict) -> dict:
        stats = request.get("stats", {})
        now = time.time()
        worker_id = request.get("worker_id", "unknown")
        current_job_key = request.get("current_job_key")
        current_job_keys = list(request.get("current_job_keys") or ([current_job_key] if current_job_key else []))
        worker_draining = self.worker_should_drain(worker_id)
        lease_renewed = False
        renewed_count = 0
        for key in current_job_keys:
            if str(key).startswith("reduce:"):
                cur = self.db.execute(
                    """
                    UPDATE reduce_jobs
                    SET lease_expires_at=?, updated_at=?
                    WHERE reduce_job_key=? AND status='leased' AND lease_owner=?
                    """,
                    (now + config.LEASE_SECONDS, now, key, worker_id),
                )
            else:
                cur = self.db.execute(
                    """
                    UPDATE jobs
                    SET lease_expires_at=?, updated_at=?
                    WHERE job_key=? AND status='leased' AND lease_owner=?
                    """,
                    (now + config.LEASE_SECONDS, now, key, worker_id),
                )
            renewed_count += cur.rowcount
        lease_renewed = renewed_count == len(current_job_keys) if current_job_keys else False
        display_job_key = current_job_key
        if current_job_keys and current_job_key is None:
            display_job_key = current_job_keys[0]
        if len(current_job_keys) > 1:
            display_job_key = f"{current_job_keys[0]} (+{len(current_job_keys) - 1})"
        self.db.execute(
            """
            INSERT INTO worker_heartbeats(
                worker_id, hostname, last_seen_at, current_job_key,
                jobs_done, candidates_seen, candidates_accepted
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                hostname=excluded.hostname,
                last_seen_at=excluded.last_seen_at,
                current_job_key=excluded.current_job_key,
                jobs_done=excluded.jobs_done,
                candidates_seen=excluded.candidates_seen,
                candidates_accepted=excluded.candidates_accepted
            """,
            (
                worker_id,
                request.get("hostname", "unknown"),
                now,
                display_job_key,
                int(stats.get("jobs_done", 0)),
                int(stats.get("candidates_seen", 0)),
                int(stats.get("candidates_accepted", 0)),
            ),
        )
        self.log_event(
            "heartbeat_sample",
            worker_id=worker_id,
            job_key=display_job_key,
            payload=stats,
        )
        self.db.commit()
        if worker_draining and (not current_job_keys or not lease_renewed):
            return {"ok": True, "global_done": False, "shutdown": True}
        return {"ok": True, "global_done": self.global_done()}

    def worker_should_drain(self, worker_id: str) -> bool:
        row = self.db.execute(
            "SELECT action FROM worker_controls WHERE worker_id=?",
            (worker_id,),
        ).fetchone()
        return row is not None and row["action"] == "drain"

    def worker_rows(self) -> list[dict]:
        now = time.time()
        return [
            {
                "worker_id": row["worker_id"],
                "hostname": row["hostname"],
                "current_job_key": row["current_job_key"],
                "seconds_since_last_seen": round(now - row["last_seen_at"], 1),
                "jobs_done": row["jobs_done"],
                "control_action": row["control_action"],
            }
            for row in self.db.execute(
                """
                SELECT h.worker_id, h.hostname, h.last_seen_at, h.current_job_key,
                       h.jobs_done, c.action AS control_action
                FROM worker_heartbeats h
                LEFT JOIN worker_controls c ON c.worker_id=h.worker_id
                ORDER BY h.last_seen_at DESC
                """
            )
        ]

    def drain_workers(self, request: dict) -> dict:
        now = time.time()
        reason = request.get("reason")
        worker_ids = [str(worker_id) for worker_id in request.get("worker_ids") or []]
        count = int(request.get("count", 0) or 0)
        if count > 0:
            existing = set(worker_ids)
            target_count = len(worker_ids) + count
            for row in self.db.execute(
                """
                SELECT h.worker_id
                FROM worker_heartbeats h
                LEFT JOIN worker_controls c ON c.worker_id=h.worker_id
                WHERE c.worker_id IS NULL
                ORDER BY h.last_seen_at DESC
                """
            ):
                if row["worker_id"] in existing:
                    continue
                worker_ids.append(row["worker_id"])
                existing.add(row["worker_id"])
                if len(worker_ids) >= target_count:
                    break
        for worker_id in worker_ids:
            self.db.execute(
                """
                INSERT INTO worker_controls(worker_id, action, reason, created_at, updated_at)
                VALUES (?, 'drain', ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    action='drain',
                    reason=excluded.reason,
                    updated_at=excluded.updated_at
                """,
                (worker_id, reason, now, now),
            )
        self.log_event(
            "workers_draining",
            payload={"worker_ids": worker_ids, "count": len(worker_ids), "reason": reason},
        )
        self.db.commit()
        return {"ok": True, "draining": worker_ids}


class Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            line = self.rfile.readline()
            request = json.loads(line.decode("utf-8"))
            response = self.server.state.handle(request)  # type: ignore[attr-defined]
        except Exception as exc:
            response = {"ok": False, "error": repr(exc)}
            print(f"coordinator handler error: {exc!r}", file=sys.stderr, flush=True)
        self.wfile.write((json.dumps(response, sort_keys=True) + "\n").encode("utf-8"))


class Server(socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, address, handler, state: CoordinatorState):
        super().__init__(address, handler)
        self.state = state


def run_server(
    run_id: str,
    total_steps: int,
    db_path: Path,
    host: str,
    port: int,
    *,
    auto_exit_when_done: bool = True,
) -> None:
    state = CoordinatorState(run_id, total_steps, db_path)
    server = Server((host, port), Handler, state)
    server.timeout = 0.5
    bound_host, bound_port = server.server_address
    print(
        f"coordinator listening on {bound_host}:{bound_port} "
        f"for run {run_id} with summary {summarize_db(state.db)}",
        flush=True,
    )
    try:
        while not state.stop_requested:
            if auto_exit_when_done and state.global_done():
                print(
                    f"coordinator auto-exit: global_done true with summary {summarize_db(state.db)}",
                    flush=True,
                )
                break
            server.handle_request()
        state.maybe_log_progress(force=True)
    finally:
        print("coordinator shutdown", flush=True)
        server.server_close()
        state.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--total-steps", type=int, required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=config.COORDINATOR_PORT)
    args = parser.parse_args()
    run_server(args.run_id, args.total_steps, Path(args.db_path), args.host, args.port)


if __name__ == "__main__":
    main()
