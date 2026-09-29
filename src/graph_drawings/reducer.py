"""Bucket reducers for compact sharded checkpoint storage."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import time

from . import config
from .automorphism import context_from_record
from .canonical import drawing_keys
from .compact import (
    CompactContext,
    compress_db_blob,
    context_from_run_config,
    decode_drawing,
)
from .shards import (
    ShardRecord,
    iter_compound_flag_shard,
    iter_compound_raw_shard,
    iter_reduced_shard,
    reduced_shard_path,
    write_flag_shard,
    write_reduced_shard,
)
from .status import (
    connect_db,
    insert_compact_job,
    insert_representative_index,
    insert_shard_manifest,
    record_timing,
    set_status,
)


@dataclass(frozen=True, slots=True)
class ReduceSummary:
    stage_index: int
    buckets: int
    raw_records: int
    reduced_records: int
    reduced_bytes: int


@dataclass(frozen=True, slots=True)
class BucketReduceSummary:
    stage_index: int
    bucket_hash: str
    bucket_dir: str
    reduced_shard_path: str | None
    raw_records: int
    reduced_records: int
    reduced_bytes: int
    raw_shard_files: int
    raw_compressed_bytes: int
    raw_payload_bytes: int
    read_seconds: float
    canonicalize_seconds: float
    write_seconds: float


def load_run(run_id: str) -> tuple[dict, CompactContext]:
    run_dir = config.RUNS_ROOT / run_id
    run_config = json.loads((run_dir / "run_config.json").read_text(encoding="utf-8"))
    return run_config, context_from_run_config(run_config)


def incoming_bucket_dirs(run_dir: Path, stage_index: int) -> list[Path]:
    root = run_dir / "shards" / f"stage-{stage_index}" / "incoming"
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_dir() and any(path.glob("*.gds.gz")))


def crossing_count_from_bucket_dir(bucket_dir: Path) -> int | None:
    for part in bucket_dir.parts:
        if part.startswith("crossings-"):
            try:
                return int(part.removeprefix("crossings-"))
            except ValueError:
                return None
    return None


def reduce_bucket(
    run_id: str,
    stage_index: int,
    bucket_dir: Path,
    *,
    run_config: dict | None = None,
    compact_context: CompactContext | None = None,
) -> BucketReduceSummary:
    run_dir = config.RUNS_ROOT / run_id
    if run_config is None or compact_context is None:
        run_config, compact_context = load_run(run_id)
    total_steps = int(run_config["total_steps"])
    final_output_mode = str(run_config.get("final_output_mode", "full_drawings"))
    profile_operations = bool(run_config.get("profile_operations", config.PROFILE_OPERATIONS))
    canonical_context = context_from_record(run_config["canonical_context"])
    include_full_keys = bool(run_config.get("debug_store_full_canonical_keys", False))
    key_strategy = (
        str(run_config.get("final_drawing_key_strategy", "full_presentation"))
        if stage_index >= total_steps and final_output_mode == "full_drawings"
        else "full_presentation"
    )
    bucket_hash = bucket_dir.name
    crossing_count = crossing_count_from_bucket_dir(bucket_dir)
    if stage_index >= total_steps and final_output_mode != "full_drawings":
        flags: set[str] = set()
        raw_records = 0
        raw_shard_files = 0
        raw_compressed_bytes = 0
        raw_payload_bytes = 0
        read_seconds = 0.0
        for shard_path in sorted(bucket_dir.glob("*.gds.gz")):
            raw_shard_files += 1
            raw_compressed_bytes += shard_path.stat().st_size
            records = iter_compound_flag_shard(shard_path)
            while True:
                read_start = time.perf_counter() if profile_operations else 0.0
                try:
                    record = next(records)
                except StopIteration:
                    if profile_operations:
                        read_seconds += time.perf_counter() - read_start
                    break
                if profile_operations:
                    read_seconds += time.perf_counter() - read_start
                raw_records += 1
                raw_payload_bytes += len(record.flag.encode("utf-8"))
                flags.add(record.flag)
        if not flags:
            return BucketReduceSummary(
                stage_index=stage_index,
                bucket_hash=bucket_hash,
                bucket_dir=str(bucket_dir.relative_to(run_dir)),
                reduced_shard_path=None,
                raw_records=raw_records,
                reduced_records=0,
                reduced_bytes=0,
                raw_shard_files=raw_shard_files,
                raw_compressed_bytes=raw_compressed_bytes,
                raw_payload_bytes=raw_payload_bytes,
                read_seconds=read_seconds,
                canonicalize_seconds=0.0,
                write_seconds=0.0,
            )
        out_path = reduced_shard_path(run_dir, stage_index, bucket_hash, crossing_count)
        write_start = time.perf_counter() if profile_operations else 0.0
        byte_count = write_flag_shard(out_path, sorted(flags))
        write_seconds = time.perf_counter() - write_start if profile_operations else 0.0
        return BucketReduceSummary(
            stage_index=stage_index,
            bucket_hash=bucket_hash,
            bucket_dir=str(bucket_dir.relative_to(run_dir)),
            reduced_shard_path=str(out_path.relative_to(run_dir)),
            raw_records=raw_records,
            reduced_records=len(flags),
            reduced_bytes=byte_count,
            raw_shard_files=raw_shard_files,
            raw_compressed_bytes=raw_compressed_bytes,
            raw_payload_bytes=raw_payload_bytes,
            read_seconds=read_seconds,
            canonicalize_seconds=0.0,
            write_seconds=write_seconds,
        )
    representatives: dict[str, ShardRecord] = {}
    raw_records = 0
    raw_shard_files = 0
    raw_compressed_bytes = 0
    raw_payload_bytes = 0
    read_seconds = 0.0
    canonicalize_seconds = 0.0
    for shard_path in sorted(bucket_dir.glob("*.gds.gz")):
        raw_shard_files += 1
        raw_compressed_bytes += shard_path.stat().st_size
        records = iter_compound_raw_shard(shard_path, fallback_bucket_hash=bucket_hash)
        while True:
            read_start = time.perf_counter() if profile_operations else 0.0
            try:
                raw_record = next(records)
            except StopIteration:
                if profile_operations:
                    read_seconds += time.perf_counter() - read_start
                break
            if profile_operations:
                read_seconds += time.perf_counter() - read_start
            raw_records += 1
            compact_payload = raw_record.compact_payload
            raw_payload_bytes += len(compact_payload)
            drawing = decode_drawing(compact_payload, compact_context)
            key_start = time.perf_counter() if profile_operations else 0.0
            keys = drawing_keys(
                stage_index,
                drawing,
                canonical_context,
                include_full_keys=include_full_keys,
                key_strategy=key_strategy,
            )
            if profile_operations:
                canonicalize_seconds += time.perf_counter() - key_start
            reduce_hash = keys.report_hash if stage_index >= total_steps else keys.work_hash
            reduce_hash = f"{raw_record.bucket_hash}:{reduce_hash}"
            record = ShardRecord(keys.report_hash, keys.work_hash, compact_payload)
            previous = representatives.get(reduce_hash)
            if previous is None or compact_payload < previous.compact_payload:
                representatives[reduce_hash] = record
    records = sorted(representatives.values(), key=lambda record: (record.report_hash, record.work_hash))
    if not records:
        return BucketReduceSummary(
            stage_index=stage_index,
            bucket_hash=bucket_hash,
            bucket_dir=str(bucket_dir.relative_to(run_dir)),
            reduced_shard_path=None,
            raw_records=raw_records,
            reduced_records=0,
            reduced_bytes=0,
            raw_shard_files=raw_shard_files,
            raw_compressed_bytes=raw_compressed_bytes,
            raw_payload_bytes=raw_payload_bytes,
            read_seconds=read_seconds,
            canonicalize_seconds=canonicalize_seconds,
            write_seconds=0.0,
        )
    out_path = reduced_shard_path(run_dir, stage_index, bucket_hash, crossing_count)
    write_start = time.perf_counter() if profile_operations else 0.0
    byte_count = write_reduced_shard(out_path, records)
    write_seconds = time.perf_counter() - write_start if profile_operations else 0.0
    return BucketReduceSummary(
        stage_index=stage_index,
        bucket_hash=bucket_hash,
        bucket_dir=str(bucket_dir.relative_to(run_dir)),
        reduced_shard_path=str(out_path.relative_to(run_dir)),
        raw_records=raw_records,
        reduced_records=len(records),
        reduced_bytes=byte_count,
        raw_shard_files=raw_shard_files,
        raw_compressed_bytes=raw_compressed_bytes,
        raw_payload_bytes=raw_payload_bytes,
        read_seconds=read_seconds,
        canonicalize_seconds=canonicalize_seconds,
        write_seconds=write_seconds,
    )


def reduce_stage(run_id: str, stage_index: int, db_path: Path | None = None) -> ReduceSummary:
    run_dir = config.RUNS_ROOT / run_id
    run_config, compact_context = load_run(run_id)
    total_steps = int(run_config["total_steps"])
    final_output_mode = str(run_config.get("final_output_mode", "full_drawings"))
    db = connect_db(db_path or (run_dir / "coordinator.sqlite"))
    start = time.perf_counter()
    buckets = incoming_bucket_dirs(run_dir, stage_index)
    raw_records = 0
    reduced_records = 0
    reduced_bytes = 0
    try:
        set_status(db, "reducing_stage", str(stage_index))
        for bucket_dir in buckets:
            summary = reduce_bucket(
                run_id,
                stage_index,
                bucket_dir,
                run_config=run_config,
                compact_context=compact_context,
            )
            raw_records += summary.raw_records
            reduced_records += summary.reduced_records
            reduced_bytes += summary.reduced_bytes
            if summary.reduced_shard_path is None:
                continue
            rel_path = summary.reduced_shard_path
            insert_shard_manifest(
                db,
                shard_path=rel_path,
                stage_index=stage_index,
                bucket_hash=summary.bucket_hash,
                kind="reduced",
                source_job_key=None,
                source_worker="reducer",
                record_count=summary.reduced_records,
                byte_count=summary.reduced_bytes,
            )
            if not (stage_index >= total_steps and final_output_mode != "full_drawings"):
                for ordinal, record in enumerate(iter_reduced_shard(run_dir / rel_path)):
                    insert_representative_index(
                        db,
                        stage_index=stage_index,
                        report_hash=record.report_hash,
                        work_hash=record.work_hash,
                        bucket_hash=summary.bucket_hash,
                        shard_path=rel_path,
                        shard_ordinal=ordinal,
                        source_worker="reducer",
                    )
                    if stage_index < total_steps:
                        insert_compact_job(
                            db,
                            stage_index=stage_index,
                            report_hash=record.report_hash,
                            work_hash=record.work_hash,
                            compact_blob=compress_db_blob(record.compact_payload),
                        )
            db.commit()
        if config.DELETE_RAW_SHARDS_AFTER_REDUCE:
            incoming_root = run_dir / "shards" / f"stage-{stage_index}" / "incoming"
            if incoming_root.exists():
                shutil.rmtree(incoming_root)
        set_status(db, "reducing_stage", "")
        set_status(db, "last_reduced_stage", str(stage_index))
        record_timing(db, f"reducer_stage_{stage_index}", time.perf_counter() - start)
        db.commit()
    finally:
        db.close()
    return ReduceSummary(stage_index, len(buckets), raw_records, reduced_records, reduced_bytes)
