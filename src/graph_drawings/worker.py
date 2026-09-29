"""Shared worker logic for local and Slurm runs."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import socket
import socket as socket_module
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from . import config
from .automorphism import CanonicalContext, build_blocks_from_records, context_from_record
from .build_plan import BuildBlock
from .canonical import drawing_bucket_hash
from .compact import (
    CompactContext,
    context_from_run_config,
    decode_drawing,
    decompress_db_blob,
    encode_normalized_drawing,
)
from .extensions import apply_step_profiled, iter_final_route_crossings_profiled, iter_step_profiled
from .drawing import Crossing
from .flag_formats import crossing_label_table, flag_vertex
from .reducer import reduce_bucket
from .resource_usage import process_max_rss_kib
from .shards import (
    CompoundFlagRecord,
    CompoundRawRecord,
    incoming_partition_shard_path,
    incoming_shard_path,
    write_compound_flag_shard,
    write_compound_raw_shard,
    write_flag_shard,
    write_raw_shard,
)


@dataclass(slots=True)
class JobResult:
    job_key: str
    stage_index: int
    job_type: str
    candidates_seen: int
    candidates: list[dict]
    shards: list[dict]
    reduction_summary: dict | None
    elapsed_seconds: float
    profile: dict


@dataclass(frozen=True, slots=True)
class RunContext:
    blocks: tuple[BuildBlock, ...]
    canonical_context: CanonicalContext
    compact_context: CompactContext
    run_dir: str
    include_full_keys: bool
    bucket_signature_properties: tuple[str, ...]
    shard_by_crossing_count: bool
    compound_shards: bool
    shard_partition_prefix_hex: int
    worker_shard_flush_bytes: int
    checkpoint_shard_batch_records: int
    checkpoint_shard_batch_bytes: int
    final_step_local_dedupe: bool
    block_search_order: str
    verify_intermediate_hash_collisions: bool
    final_output_mode: str
    export_vertex_colors: tuple[int, ...] | None
    final_flag_labels: dict[Crossing, str]
    final_flag_prefix: str
    profile_operations: bool = True


def load_run_context(run_id: str) -> RunContext:
    run_config_path = config.RUNS_ROOT / run_id / "run_config.json"
    run_config = json.loads(run_config_path.read_text(encoding="utf-8"))
    config.validate_entrypoint_runtime(
        operation="worker",
        require_compute_node=True,
        edges=run_config["graph_edges"],
    )
    final_output_mode = str(run_config.get("final_output_mode", "full_drawings"))
    if final_output_mode not in {"full_drawings", "crossing_pair_flags", "four_graph_flags"}:
        raise ValueError("unknown final_output_mode in run configuration")
    compact_context = context_from_run_config(run_config)
    colors = (tuple(int(color) for color in run_config["export_vertex_colors"])
              if run_config.get("export_vertex_colors") is not None else None)
    final_flag_labels = {}
    final_flag_prefix = ""
    if final_output_mode != "full_drawings":
        vertices = compact_context.graph_vertices
        for vertex in vertices:
            flag_vertex(vertex, 1)
        prefix = [str(len(vertices)), "0"]
        if colors is not None:
            if len(colors) != len(vertices):
                raise ValueError(f"expected {len(vertices)} vertex colors, got {len(colors)}")
            prefix.extend(str(color) for color in colors)
        final_flag_prefix = " ".join(prefix)
        final_flag_labels = crossing_label_table(
            compact_context.graph_edges, four_graph=final_output_mode == "four_graph_flags",
        )
    return RunContext(
        blocks=build_blocks_from_records(run_config["build_blocks"]),
        canonical_context=context_from_record(run_config["canonical_context"]),
        compact_context=compact_context,
        run_dir=str(config.RUNS_ROOT / run_id),
        include_full_keys=bool(run_config.get("debug_store_full_canonical_keys", False)),
        bucket_signature_properties=tuple(run_config.get("bucket_signature_properties", config.BUCKET_SIGNATURE_PROPERTIES)),
        shard_by_crossing_count=bool(run_config.get("shard_by_crossing_count", config.SHARD_BY_CROSSING_COUNT)),
        compound_shards=bool(run_config.get("compound_shards", config.COMPOUND_SHARDS)),
        shard_partition_prefix_hex=max(
            1,
            int(run_config.get("shard_partition_prefix_hex", config.SHARD_PARTITION_PREFIX_HEX)),
        ),
        worker_shard_flush_bytes=max(
            1,
            int(run_config.get("worker_shard_flush_bytes", config.WORKER_SHARD_FLUSH_BYTES)),
        ),
        checkpoint_shard_batch_records=int(run_config.get("checkpoint_shard_batch_records", config.CHECKPOINT_SHARD_BATCH_RECORDS)),
        checkpoint_shard_batch_bytes=int(run_config.get("checkpoint_shard_batch_bytes", config.CHECKPOINT_SHARD_BATCH_BYTES)),
        final_step_local_dedupe=bool(run_config.get("final_step_local_dedupe", config.FINAL_STEP_LOCAL_DEDUPE)),
        block_search_order=str(run_config.get("block_search_order", config.BLOCK_SEARCH_ORDER)),
        verify_intermediate_hash_collisions=bool(
            run_config.get("verify_intermediate_hash_collisions", config.VERIFY_INTERMEDIATE_HASH_COLLISIONS)
        ),
        final_output_mode=final_output_mode,
        final_flag_labels=final_flag_labels,
        final_flag_prefix=final_flag_prefix,
        profile_operations=bool(run_config.get("profile_operations", config.PROFILE_OPERATIONS)),
        export_vertex_colors=colors,
    )


def add_timing(profile: dict, key: str, seconds: float) -> None:
    if not profile.get("profile_operations", True):
        return
    timings = profile.setdefault("timings", {})
    timings[key] = timings.get(key, 0.0) + seconds


def simple_flag_invariant(flag: str) -> str:
    parts = flag.split()
    if len(parts) < 3:
        return flag
    count_index = None
    for idx in range(2, len(parts)):
        remaining = len(parts) - idx - 1
        try:
            count = int(parts[idx])
        except ValueError:
            continue
        if remaining == count:
            count_index = idx
            break
    if count_index is None:
        return " ".join(parts[:2])
    return " ".join(parts[: count_index + 1])


class ShardBuffer:
    def __init__(self, run_context: RunContext, worker_id: str):
        self.run_context = run_context
        self.worker_id = worker_id
        self.run_dir = config.RUNS_ROOT / Path(run_context.run_dir).name
        self.prefix_len = run_context.shard_partition_prefix_hex
        self.max_bytes = run_context.worker_shard_flush_bytes
        self.instance_hash = hashlib.sha256(f"{worker_id}:{time.time_ns()}".encode("utf-8")).hexdigest()[:16]
        self.records: dict[tuple[int, str, str], list[CompoundRawRecord | CompoundFlagRecord]] = {}
        self.approx_bytes = 0
        self.flush_index = 0
        self.shards: list[dict] = []
        self.metrics = {
            "flush_count": 0,
            "shard_files": 0,
            "records": 0,
            "approx_bytes": 0,
            "max_flush_approx_bytes": 0,
            "max_records_per_flush": 0,
            "max_files_per_flush": 0,
        }

    def partition(self, digest: str) -> str:
        return digest[: self.prefix_len]

    def add_compact(self, stage_index: int, bucket_hash: str, compact_payload: bytes) -> None:
        partition_hash = self.partition(bucket_hash)
        record = CompoundRawRecord(bucket_hash=bucket_hash, compact_payload=compact_payload)
        self.records.setdefault((stage_index, "compact_drawings", partition_hash), []).append(record)
        self.approx_bytes += len(compact_payload) + 68
        self.metrics["records"] += 1
        self.metrics["approx_bytes"] += len(compact_payload) + 68

    def add_flag(self, stage_index: int, flag: str) -> None:
        flag_bytes = flag.encode("utf-8")
        flag_hash = hashlib.sha256(flag_bytes).hexdigest()
        partition_hash = self.partition(flag_hash)
        invariant_key = simple_flag_invariant(flag)
        record = CompoundFlagRecord(flag_hash=flag_hash, invariant_key=invariant_key, flag=flag)
        self.records.setdefault((stage_index, "final_flags", partition_hash), []).append(record)
        self.approx_bytes += len(flag_bytes) + len(invariant_key) + 72
        self.metrics["records"] += 1
        self.metrics["approx_bytes"] += len(flag_bytes) + len(invariant_key) + 72

    def should_flush(self) -> bool:
        return self.approx_bytes >= self.max_bytes

    def flush(self) -> list[dict]:
        if not self.records:
            return []
        self.flush_index += 1
        flush_records = sum(len(records) for records in self.records.values())
        flush_bytes = self.approx_bytes
        flush_shards = []
        for (stage_index, record_kind, partition_hash), records in sorted(self.records.items()):
            digest_source = f"{self.instance_hash}:{self.worker_id}:{self.flush_index}:{stage_index}:{record_kind}:{partition_hash}"
            flush_hash = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]
            shard_path = incoming_partition_shard_path(
                self.run_dir,
                stage_index,
                partition_hash,
                self.worker_id,
                flush_hash,
            )
            if record_kind == "final_flags":
                byte_count = write_compound_flag_shard(shard_path, records)  # type: ignore[arg-type]
            else:
                byte_count = write_compound_raw_shard(shard_path, records)  # type: ignore[arg-type]
            shard = {
                "stage_index": stage_index,
                "bucket_hash": partition_hash,
                "kind": record_kind,
                "shard_path": str(shard_path.relative_to(self.run_dir)),
                "record_count": len(records),
                "byte_count": byte_count,
            }
            flush_shards.append(shard)
            self.shards.append(shard)
        self.metrics["flush_count"] += 1
        self.metrics["shard_files"] += len(flush_shards)
        self.metrics["max_flush_approx_bytes"] = max(self.metrics["max_flush_approx_bytes"], flush_bytes)
        self.metrics["max_records_per_flush"] = max(self.metrics["max_records_per_flush"], flush_records)
        self.metrics["max_files_per_flush"] = max(self.metrics["max_files_per_flush"], len(flush_shards))
        self.records = {}
        self.approx_bytes = 0
        return flush_shards


def apply_block(drawing, block: BuildBlock):
    block_profile = {
        "kind": block.kind,
        "vertex": block.vertex,
        "step_count": len(block.steps),
        "steps": [],
        "max_frontier": 1,
        "exact_dedupe_seen": 0,
        "exact_dedupe_kept": 0,
    }
    total_start = time.perf_counter()
    frontier = [drawing]
    for step_index, step in enumerate(block.steps):
        step_start = time.perf_counter()
        next_frontier = []
        seen_exact = set()
        step_profiles = []
        raw_outputs = 0
        exact_serialize_seconds = 0.0
        for current in frontier:
            extensions, step_profile = apply_step_profiled(current, step)
            step_profiles.append(step_profile)
            raw_outputs += len(extensions)
            for candidate in extensions:
                serialize_start = time.perf_counter()
                payload = candidate
                exact_serialize_seconds += time.perf_counter() - serialize_start
                if payload in seen_exact:
                    continue
                seen_exact.add(payload)
                next_frontier.append(candidate)
        merged_step_profile = merge_step_profiles(step_profiles)
        merged_step_profile.update(
            {
                "block_step_index": step_index,
                "frontier_in": len(frontier),
                "raw_outputs": raw_outputs,
                "frontier_out": len(next_frontier),
                "exact_dedupe_removed": raw_outputs - len(next_frontier),
                "exact_dedupe_serialize_seconds": exact_serialize_seconds,
                "block_step_seconds": time.perf_counter() - step_start,
            }
        )
        block_profile["steps"].append(merged_step_profile)
        block_profile["max_frontier"] = max(block_profile["max_frontier"], len(next_frontier))
        block_profile["exact_dedupe_seen"] += raw_outputs
        block_profile["exact_dedupe_kept"] += len(next_frontier)
        frontier = next_frontier
        if not frontier:
            break
    block_profile["output_count"] = len(frontier)
    block_profile["total_seconds"] = time.perf_counter() - total_start
    return frontier, block_profile


def flush_checkpoint_buckets(
    *,
    run_dir: Path,
    next_stage: int,
    buckets: dict[tuple[int | None, str], list[bytes]],
    worker_id: str,
    job_hash: str,
    batch_index: int,
    shards: list[dict],
    shard_buffer: ShardBuffer | None = None,
    flags_only: bool = False,
) -> tuple[int, int, int]:
    if not buckets:
        return 0, 0, 0
    compact_bytes = 0
    compressed_bytes = 0
    record_count = 0
    if shard_buffer is not None:
        for (_crossing_count, bucket_hash), records in sorted(buckets.items()):
            for record in records:
                if flags_only:
                    shard_buffer.add_flag(next_stage, record.decode("utf-8"))
                else:
                    shard_buffer.add_compact(next_stage, bucket_hash, record)
                if shard_buffer.should_flush():
                    shards.extend(shard_buffer.flush())
            compact_bytes += sum(len(record) for record in records)
            record_count += len(records)
        buckets.clear()
        return record_count, compact_bytes, compressed_bytes

    batch_hash = f"{job_hash}-{batch_index:06d}"
    for (crossing_count, bucket_hash), records in sorted(buckets.items()):
        shard_path = incoming_shard_path(
            run_dir,
            next_stage,
            bucket_hash,
            worker_id,
            batch_hash,
            crossing_count,
        )
        byte_count = (write_flag_shard(shard_path, [record.decode("utf-8") for record in records])
                      if flags_only else write_raw_shard(shard_path, records))
        records_compact_bytes = sum(len(record) for record in records)
        compressed_bytes += byte_count
        compact_bytes += records_compact_bytes
        record_count += len(records)
        shards.append(
            {
                "stage_index": next_stage,
                "bucket_hash": bucket_hash,
                "kind": "final_flags" if flags_only else "compact_drawings",
                "crossing_count": crossing_count,
                "shard_path": str(shard_path.relative_to(run_dir)),
                "record_count": len(records),
                "byte_count": byte_count,
            }
        )
    buckets.clear()
    return record_count, compact_bytes, compressed_bytes


def hash_seen_before(
    *,
    digest: bytes,
    payload: bytes,
    seen_hashes: set[bytes],
    seen_payloads: dict[bytes, bytes | set[bytes]] | None,
) -> tuple[bool, bool]:
    """Return (already_seen, true_hash_collision)."""
    if seen_payloads is None:
        if digest in seen_hashes:
            return True, False
        seen_hashes.add(digest)
        return False, False

    previous = seen_payloads.get(digest)
    if previous is None:
        seen_hashes.add(digest)
        seen_payloads[digest] = payload
        return False, False
    if isinstance(previous, bytes):
        if previous == payload:
            return True, False
        seen_payloads[digest] = {previous, payload}
        return False, True
    if payload in previous:
        return True, False
    previous.add(payload)
    return False, True


def make_checkpoint_writer(
    run_context: RunContext,
    stage_index: int,
    job: dict,
    shard_buffer: ShardBuffer | None = None,
):
    operation_clock = time.perf_counter if run_context.profile_operations else lambda: 0.0
    next_stage = stage_index + 1
    flags_only = next_stage == len(run_context.blocks) and run_context.final_output_mode != "full_drawings"
    flag_labels = run_context.final_flag_labels
    flag_prefix = run_context.final_flag_prefix
    run_dir = config.RUNS_ROOT / Path(run_context.run_dir).name
    job_hash = hashlib.sha256(job["job_key"].encode("utf-8")).hexdigest()[:16]
    worker_id = job.get("worker_id", "worker")
    batch_record_limit = max(1, run_context.checkpoint_shard_batch_records)
    batch_byte_limit = max(1, run_context.checkpoint_shard_batch_bytes)
    state = {
        "buckets": {},
        "seen_flags": set(),
        "bucket_record_counts": {},
        "shards": [],
        "flush_count": 0,
        "buffered_records": 0,
        "buffered_bytes": 0,
        "total_compact_bytes": 0,
        "total_compressed_bytes": 0,
        "total_written_records": 0,
        "max_batch_records": 0,
        "max_batch_bytes": 0,
        "write_seconds": 0.0,
        "bucket_seconds": 0.0,
    }

    def flush() -> None:
        if not state["buckets"]:
            return
        state["max_batch_records"] = max(state["max_batch_records"], state["buffered_records"])
        state["max_batch_bytes"] = max(state["max_batch_bytes"], state["buffered_bytes"])
        write_start = operation_clock()
        written, compact_bytes, compressed_bytes = flush_checkpoint_buckets(
            run_dir=run_dir,
            next_stage=next_stage,
            buckets=state["buckets"],
            worker_id=worker_id,
            job_hash=job_hash,
            batch_index=state["flush_count"],
            shards=state["shards"],
            shard_buffer=shard_buffer,
            flags_only=flags_only,
        )
        state["write_seconds"] += operation_clock() - write_start
        state["total_written_records"] += written
        state["total_compact_bytes"] += compact_bytes
        state["total_compressed_bytes"] += compressed_bytes
        state["flush_count"] += 1
        state["buffered_records"] = 0
        state["buffered_bytes"] = 0
        state["seen_flags"].clear()

    def emit(candidate=None, compact_payload: bytes | None = None, *, crossings=None) -> None:
        if flags_only:
            if crossings is None:
                raise TypeError("flag-only checkpoint emission requires crossings=")
            labels = sorted({flag_labels[crossing] for crossing in crossings})
            flag = " ".join([flag_prefix, str(len(labels)), *labels])
            if flag in state["seen_flags"]:
                return
            state["seen_flags"].add(flag)
            compact_payload = flag.encode("utf-8")
        elif crossings is not None:
            raise TypeError("drawing checkpoint emission does not accept crossings=")
        elif candidate is None:
            raise TypeError("drawing checkpoint emission requires a candidate")
        elif compact_payload is None:
            compact_payload = encode_normalized_drawing(candidate, next_stage, run_context.compact_context)
        bucket_start = operation_clock()
        crossing_count = len(candidate.crossings) if run_context.shard_by_crossing_count and not flags_only else None
        bucket_hash = (hashlib.sha256(compact_payload).hexdigest() if flags_only
                       else drawing_bucket_hash(candidate, run_context.bucket_signature_properties))
        bucket_key = (crossing_count, bucket_hash)
        state["buckets"].setdefault(bucket_key, []).append(compact_payload)
        count_key = (None, bucket_hash[:run_context.shard_partition_prefix_hex]) if flags_only else bucket_key
        state["bucket_record_counts"][count_key] = state["bucket_record_counts"].get(count_key, 0) + 1
        state["buffered_records"] += 1
        state["buffered_bytes"] += len(compact_payload)
        state["bucket_seconds"] += operation_clock() - bucket_start
        if state["buffered_records"] >= batch_record_limit or state["buffered_bytes"] >= batch_byte_limit:
            flush()

    return emit, flush, state


def apply_block_to_shards(
    drawing,
    block: BuildBlock,
    stage_index: int,
    run_context: RunContext,
    job: dict,
    profile: dict,
    shard_buffer: ShardBuffer | None = None,
):
    operation_clock = time.perf_counter if run_context.profile_operations else lambda: 0.0
    block_profile = {
        "kind": block.kind,
        "vertex": block.vertex,
        "step_count": len(block.steps),
        "steps": [],
        "max_frontier": 1,
        "exact_dedupe_seen": 0,
        "exact_dedupe_kept": 0,
    }
    total_start = time.perf_counter()
    frontier = [drawing]
    next_stage = stage_index + 1
    run_dir = config.RUNS_ROOT / Path(run_context.run_dir).name
    job_hash = hashlib.sha256(job["job_key"].encode("utf-8")).hexdigest()[:16]
    worker_id = job.get("worker_id", "worker")
    batch_record_limit = max(1, run_context.checkpoint_shard_batch_records)
    batch_byte_limit = max(1, run_context.checkpoint_shard_batch_bytes)
    shards: list[dict] = []
    total_compact_bytes = 0
    total_compressed_bytes = 0
    total_written_records = 0
    bucket_record_counts: dict[tuple[int | None, str], int] = {}
    flush_count = 0
    max_batch_records = 0
    max_batch_bytes = 0
    final_raw_outputs = 0
    final_kept_outputs = 0

    for step_index, step in enumerate(block.steps):
        step_start = operation_clock()
        is_final_step = step_index == len(block.steps) - 1
        next_frontier = [] if not is_final_step else None
        seen_exact = set()
        merged_step_profile = {}
        raw_outputs = 0
        kept_outputs = 0
        exact_serialize_seconds = 0.0
        bucket_seconds = 0.0
        write_seconds = 0.0
        buckets: dict[tuple[int | None, str], list[bytes]] = {}
        buffered_records = 0
        buffered_bytes = 0

        for current in frontier:
            candidates, step_profile = iter_step_profiled(current, step, profile_operations=run_context.profile_operations)
            for candidate in candidates:
                raw_outputs += 1
                if is_final_step:
                    serialize_start = operation_clock()
                    compact_payload = encode_normalized_drawing(candidate, next_stage, run_context.compact_context)
                    dedupe_key = hashlib.sha256(compact_payload).digest() if run_context.final_step_local_dedupe else None
                    exact_serialize_seconds += operation_clock() - serialize_start
                    if run_context.final_step_local_dedupe:
                        if dedupe_key in seen_exact:
                            continue
                        seen_exact.add(dedupe_key)
                    kept_outputs += 1

                    bucket_start = operation_clock()
                    crossing_count = len(candidate.crossings) if run_context.shard_by_crossing_count else None
                    bucket_hash = drawing_bucket_hash(candidate, run_context.bucket_signature_properties)
                    bucket_key = (crossing_count, bucket_hash)
                    buckets.setdefault(bucket_key, []).append(compact_payload)
                    bucket_record_counts[bucket_key] = bucket_record_counts.get(bucket_key, 0) + 1
                    buffered_records += 1
                    buffered_bytes += len(compact_payload)
                    bucket_seconds += operation_clock() - bucket_start

                    if buffered_records >= batch_record_limit or buffered_bytes >= batch_byte_limit:
                        max_batch_records = max(max_batch_records, buffered_records)
                        max_batch_bytes = max(max_batch_bytes, buffered_bytes)
                        write_start = operation_clock()
                        written, compact_bytes, compressed_bytes = flush_checkpoint_buckets(
                            run_dir=run_dir,
                            next_stage=next_stage,
                            buckets=buckets,
                            worker_id=worker_id,
                            job_hash=job_hash,
                            batch_index=flush_count,
                            shards=shards,
                            shard_buffer=shard_buffer,
                        )
                        write_seconds += operation_clock() - write_start
                        total_written_records += written
                        total_compact_bytes += compact_bytes
                        total_compressed_bytes += compressed_bytes
                        flush_count += 1
                        buffered_records = 0
                        buffered_bytes = 0
                else:
                    serialize_start = operation_clock()
                    payload = candidate
                    exact_serialize_seconds += operation_clock() - serialize_start
                    if payload in seen_exact:
                        continue
                    seen_exact.add(payload)
                    kept_outputs += 1
                    next_frontier.append(candidate)  # type: ignore[union-attr]

            if run_context.profile_operations:
                accumulate_step_profile(merged_step_profile, step_profile)

        if is_final_step:
            final_raw_outputs = raw_outputs
            final_kept_outputs = kept_outputs
            if buffered_records:
                max_batch_records = max(max_batch_records, buffered_records)
                max_batch_bytes = max(max_batch_bytes, buffered_bytes)
            write_start = operation_clock()
            written, compact_bytes, compressed_bytes = flush_checkpoint_buckets(
                run_dir=run_dir,
                next_stage=next_stage,
                buckets=buckets,
                worker_id=worker_id,
                job_hash=job_hash,
                batch_index=flush_count,
                shards=shards,
                shard_buffer=shard_buffer,
            )
            write_seconds += operation_clock() - write_start
            total_written_records += written
            total_compact_bytes += compact_bytes
            total_compressed_bytes += compressed_bytes
            if written:
                flush_count += 1

        merged_step_profile.update(
            {
                "block_step_index": step_index,
                "frontier_in": len(frontier),
                "raw_outputs": raw_outputs,
                "frontier_out": kept_outputs,
                "exact_dedupe_removed": raw_outputs - kept_outputs,
                "exact_dedupe_serialize_seconds": exact_serialize_seconds,
                "checkpoint_bucket_seconds": bucket_seconds,
                "checkpoint_write_shards_seconds": write_seconds,
                "block_step_seconds": operation_clock() - step_start,
            }
        )
        if not run_context.profile_operations:
            merged_step_profile = {key: value for key, value in merged_step_profile.items()
                                   if not key.endswith("_seconds")}
        block_profile["steps"].append(merged_step_profile)
        block_profile["exact_dedupe_seen"] += raw_outputs
        block_profile["exact_dedupe_kept"] += kept_outputs
        if is_final_step:
            block_profile["max_frontier"] = max(block_profile["max_frontier"], len(frontier))
        else:
            block_profile["max_frontier"] = max(block_profile["max_frontier"], len(next_frontier or []))
            frontier = next_frontier or []
            if not frontier:
                break

    block_profile["output_count"] = total_written_records
    block_profile["total_seconds"] = time.perf_counter() - total_start
    add_timing(profile, "checkpoint_bucket", sum(step.get("checkpoint_bucket_seconds", 0.0) for step in block_profile["steps"]))
    add_timing(profile, "checkpoint_write_shards", sum(step.get("checkpoint_write_shards_seconds", 0.0) for step in block_profile["steps"]))
    profile["checkpoint_bucket_count"] = len(bucket_record_counts)
    profile["checkpoint_bucket_max_size"] = max(bucket_record_counts.values(), default=0)
    profile["checkpoint_bucket_singletons"] = sum(1 for count in bucket_record_counts.values() if count == 1)
    profile["checkpoint_batch_flushes"] = flush_count
    profile["checkpoint_batch_max_records"] = max_batch_records
    profile["checkpoint_batch_max_bytes"] = max_batch_bytes
    profile["output_shard_files"] = len(shards)
    profile["output_shard_compressed_bytes"] = total_compressed_bytes
    profile["output_compact_payload_bytes"] = total_compact_bytes
    profile["checkpoint_candidates_before_local_reduce"] = final_raw_outputs
    profile["checkpoint_candidates_after_local_reduce"] = final_kept_outputs
    profile["checkpoint_local_reduce_removed"] = final_raw_outputs - final_kept_outputs
    return shards, block_profile


def apply_block_depth_first_to_shards(
    drawing,
    block: BuildBlock,
    stage_index: int,
    run_context: RunContext,
    job: dict,
    profile: dict,
    shard_buffer: ShardBuffer | None = None,
):
    operation_clock = time.perf_counter if run_context.profile_operations else lambda: 0.0
    block_profile = {
        "kind": block.kind,
        "vertex": block.vertex,
        "step_count": len(block.steps),
        "steps": [],
        "max_frontier": 1,
        "exact_dedupe_seen": 0,
        "exact_dedupe_kept": 0,
        "search_order": "dfs",
    }
    total_start = time.perf_counter()
    next_stage = stage_index + 1
    emit, flush, writer_state = make_checkpoint_writer(run_context, stage_index, job, shard_buffer=shard_buffer)
    step_profiles: list[dict] = [{} for _ in block.steps]
    flags_only = next_stage == len(run_context.blocks) and run_context.final_output_mode != "full_drawings"
    raw_outputs = [0 for _ in block.steps]
    kept_outputs = [0 for _ in block.steps]
    serialize_seconds = [0.0 for _ in block.steps]
    hash_duplicate_hits = [0 for _ in block.steps]
    true_hash_collisions = [0 for _ in block.steps]
    input_drawings = [0 for _ in block.steps]
    step_seconds = [0.0 for _ in block.steps]
    routes_seen = [0 for _ in block.steps]
    feasible_routes = [0 for _ in block.steps]
    seen_hashes = [set() for _ in block.steps]
    seen_payloads = [
        {} if run_context.verify_intermediate_hash_collisions else None
        for _ in block.steps
    ]

    def visit(current, depth: int) -> None:
        if depth >= len(block.steps):
            return
        step_start = operation_clock()
        input_drawings[depth] += 1
        is_final_step = depth == len(block.steps) - 1
        child_seconds = 0.0
        if is_final_step and flags_only:
            candidates, step_profile = iter_final_route_crossings_profiled(
                current,
                block.steps[depth],
                profile_operations=run_context.profile_operations,
            )
            for crossings in candidates:
                emit(crossings=crossings)
        else:
            candidates, step_profile = iter_step_profiled(
                current,
                block.steps[depth],
                profile_operations=run_context.profile_operations,
            )
            for candidate in candidates:
                raw_outputs[depth] += 1
                serialize_start = operation_clock()
                compact_payload = encode_normalized_drawing(
                    candidate,
                    next_stage if is_final_step else stage_index,
                    run_context.compact_context,
                )
                if is_final_step and not run_context.final_step_local_dedupe:
                    serialize_seconds[depth] += operation_clock() - serialize_start
                    kept_outputs[depth] += 1
                    emit(candidate, compact_payload)
                    continue
                digest = hashlib.sha256(compact_payload).digest()
                serialize_seconds[depth] += operation_clock() - serialize_start
                already_seen, true_collision = hash_seen_before(
                    digest=digest,
                    payload=compact_payload,
                    seen_hashes=seen_hashes[depth],
                    seen_payloads=seen_payloads[depth],
                )
                if already_seen:
                    hash_duplicate_hits[depth] += 1
                    continue
                if true_collision:
                    true_hash_collisions[depth] += 1
                kept_outputs[depth] += 1
                if is_final_step:
                    emit(candidate, compact_payload)
                else:
                    child_start = operation_clock()
                    visit(candidate, depth + 1)
                    child_seconds += operation_clock() - child_start
        if is_final_step and flags_only:
            routes_seen[depth] += int(step_profile.get("route_count", 0))
            feasible_routes[depth] += int(step_profile.get("feasible_route_count", 0))
        if run_context.profile_operations:
            accumulate_step_profile(step_profiles[depth], step_profile)
        step_seconds[depth] += operation_clock() - step_start - child_seconds

    visit(drawing, 0)
    flush()

    for depth, merged_step_profile in enumerate(step_profiles):
        merged_step_profile["block_step_index"] = depth
        merged_step_profile["frontier_in"] = input_drawings[depth]
        merged_step_profile["block_step_seconds"] = step_seconds[depth]
        if depth == len(block.steps) - 1 and flags_only:
            merged_step_profile.update(
                {
                    "count_kind": "routes",
                    "routes_seen": routes_seen[depth],
                    "feasible_routes": feasible_routes[depth],
                    "infeasible_routes": routes_seen[depth] - feasible_routes[depth],
                    "flag_route_emissions": feasible_routes[depth],
                }
            )
        else:
            merged_step_profile.update(
                {
                    "raw_outputs": raw_outputs[depth],
                    "frontier_out": kept_outputs[depth],
                    "exact_dedupe_removed": raw_outputs[depth] - kept_outputs[depth],
                    "exact_dedupe_serialize_seconds": serialize_seconds[depth],
                    "hash_duplicate_hits": hash_duplicate_hits[depth],
                    "true_hash_collisions": true_hash_collisions[depth],
                    "transposition_table_entries": len(seen_hashes[depth]),
                }
            )
        if depth == len(block.steps) - 1:
            merged_step_profile["checkpoint_bucket_seconds"] = writer_state["bucket_seconds"]
            merged_step_profile["checkpoint_write_shards_seconds"] = writer_state["write_seconds"]
        if not run_context.profile_operations:
            merged_step_profile = {key: value for key, value in merged_step_profile.items()
                                   if not key.endswith("_seconds")}
        block_profile["steps"].append(merged_step_profile)
        block_profile["exact_dedupe_seen"] += raw_outputs[depth]
        block_profile["exact_dedupe_kept"] += kept_outputs[depth]

    block_profile["output_kind"] = "flags_from_routes" if flags_only else "drawings"
    if flags_only:
        block_profile["routes_seen"] = sum(routes_seen)
        block_profile["feasible_routes"] = sum(feasible_routes)
        block_profile["flag_route_emissions"] = sum(feasible_routes)
        block_profile["flag_records_written"] = writer_state["total_written_records"]
    else:
        block_profile["output_count"] = writer_state["total_written_records"]
    block_profile["total_seconds"] = time.perf_counter() - total_start
    block_profile["max_frontier"] = len(block.steps) + 1
    block_profile["transposition_table_entries"] = sum(len(table) for table in seen_hashes)
    block_profile["hash_duplicate_hits"] = sum(hash_duplicate_hits)
    block_profile["true_hash_collisions"] = sum(true_hash_collisions)
    block_profile["verify_intermediate_hash_collisions"] = run_context.verify_intermediate_hash_collisions
    add_timing(profile, "checkpoint_bucket", float(writer_state["bucket_seconds"]))
    add_timing(profile, "checkpoint_write_shards", float(writer_state["write_seconds"]))
    profile["checkpoint_bucket_count"] = len(writer_state["bucket_record_counts"])
    profile["checkpoint_bucket_max_size"] = max(writer_state["bucket_record_counts"].values(), default=0)
    profile["checkpoint_bucket_singletons"] = sum(1 for count in writer_state["bucket_record_counts"].values() if count == 1)
    profile["checkpoint_batch_flushes"] = writer_state["flush_count"]
    profile["checkpoint_batch_max_records"] = writer_state["max_batch_records"]
    profile["checkpoint_batch_max_bytes"] = writer_state["max_batch_bytes"]
    profile["output_shard_files"] = len(writer_state["shards"])
    profile["output_shard_compressed_bytes"] = writer_state["total_compressed_bytes"]
    if flags_only:
        profile["output_flag_payload_bytes"] = writer_state["total_compact_bytes"]
    else:
        profile["output_compact_payload_bytes"] = writer_state["total_compact_bytes"]
    if flags_only:
        profile["checkpoint_routes_seen"] = routes_seen[-1] if routes_seen else 0
        profile["checkpoint_feasible_routes"] = feasible_routes[-1] if feasible_routes else 0
        profile["checkpoint_flags_written"] = writer_state["total_written_records"]
        profile["checkpoint_local_flag_duplicates_removed"] = (
            profile["checkpoint_feasible_routes"] - profile["checkpoint_flags_written"]
        )
    else:
        profile["checkpoint_candidates_before_local_reduce"] = raw_outputs[-1] if raw_outputs else 0
        profile["checkpoint_candidates_after_local_reduce"] = writer_state["total_written_records"]
        profile["checkpoint_local_reduce_removed"] = (raw_outputs[-1] - writer_state["total_written_records"]) if raw_outputs else 0
    profile["block_search_order"] = "dfs"
    profile["transposition_table_entries"] = block_profile["transposition_table_entries"]
    profile["hash_duplicate_hits"] = block_profile["hash_duplicate_hits"]
    profile["true_hash_collisions"] = block_profile["true_hash_collisions"]
    return writer_state["shards"], block_profile


def accumulate_step_profile(merged: dict, profile: dict) -> None:
    if not merged:
        merged.update(kind=profile.get("kind", ""), edge=list(profile.get("edge", ())), input_drawings=0)
    merged["input_drawings"] += 1
    sum_keys = [
        "enumerate_sequences_seconds",
        "make_extended_total_seconds",
        "make_extended_seconds",
        "drawing_indices_seconds",
        "face_choice_seconds",
        "path_construction_seconds",
        "route_preparation_seconds",
        "cartesian_product_seconds",
        "unique_presentation_seconds",
        "sequence_count",
        "route_count",
        "feasible_route_count",
        "infeasible_route_count",
        "flag_route_emissions",
        "sequence_arc_steps",
        "make_extended_calls",
        "make_extended_output_drawings",
        "cartesian_product_bound",
        "cartesian_product_outputs",
        "output_count",
    ]
    for key in sum_keys:
        merged[key] = merged.get(key, 0) + profile.get(key, 0)
    for key in ("max_sequence_length", "max_cartesian_product_bound"):
        merged[key] = max(merged.get(key, 0), profile.get(key, 0))


def merge_step_profiles(step_profiles: list[dict]) -> dict:
    merged = {}
    for profile in step_profiles:
        accumulate_step_profile(merged, profile)
    return merged


def process_job(job: dict, run_context: RunContext, shard_buffer: ShardBuffer | None = None) -> JobResult:
    start = time.perf_counter()
    cpu_start = time.process_time()
    profile = {
        "stage_index": int(job["stage_index"]),
        "job_key_bytes": len(job["job_key"]),
        "input_compact_blob_bytes": len(job.get("compact_blob_b64") or ""),
        "timings": {},
        "profile_operations": run_context.profile_operations,
    }
    stage_index = int(job["stage_index"])
    deserialize_start = time.perf_counter()
    compact_payload = decompress_db_blob(base64.b64decode(job["compact_blob_b64"]))
    drawing = decode_drawing(compact_payload, run_context.compact_context)
    add_timing(profile, "deserialize_job", time.perf_counter() - deserialize_start)
    candidates = []
    shards: list[dict] = []
    if stage_index >= len(run_context.blocks):
        block_profile = {"output_count": 0, "total_seconds": 0.0, "steps": []}
    else:
        block_start = time.perf_counter()
        next_stage = stage_index + 1
        final_flags_only = next_stage == len(run_context.blocks) and run_context.final_output_mode != "full_drawings"
        if final_flags_only or run_context.block_search_order == "dfs":
            shards, block_profile = apply_block_depth_first_to_shards(
                drawing,
                run_context.blocks[stage_index],
                stage_index,
                run_context,
                job,
                profile,
                shard_buffer=shard_buffer if run_context.compound_shards else None,
            )
            add_timing(profile, "apply_block", time.perf_counter() - block_start)
        elif run_context.block_search_order == "bfs":
            shards, block_profile = apply_block_to_shards(
                drawing,
                run_context.blocks[stage_index],
                stage_index,
                run_context,
                job,
                profile,
                shard_buffer=shard_buffer if run_context.compound_shards else None,
            )
            add_timing(profile, "apply_block", time.perf_counter() - block_start)
        else:
            raise ValueError(f"unknown block search order: {run_context.block_search_order!r}")
    profile["block"] = block_profile
    if block_profile.get("output_kind") == "flags_from_routes":
        profile["candidate_kind"] = "route"
        profile["candidate_count"] = int(profile.get("checkpoint_routes_seen", 0))
    else:
        profile["candidate_kind"] = "drawing"
        profile["candidate_count"] = int(profile.get("checkpoint_candidates_before_local_reduce", len(candidates)))
    profile["elapsed_seconds"] = time.perf_counter() - start
    profile["worker_cpu_seconds"] = time.process_time() - cpu_start
    profile["worker_max_rss_kib"] = process_max_rss_kib()
    return JobResult(
        job_key=job["job_key"],
        stage_index=stage_index,
        job_type="expand",
        candidates_seen=int(profile["candidate_count"]),
        candidates=candidates,
        shards=shards,
        reduction_summary=None,
        elapsed_seconds=time.perf_counter() - start,
        profile=profile,
    )


def process_reduce_job(job: dict, run_context: RunContext) -> JobResult:
    start = time.perf_counter()
    cpu_start = time.process_time()
    stage_index = int(job["stage_index"])
    run_dir = Path(run_context.run_dir)
    bucket_dir = run_dir / job["bucket_dir"]
    profile = {
        "stage_index": stage_index,
        "job_key_bytes": len(job["job_key"]),
        "bucket_hash": job.get("bucket_hash"),
        "bucket_dir": job.get("bucket_dir"),
        "timings": {},
        "profile_operations": run_context.profile_operations,
    }
    reduce_start = time.perf_counter()
    summary = reduce_bucket(Path(run_context.run_dir).name, stage_index, bucket_dir)
    add_timing(profile, "reduce_bucket", time.perf_counter() - reduce_start)
    profile["raw_records"] = summary.raw_records
    profile["reduced_records"] = summary.reduced_records
    profile["reduced_bytes"] = summary.reduced_bytes
    profile["raw_shard_files"] = summary.raw_shard_files
    profile["raw_shard_compressed_bytes"] = summary.raw_compressed_bytes
    profile["raw_payload_bytes"] = summary.raw_payload_bytes
    add_timing(profile, "reduce_read_shards", summary.read_seconds)
    add_timing(profile, "reduce_canonicalize", summary.canonicalize_seconds)
    add_timing(profile, "reduce_write_shard", summary.write_seconds)
    elapsed = time.perf_counter() - start
    profile["elapsed_seconds"] = elapsed
    profile["worker_cpu_seconds"] = time.process_time() - cpu_start
    profile["worker_max_rss_kib"] = process_max_rss_kib()
    return JobResult(
        job_key=job["job_key"],
        stage_index=stage_index,
        job_type="reduce",
        candidates_seen=summary.raw_records,
        candidates=[],
        shards=[],
        reduction_summary={
            "stage_index": summary.stage_index,
            "bucket_hash": summary.bucket_hash,
            "bucket_dir": summary.bucket_dir,
            "reduced_shard_path": summary.reduced_shard_path,
            "raw_records": summary.raw_records,
            "reduced_records": summary.reduced_records,
            "reduced_bytes": summary.reduced_bytes,
            "raw_shard_files": summary.raw_shard_files,
            "raw_compressed_bytes": summary.raw_compressed_bytes,
            "raw_payload_bytes": summary.raw_payload_bytes,
        },
        elapsed_seconds=elapsed,
        profile=profile,
    )


class CoordinatorClient:
    def __init__(self, host: str, port: int, worker_id: str, run_id: str):
        self.host = host
        self.port = port
        self.worker_id = worker_id
        self.run_id = run_id
        self.last_heartbeat = float("-inf")

    def run_complete_from_db(self) -> bool:
        db_path = config.RUNS_ROOT / self.run_id / "coordinator.sqlite"
        if not db_path.exists():
            return False
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        try:
            row = db.execute("SELECT value FROM run_status WHERE key='run_complete'").fetchone()
            return row is not None and row[0] == "1"
        except sqlite3.Error:
            return False
        finally:
            db.close()

    def completed_response(self, payload: dict) -> dict:
        response = {"ok": True, "global_done": True}
        if payload.get("type") == "GET_JOB":
            response["job"] = None
        return response

    def request(self, payload: dict) -> dict:
        payload.setdefault("worker_id", self.worker_id)
        deadline = time.monotonic() + config.COORDINATOR_CONNECT_RETRY_SECONDS
        attempt = 0
        while True:
            try:
                sock = socket.create_connection(
                    (self.host, self.port),
                    timeout=config.COORDINATOR_CONNECT_TIMEOUT_SECONDS,
                )
                break
            except OSError as exc:
                if self.run_complete_from_db():
                    print(
                        f"worker {self.worker_id}: coordinator unavailable but run {self.run_id} is complete; exiting",
                        flush=True,
                    )
                    return self.completed_response(payload)
                attempt += 1
                if time.monotonic() >= deadline:
                    raise ConnectionError(
                        f"could not connect to coordinator at {self.host}:{self.port} "
                        f"after {config.COORDINATOR_CONNECT_RETRY_SECONDS}s"
                    ) from exc
                sleep_seconds = min(2**min(attempt - 1, 4), 10)
                print(
                    f"worker {self.worker_id}: coordinator {self.host}:{self.port} unavailable "
                    f"({exc}); retrying in {sleep_seconds}s",
                    flush=True,
                )
                time.sleep(sleep_seconds)
        with sock:
            sock.settimeout(3600)
            sock.sendall((json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
            data = b""
            while not data.endswith(b"\n"):
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
        if not data:
            if self.run_complete_from_db():
                print(
                    f"worker {self.worker_id}: coordinator closed connection after run {self.run_id} completed; exiting",
                    flush=True,
                )
                return self.completed_response(payload)
            raise RuntimeError("coordinator returned no data")
        response = json.loads(data.decode("utf-8"))
        if not response.get("ok", False):
            raise RuntimeError(response.get("error", "coordinator request failed"))
        return response

    def heartbeat(self, current_job_key: str | None, stats: dict, current_job_keys: list[str] | None = None) -> dict:
        response = self.request(
            {
                "type": "HEARTBEAT",
                "hostname": socket_module.gethostname(),
                "current_job_key": current_job_key,
                "current_job_keys": current_job_keys or ([] if current_job_key is None else [current_job_key]),
                "stats": stats,
            }
        )

        self.last_heartbeat = time.monotonic()
        return response


def start_heartbeat_loop(
    client: CoordinatorClient,
    current_job_key: str,
    stats: dict,
    current_job_keys: list[str] | None = None,
) -> tuple[threading.Event, threading.Event]:
    stop = threading.Event()
    shutdown_requested = threading.Event()

    def run() -> None:
        while not stop.wait(config.WORKER_HEARTBEAT_SECONDS):
            try:
                response = client.heartbeat(current_job_key, stats, current_job_keys=current_job_keys)
                if response.get("shutdown"):
                    shutdown_requested.set()
                    return
            except Exception as exc:
                print(
                    f"worker {client.worker_id}: heartbeat failed for {current_job_key}: {exc!r}",
                    flush=True,
                )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return stop, shutdown_requested


def run_network_worker(worker_id: str, host: str, port: int, idle_exit_seconds: int, run_id: str) -> None:
    run_context = load_run_context(run_id)
    client = CoordinatorClient(host, port, worker_id, run_id)
    idle_since = time.time()
    stats = {"jobs_done": 0, "candidates_seen": 0, "candidates_accepted": 0}
    shard_buffer = ShardBuffer(run_context, worker_id)
    pending_results: list[JobResult] = []
    pending_shards: list[dict] = []
    pending_job_keys: list[str] = []
    heartbeat_stop: threading.Event | None = None
    shutdown_requested: threading.Event | None = None

    def start_or_renew_buffer_heartbeat() -> None:
        nonlocal heartbeat_stop, shutdown_requested
        if not pending_job_keys:
            return
        display_job_key = pending_job_keys[0]
        # Throttle per-job announcements; the loop renews long-running jobs.
        if time.monotonic() - client.last_heartbeat >= config.WORKER_HEARTBEAT_SECONDS:
            client.heartbeat(display_job_key, stats, current_job_keys=pending_job_keys)
        if heartbeat_stop is None:
            heartbeat_stop, shutdown_requested = start_heartbeat_loop(
                client,
                display_job_key,
                stats,
                current_job_keys=pending_job_keys,
            )

    def stop_buffer_heartbeat() -> None:
        nonlocal heartbeat_stop, shutdown_requested
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        heartbeat_stop = None
        shutdown_requested = None

    def flush_pending() -> dict:
        nonlocal shard_buffer, pending_results, pending_shards, pending_job_keys
        if not pending_results:
            stop_buffer_heartbeat()
            return {"ok": True, "global_done": False}
        final_flush_start = time.perf_counter()
        final_flush_shards = shard_buffer.flush()
        final_flush_seconds = time.perf_counter() - final_flush_start
        pending_shards.extend(final_flush_shards)
        if final_flush_shards:
            add_timing(pending_results[-1].profile, "checkpoint_write_shards", final_flush_seconds)
            add_timing(pending_results[-1].profile, "checkpoint_final_flush", final_flush_seconds)
        for result in pending_results:
            result.profile["checkpoint_buffered_jobs"] = 0
            result.profile["checkpoint_flush_count"] = 0
            result.profile["checkpoint_shard_files_written"] = 0
            result.profile["checkpoint_flush_approx_bytes"] = 0
            result.profile["checkpoint_max_records_per_flush"] = 0
            result.profile["checkpoint_max_files_per_flush"] = 0
        pending_results[-1].profile["checkpoint_buffered_jobs"] = len(pending_results)
        pending_results[-1].profile["checkpoint_flush_count"] = shard_buffer.metrics["flush_count"]
        pending_results[-1].profile["checkpoint_shard_files_written"] = shard_buffer.metrics["shard_files"]
        pending_results[-1].profile["checkpoint_flush_approx_bytes"] = shard_buffer.metrics["approx_bytes"]
        pending_results[-1].profile["checkpoint_max_flush_approx_bytes"] = shard_buffer.metrics["max_flush_approx_bytes"]
        pending_results[-1].profile["checkpoint_max_records_per_flush"] = shard_buffer.metrics["max_records_per_flush"]
        pending_results[-1].profile["checkpoint_max_files_per_flush"] = shard_buffer.metrics["max_files_per_flush"]
        flush_key = "flush:" + hashlib.sha256(",".join(pending_job_keys).encode("utf-8")).hexdigest()[:16]
        submit = client.request(
            {
                "type": "SUBMIT_SHARDS",
                "job_key": flush_key,
                "stage_index": pending_results[-1].stage_index,
                "elapsed_seconds": sum(result.elapsed_seconds for result in pending_results),
                "shards": pending_shards,
                "job_results": [
                    {
                        "job_key": result.job_key,
                        "stage_index": result.stage_index,
                        "elapsed_seconds": result.elapsed_seconds,
                        "candidates_seen": result.candidates_seen,
                        "candidates_submitted": int(
                            result.profile.get("checkpoint_flags_written", result.candidates_seen)
                        ),
                        "candidates_kind": result.profile.get("candidate_kind", "drawing"),
                        "shards": 0,
                        "shard_bytes": 0,
                        "profile": result.profile,
                    }
                    for result in pending_results
                ],
            }
        )
        stats["jobs_done"] += len(pending_results)
        stats["candidates_seen"] += sum(result.candidates_seen for result in pending_results)
        stats["candidates_accepted"] += int(submit.get("accepted", 0))
        close = client.request(
            {
                "type": "CLOSE_JOBS",
                "jobs": [
                    {
                        "job_key": result.job_key,
                        "job_type": result.job_type,
                        "stage_index": result.stage_index,
                    }
                    for result in pending_results
                ],
            }
        )
        stop_buffer_heartbeat()
        pending_results = []
        pending_shards = []
        pending_job_keys.clear()
        shard_buffer = ShardBuffer(run_context, worker_id)
        return close

    while True:
        response = client.request({"type": "GET_JOB"})
        if response.get("shutdown"):
            flush_pending()
            return
        job = response.get("job")
        if job is None:
            if pending_results:
                close = flush_pending()
                if close.get("global_done") or response.get("global_done"):
                    return
                continue
            if response.get("global_done"):
                return
            if time.monotonic() - client.last_heartbeat >= config.WORKER_HEARTBEAT_SECONDS:
                heartbeat_response = client.heartbeat(None, stats)
                if heartbeat_response.get("shutdown"):
                    return
            if idle_exit_seconds and time.time() - idle_since >= idle_exit_seconds:
                return
            time.sleep(2)
            continue
        idle_since = time.time()
        if job.get("job_type") == "reduce":
            job["worker_id"] = worker_id
            if time.monotonic() - client.last_heartbeat >= config.WORKER_HEARTBEAT_SECONDS:
                client.heartbeat(job["job_key"], stats)
            heartbeat_stop, shutdown_requested = start_heartbeat_loop(client, job["job_key"], stats)
            try:
                result = process_reduce_job(job, run_context)
                if shutdown_requested.is_set():
                    return
                submit = client.request(
                    {
                        "type": "SUBMIT_REDUCTION",
                        "job_key": result.job_key,
                        "stage_index": result.stage_index,
                        "elapsed_seconds": result.elapsed_seconds,
                        "summary": result.reduction_summary,
                        "profile": result.profile,
                    }
                )
            finally:
                heartbeat_stop.set()
            stats["jobs_done"] += 1
            stats["candidates_seen"] += result.candidates_seen
            stats["candidates_accepted"] += int(submit.get("accepted", 0))
            close = client.request(
                {
                    "type": "CLOSE_JOB",
                    "job_key": result.job_key,
                    "job_type": result.job_type,
                    "stage_index": result.stage_index,
                }
            )
            if close.get("global_done"):
                return
            continue

        job["worker_id"] = worker_id
        pending_job_keys.append(job["job_key"])
        start_or_renew_buffer_heartbeat()
        result = process_job(job, run_context, shard_buffer)
        pending_results.append(result)
        pending_shards.extend(result.shards)
        if shutdown_requested is not None and shutdown_requested.is_set():
            flush_pending()
            return
        if result.shards:
            close = flush_pending()
            if close.get("global_done"):
                return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--host", default=config.COORDINATOR_HOST)
    parser.add_argument("--port", type=int, default=config.COORDINATOR_PORT)
    parser.add_argument("--idle-exit-seconds", type=int, default=config.WORKER_IDLE_EXIT_SECONDS)
    args = parser.parse_args()
    run_id = args.run_id or config.CURRENT_RUN_FILE.read_text(encoding="utf-8").strip()
    run_network_worker(args.worker_id, args.host, args.port, args.idle_exit_seconds, run_id)


if __name__ == "__main__":
    main()
