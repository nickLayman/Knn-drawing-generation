#!/usr/bin/env python3
"""Generate and measure a complete-bipartite drawing and flag census.

The drawing stage is the existing planarization-based graph-drawings pipeline.
This driver adds resumable export, square-case side-swap accounting, and exact
parallel flag.cpp reductions for crossing-pair and 4-graph flags under both
color conventions.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import gzip
import json
import multiprocessing
import os
from pathlib import Path
import resource
import shutil
import socket
import subprocess
import sys
import tempfile
import time

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from graph_drawings import config
from graph_drawings.automorphism import build_block_to_record, context_to_record, make_canonical_context
from graph_drawings.build_plan import base_drawings_for_cycle, derive_build_blocks, derive_build_plan
from graph_drawings.canonical import HASH_ALGORITHM, drawing_keys
from graph_drawings.compact import CompactContext, compress_db_blob, context_from_run_config, decode_drawing, encode_drawing
from graph_drawings.complete_bipartite_census import (
    bipartition,
    complete_bipartite_graph,
    flag_invariant_bucket,
    line_count,
    sha256,
    side_swap_analysis,
)
from graph_drawings.drawing import normalize_edge
from graph_drawings.flag_formats import crossing_pair_flag, four_graph_flag
from graph_drawings.io_utils import create_run_dir, make_run_id, write_json
from graph_drawings.local_runner import run_local
from graph_drawings.serialization import drawing_summary
from graph_drawings.shards import iter_reduced_shard
from graph_drawings.status import connect_db, init_schema, insert_job, insert_reported_class, set_status


_EXPORT_RUN_CONFIG: dict | None = None
_EXPORT_CONTEXT = None
_EXPORT_M = 0
_EXPORT_N = 0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(f"{path}.partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return value


def usage_snapshot() -> dict:
    self_usage = resource.getrusage(resource.RUSAGE_SELF)
    child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "self_user_seconds": self_usage.ru_utime,
        "self_system_seconds": self_usage.ru_stime,
        "children_user_seconds": child_usage.ru_utime,
        "children_system_seconds": child_usage.ru_stime,
        "self_max_rss_kib": self_usage.ru_maxrss,
        "children_max_rss_kib": child_usage.ru_maxrss,
    }


def measured_call(function, *args, **kwargs):
    before = usage_snapshot()
    started = time.perf_counter()
    value = function(*args, **kwargs)
    after = usage_snapshot()
    return value, {
        "wall_seconds": time.perf_counter() - started,
        "self_cpu_seconds": (
            after["self_user_seconds"] + after["self_system_seconds"]
            - before["self_user_seconds"] - before["self_system_seconds"]
        ),
        "children_cpu_seconds": (
            after["children_user_seconds"] + after["children_system_seconds"]
            - before["children_user_seconds"] - before["children_system_seconds"]
        ),
        "self_max_rss_kib": after["self_max_rss_kib"],
        "children_max_rss_kib": after["children_max_rss_kib"],
    }


def git_revision(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=config.PROJECT_ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "source revision is unavailable; pass --source-revision when running "
            "from a deployment without .git"
        )
    return result.stdout.strip()


def initialize_run(run_id: str, m: int, n: int, workers: int, source_revision: str) -> Path:
    vertices, graph_edges, colors = complete_bipartite_graph(m, n)
    config.validate_entrypoint_runtime(operation="complete_bipartite_census_init", edges=graph_edges)
    run_dir = config.RUNS_ROOT / run_id
    run_config_path = run_dir / "run_config.json"
    if run_config_path.is_file():
        existing = load_json(run_config_path)
        if existing.get("graph_edges") != [list(edge) for edge in graph_edges]:
            raise ValueError(f"existing run {run_id!r} is for a different graph")
        if existing.get("final_output_mode") != "full_drawings":
            raise ValueError(f"existing run {run_id!r} does not retain full drawings")
        return run_dir
    if run_dir.exists() and any(run_dir.iterdir()):
        raise ValueError(f"existing nonempty run directory has no run_config.json: {run_dir}")

    run_dir = create_run_dir(run_id)
    base_cycle, steps = derive_build_plan(graph_edges)
    blocks = derive_build_blocks(base_cycle, steps)
    context = make_canonical_context(
        vertices,
        {normalize_edge(edge) for edge in graph_edges},
        base_cycle,
        blocks,
    )
    base_drawings = base_drawings_for_cycle(base_cycle)
    normalized_edges = tuple(normalize_edge(edge) for edge in graph_edges)
    compact_context = CompactContext(
        graph_vertices=vertices,
        graph_edges=normalized_edges,
        canonical_context=context,
        vertex_to_id={vertex: index for index, vertex in enumerate(vertices)},
        edge_to_id={edge: index for index, edge in enumerate(normalized_edges)},
    )

    db = connect_db(run_dir / "coordinator.sqlite")
    init_schema(db)
    for drawing in base_drawings:
        keys = drawing_keys(0, drawing, context, include_full_keys=False)
        compact_blob = compress_db_blob(encode_drawing(drawing, 0, compact_context))
        insert_reported_class(
            db,
            stage_index=0,
            report_hash=keys.report_hash,
            work_hash=keys.work_hash,
            compact_blob=compact_blob,
            source_job_key=None,
            source_worker="census-init",
        )
        if blocks:
            insert_job(
                db,
                stage_index=0,
                report_hash=keys.report_hash,
                work_hash=keys.work_hash,
                compact_blob=compact_blob,
            )
    set_status(db, "run_id", run_id)
    set_status(db, "graph_name", f"K{m}_{n}")
    set_status(db, "total_steps", str(len(blocks)))
    db.commit()
    db.close()

    run_config = {
        "run_id": run_id,
        "graph_name": f"K{m}_{n}",
        "graph_vertices": list(vertices),
        "graph_edges": [list(edge) for edge in graph_edges],
        "export_vertex_colors": list(colors),
        "final_output_mode": "full_drawings",
        "final_flag_canonicalization": "none",
        "base_cycle": list(base_cycle),
        "build_steps": [{"kind": step.kind, "edge": list(step.edge)} for step in steps],
        "build_blocks": [build_block_to_record(block) for block in blocks],
        "edge_steps": len(steps),
        "total_steps": len(blocks),
        "base_drawing_summaries": [drawing_summary(drawing) for drawing in base_drawings],
        "canonicalization_policy": config.CANONICALIZATION_POLICY,
        "canonical_backend": config.CANONICAL_BACKEND,
        "canonical_key_strategy": config.CANONICAL_KEY_STRATEGY,
        "storage_mode": config.STORAGE_MODE,
        "compact_storage_version": config.COMPACT_STORAGE_VERSION,
        "compact_primary": config.COMPACT_PRIMARY,
        "shard_compression": config.SHARD_COMPRESSION,
        "db_blob_compression": config.DB_BLOB_COMPRESSION,
        "delete_raw_shards_after_reduce": config.DELETE_RAW_SHARDS_AFTER_REDUCE,
        "compound_shards": config.COMPOUND_SHARDS,
        "shard_partition_prefix_hex": config.SHARD_PARTITION_PREFIX_HEX,
        "worker_shard_flush_bytes": config.WORKER_SHARD_FLUSH_BYTES,
        "stage_barriers": config.STAGE_BARRIERS,
        "shard_by_crossing_count": config.SHARD_BY_CROSSING_COUNT,
        # Faces are deliberately excluded here.  At the final stage the strong
        # identity is the ordered crossing sequence on each edge, and two
        # equivalent planarization presentations need not have identical face
        # lists.
        "bucket_signature_properties": ["path-crossing-counts"],
        "report_hash_algorithm": HASH_ALGORITHM,
        "debug_store_full_canonical_keys": False,
        "compact_run_db_after_completion": config.COMPACT_RUN_DB_AFTER_COMPLETION,
        "delete_transient_rows_after_completion": config.DELETE_TRANSIENT_ROWS_AFTER_COMPLETION,
        "checkpoint_shard_batch_records": config.CHECKPOINT_SHARD_BATCH_RECORDS,
        "checkpoint_shard_batch_bytes": config.CHECKPOINT_SHARD_BATCH_BYTES,
        "final_step_local_dedupe": config.FINAL_STEP_LOCAL_DEDUPE,
        "block_search_order": config.BLOCK_SEARCH_ORDER,
        "verify_intermediate_hash_collisions": config.VERIFY_INTERMEDIATE_HASH_COLLISIONS,
        "final_drawing_key_strategy": "crossing_order_paths",
        "canonical_context": context_to_record(context),
        "full_graph_automorphism_count": len(context.full_automorphisms),
        "report_perms_by_stage": [len(stage.report_perms) for stage in context.stages],
        "work_perms_by_stage": [len(stage.work_perms) for stage in context.stages],
        "worker_count": 0,
        "local_process_count": workers,
        "project_root": str(config.PROJECT_ROOT),
        "runtime_target": config.RUNTIME_TARGET,
        "shared_root": str(config.SHARED_ROOT),
        "results_root": str(config.RESULTS_ROOT),
        "scratch_root": str(config.SCRATCH_ROOT),
        "python_bin": str(config.PYTHON_BIN),
        "profile_operations": False,
        "source_revision": source_revision,
        "census_graph": {"m": m, "n": n},
        "created_at": time.time(),
    }
    write_json(run_config_path, run_config)
    config.CURRENT_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
    config.CURRENT_RUN_FILE.write_text(run_id + "\n", encoding="utf-8")
    return run_dir


def record_stage(manifest_path: Path, name: str, function, *args, **kwargs):
    manifest = load_json(manifest_path)
    attempt = {"started_at": utc_now(), "status": "running"}
    manifest.setdefault("stages", {}).setdefault(name, []).append(attempt)
    atomic_json(manifest_path, manifest)
    try:
        value, measurements = measured_call(function, *args, **kwargs)
    except BaseException as error:
        manifest = load_json(manifest_path)
        attempt = manifest["stages"][name][-1]
        attempt.update({"finished_at": utc_now(), "status": "failed", "error": repr(error)})
        manifest["status"] = "failed"
        atomic_json(manifest_path, manifest)
        raise
    manifest = load_json(manifest_path)
    attempt = manifest["stages"][name][-1]
    attempt.update({"finished_at": utc_now(), "status": "complete", **measurements})
    manifest["status"] = "running"
    atomic_json(manifest_path, manifest)
    return value


def final_shards(run_dir: Path, final_stage: int) -> list[dict]:
    db = connect_db(run_dir / "coordinator.sqlite")
    try:
        rows = db.execute(
            """
            SELECT shard_path, record_count, byte_count
            FROM shard_manifests
            WHERE stage_index=? AND kind='reduced'
            ORDER BY shard_path
            """,
            (final_stage,),
        ).fetchall()
    finally:
        db.close()
    if not rows:
        raise RuntimeError("completed drawing run has no final reduced shards")
    return [dict(row) for row in rows]


def init_export_worker(run_config: dict, m: int, n: int) -> None:
    global _EXPORT_RUN_CONFIG, _EXPORT_CONTEXT, _EXPORT_M, _EXPORT_N
    _EXPORT_RUN_CONFIG = run_config
    _EXPORT_CONTEXT = context_from_run_config(run_config)
    _EXPORT_M = m
    _EXPORT_N = n


def export_shard(task: tuple[int, str, str]) -> dict:
    index, source_name, output_name = task
    if _EXPORT_CONTEXT is None or _EXPORT_RUN_CONFIG is None:
        raise RuntimeError("export worker was not initialized")
    source_path = Path(source_name)
    output_root = Path(output_name)
    stats_path = output_root / f"shard-{index:06d}.json"
    crossing_path = output_root / f"shard-{index:06d}.crossing.tsv"
    four_path = output_root / f"shard-{index:06d}.four.tsv"
    if stats_path.is_file() and crossing_path.is_file() and four_path.is_file():
        return load_json(stats_path)

    output_root.mkdir(parents=True, exist_ok=True)
    crossing_partial = Path(f"{crossing_path}.partial")
    four_partial = Path(f"{four_path}.partial")
    crossing_partial.unlink(missing_ok=True)
    four_partial.unlink(missing_ok=True)
    a_vertices, b_vertices = bipartition(_EXPORT_M, _EXPORT_N)
    colors = (1,) * _EXPORT_M + (2,) * _EXPORT_N
    orientations = (colors,)
    if _EXPORT_M == _EXPORT_N:
        orientations = (colors, (2,) * _EXPORT_M + (1,) * _EXPORT_N)

    started = time.perf_counter()
    cpu_started = time.process_time()
    drawing_count = 0
    profile_candidates = 0
    fixed_by_side_swap = 0
    crossing_rows = 0
    four_rows = 0
    with crossing_partial.open("x", encoding="utf-8") as crossing_output:
        with four_partial.open("x", encoding="utf-8") as four_output:
            for record in iter_reduced_shard(source_path):
                drawing = decode_drawing(record.compact_payload, _EXPORT_CONTEXT)
                drawing_count += 1
                if _EXPORT_M == _EXPORT_N:
                    candidate, fixed = side_swap_analysis(drawing, a_vertices, b_vertices)
                    profile_candidates += int(candidate)
                    fixed_by_side_swap += int(fixed)
                for orientation in orientations:
                    crossing = crossing_pair_flag(drawing, vertex_colors=orientation)
                    four = four_graph_flag(drawing, vertex_colors=orientation)
                    crossing_output.write(
                        f"{flag_invariant_bucket(crossing, colors_blind=False)}\t{crossing}\n"
                    )
                    four_output.write(
                        f"{flag_invariant_bucket(four, colors_blind=False)}\t{four}\n"
                    )
                    crossing_rows += 1
                    four_rows += 1
    crossing_partial.replace(crossing_path)
    four_partial.replace(four_path)
    worker_usage = resource.getrusage(resource.RUSAGE_SELF)
    result = {
        "shard_index": index,
        "source_path": str(source_path),
        "drawing_count": drawing_count,
        "side_swap_profile_candidates": profile_candidates,
        "fixed_by_side_swap": fixed_by_side_swap,
        "crossing_rows": crossing_rows,
        "four_rows": four_rows,
        "wall_seconds": time.perf_counter() - started,
        "cpu_seconds": time.process_time() - cpu_started,
        "max_rss_kib": worker_usage.ru_maxrss,
        "crossing_path": str(crossing_path),
        "four_path": str(four_path),
    }
    atomic_json(stats_path, result)
    return result


def export_all_shards(
    run_dir: Path,
    run_config: dict,
    m: int,
    n: int,
    workers: int,
) -> dict:
    rows = final_shards(run_dir, int(run_config["total_steps"]))
    output_root = run_dir / "census" / "export-shards"
    tasks = [
        (index, str(run_dir / row["shard_path"]), str(output_root))
        for index, row in enumerate(rows)
    ]
    context = multiprocessing.get_context("fork")
    results = []
    with context.Pool(
        processes=min(workers, len(tasks)),
        initializer=init_export_worker,
        initargs=(run_config, m, n),
    ) as pool:
        for completed, result in enumerate(pool.imap_unordered(export_shard, tasks), start=1):
            results.append(result)
            print(
                f"exported final shard {completed}/{len(tasks)}: "
                f"{result['drawing_count']:,} drawings",
                flush=True,
            )
    results.sort(key=lambda value: value["shard_index"])
    summary = {
        "shard_count": len(results),
        "drawing_count": sum(value["drawing_count"] for value in results),
        "side_swap_profile_candidates": sum(
            value["side_swap_profile_candidates"] for value in results
        ),
        "fixed_by_side_swap": sum(value["fixed_by_side_swap"] for value in results),
        "crossing_rows": sum(value["crossing_rows"] for value in results),
        "four_rows": sum(value["four_rows"] for value in results),
        "worker_wall_seconds_sum": sum(value["wall_seconds"] for value in results),
        "worker_cpu_seconds_sum": sum(value["cpu_seconds"] for value in results),
        "worker_max_rss_kib": max((value["max_rss_kib"] for value in results), default=0),
    }
    atomic_json(run_dir / "census" / "export-summary.json", summary)
    return summary


def run_command(command: list[str], *, cwd: Path, log_path: Path, environment: dict | None = None) -> dict:
    before = usage_snapshot()
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=environment,
        )
    after = usage_snapshot()
    return {
        "command": command,
        "wall_seconds": time.perf_counter() - started,
        "children_cpu_seconds": (
            after["children_user_seconds"] + after["children_system_seconds"]
            - before["children_user_seconds"] - before["children_system_seconds"]
        ),
        "children_max_rss_kib": after["children_max_rss_kib"],
        "log_path": str(log_path),
    }


def sort_fixed_rows(
    run_dir: Path,
    kind: str,
    workers: int,
    sort_memory: str,
    scratch_root: Path,
) -> dict:
    output_dir = run_dir / "census" / "sorted"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{kind}-color-preserving.tsv"
    marker_path = output_dir / f"{kind}-color-preserving.json"
    if output_path.is_file() and marker_path.is_file():
        return load_json(marker_path)
    inputs = sorted((run_dir / "census" / "export-shards").glob(f"shard-*.{kind}.tsv"))
    if not inputs:
        raise RuntimeError(f"no exported {kind} shards were found")
    scratch_root.mkdir(parents=True, exist_ok=True)
    partial = Path(f"{output_path}.partial")
    partial.unlink(missing_ok=True)
    environment = dict(os.environ)
    environment["LC_ALL"] = "C"
    command = [
        "sort",
        "-u",
        f"--parallel={workers}",
        "-S",
        sort_memory,
        "-T",
        str(scratch_root),
        "-o",
        str(partial),
        *(str(path) for path in inputs),
    ]
    measurements = run_command(
        command,
        cwd=config.PROJECT_ROOT,
        log_path=run_dir / "census" / "logs" / f"sort-{kind}.log",
        environment=environment,
    )
    partial.replace(output_path)
    result = {
        **measurements,
        "input_shards": len(inputs),
        "unique_labeled_rows": line_count(output_path),
        "output_path": str(output_path),
        "output_bytes": output_path.stat().st_size,
        "sha256": sha256(output_path),
    }
    atomic_json(marker_path, result)
    return result


def split_sorted_buckets(sorted_path: Path, output_dir: Path) -> dict:
    marker_path = output_dir / "complete.json"
    if marker_path.is_file():
        return load_json(marker_path)
    if output_dir.exists():
        raise ValueError(f"incomplete bucket directory needs inspection: {output_dir}")
    building = Path(f"{output_dir}.building-{os.getpid()}")
    building.mkdir(parents=True)
    buckets = []
    current_key = None
    output = None
    try:
        with sorted_path.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                key, separator, flag = line.partition("\t")
                if not separator:
                    raise ValueError(f"sorted row {line_number} has no bucket separator")
                if key != current_key:
                    if output is not None:
                        output.close()
                    filename = f"bucket-{key}.txt"
                    output = (building / filename).open("x", encoding="utf-8")
                    buckets.append({"key": key, "filename": filename, "row_count": 0})
                    current_key = key
                output.write(flag)
                buckets[-1]["row_count"] += 1
        if output is not None:
            output.close()
            output = None
        marker = {
            "bucket_count": len(buckets),
            "input_rows": sum(bucket["row_count"] for bucket in buckets),
            "buckets": buckets,
        }
        atomic_json(building / "complete.json", marker)
        building.replace(output_dir)
        return marker
    finally:
        if output is not None:
            output.close()


def split_blind_buckets(source_path: Path, output_dir: Path) -> dict:
    marker_path = output_dir / "complete.json"
    if marker_path.is_file():
        return load_json(marker_path)
    if output_dir.exists():
        raise ValueError(f"incomplete bucket directory needs inspection: {output_dir}")
    building = Path(f"{output_dir}.building-{os.getpid()}")
    building.mkdir(parents=True)
    counts: dict[str, int] = {}
    handles = {}
    with ExitStack() as stack:
        with gzip.open(source_path, "rt", encoding="utf-8") as source:
            for flag in source:
                flag = flag.rstrip("\n")
                key = flag_invariant_bucket(flag, colors_blind=True)
                if key not in handles:
                    handles[key] = stack.enter_context(
                        (building / f"bucket-{key}.txt").open("x", encoding="utf-8")
                    )
                    counts[key] = 0
                handles[key].write(flag + "\n")
                counts[key] += 1
    buckets = [
        {"key": key, "filename": f"bucket-{key}.txt", "row_count": counts[key]}
        for key in sorted(counts)
    ]
    marker = {
        "bucket_count": len(buckets),
        "input_rows": sum(counts.values()),
        "buckets": buckets,
    }
    atomic_json(building / "complete.json", marker)
    building.replace(output_dir)
    return marker


def reducer_macros(kind: str, colors_blind: bool) -> list[str]:
    macros = ["-DG_USE_LEXMIN_FOR_ISOMORPHISM"]
    if kind == "crossing":
        macros.extend(["-DG_CROSSINGS", "-DG_CROSSINGS_HUMAN_READABLE"])
    elif kind == "four":
        macros.append("-DG_4EDGES")
    else:
        raise ValueError(f"unknown flag kind: {kind}")
    macros.append("-DG_COLORED_VERTICES=2")
    if colors_blind:
        macros.append("-DG_COLORED_VERTICES_BLIND")
    else:
        macros.append("-DG_COLORED_VERTICES_SAMPLED_SEPARATELY_BY_COLORS")
    return macros


def compile_reducer(run_dir: Path, flag_cpp: Path, kind: str, colors_blind: bool) -> Path:
    convention = "blind" if colors_blind else "preserving"
    binary_dir = run_dir / "census" / "bin"
    log_dir = run_dir / "census" / "logs"
    binary_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    binary = binary_dir / f"flag-{kind}-{convention}"
    marker_path = binary_dir / f"flag-{kind}-{convention}.json"
    command = [
        "g++",
        "-O3",
        "-std=c++17",
        *reducer_macros(kind, colors_blind),
        str(flag_cpp),
        "-o",
        str(Path(f"{binary}.partial")),
    ]
    identity = {"flag_cpp_sha256": sha256(flag_cpp), "command": command[:-1] + [str(binary)]}
    if binary.is_file() and marker_path.is_file():
        marker = load_json(marker_path)
        if marker.get("identity") == identity:
            return binary
    Path(f"{binary}.partial").unlink(missing_ok=True)
    measurements = run_command(
        command,
        cwd=config.PROJECT_ROOT,
        log_path=log_dir / f"compile-{kind}-{convention}.log",
    )
    Path(f"{binary}.partial").replace(binary)
    atomic_json(marker_path, {"identity": identity, **measurements})
    return binary


def parse_time_report(path: Path) -> dict:
    values = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        key, separator, value = line.strip().partition(": ")
        if separator:
            values[key] = value
    return values


def gzip_copy(source_path: Path, output_path: Path) -> None:
    temporary = Path(f"{output_path}.partial")
    temporary.unlink(missing_ok=True)
    with source_path.open("rb") as source:
        with temporary.open("xb") as raw_output:
            with gzip.GzipFile(filename="", mode="wb", compresslevel=1, mtime=0, fileobj=raw_output) as output:
                shutil.copyfileobj(source, output)
    temporary.replace(output_path)


def reduce_bucket(task: tuple[int, str, str, str, str, str]) -> dict:
    index, input_name, output_name, binary_name, log_root_name, scratch_name = task
    input_path = Path(input_name)
    output_path = Path(output_name)
    binary = Path(binary_name)
    log_root = Path(log_root_name)
    scratch_root = Path(scratch_name)
    stats_path = output_path.with_suffix(".json")
    if output_path.is_file() and stats_path.is_file():
        return load_json(stats_path)
    log_root.mkdir(parents=True, exist_ok=True)
    scratch_root.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix=f"flag-reduce-{index:04d}-", dir=scratch_root))
    result_path = work_dir / "result.txt"
    stderr_path = log_root / f"bucket-{index:04d}.stderr.log"
    time_path = log_root / f"bucket-{index:04d}.time.log"
    started = time.perf_counter()
    try:
        command = [str(binary), "-ffrd", str(input_path)]
        timed_command = command
        if Path("/usr/bin/time").is_file():
            timed_command = ["/usr/bin/time", "-v", "-o", str(time_path), *command]
        with result_path.open("xb") as result:
            with stderr_path.open("w", encoding="utf-8") as errors:
                subprocess.run(
                    timed_command,
                    cwd=work_dir,
                    check=True,
                    stdout=result,
                    stderr=errors,
                )
        class_count = line_count(result_path)
        gzip_copy(result_path, output_path)
        report = parse_time_report(time_path)
        stats = {
            "bucket_index": index,
            "input_path": str(input_path),
            "input_rows": line_count(input_path),
            "class_count": class_count,
            "wall_seconds": time.perf_counter() - started,
            "user_seconds": report.get("User time (seconds)"),
            "system_seconds": report.get("System time (seconds)"),
            "max_rss_kib": report.get("Maximum resident set size (kbytes)"),
            "output_path": str(output_path),
            "output_bytes": output_path.stat().st_size,
            "sha256": sha256(output_path),
        }
        atomic_json(stats_path, stats)
        return stats
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def concatenate_reduced(outputs: list[dict], output_path: Path) -> dict:
    marker_path = output_path.with_suffix(".json")
    if output_path.is_file() and marker_path.is_file():
        return load_json(marker_path)
    temporary = Path(f"{output_path}.partial")
    temporary.unlink(missing_ok=True)
    with temporary.open("xb") as destination:
        for record in sorted(outputs, key=lambda value: value["bucket_index"]):
            with Path(record["output_path"]).open("rb") as source:
                shutil.copyfileobj(source, destination)
    temporary.replace(output_path)
    summary = {
        "class_count": sum(record["class_count"] for record in outputs),
        "bucket_count": len(outputs),
        "input_rows": sum(record["input_rows"] for record in outputs),
        "worker_wall_seconds_sum": sum(record["wall_seconds"] for record in outputs),
        "worker_user_seconds_sum": sum(float(record["user_seconds"] or 0) for record in outputs),
        "worker_system_seconds_sum": sum(float(record["system_seconds"] or 0) for record in outputs),
        "worker_max_rss_kib": max((int(record["max_rss_kib"] or 0) for record in outputs), default=0),
        "output_path": str(output_path),
        "output_bytes": output_path.stat().st_size,
        "sha256": sha256(output_path),
    }
    atomic_json(marker_path, summary)
    return summary


def reduce_variant(
    run_dir: Path,
    flag_cpp: Path,
    kind: str,
    colors_blind: bool,
    buckets: dict,
    bucket_dir: Path,
    workers: int,
    scratch_root: Path,
) -> dict:
    convention = "color-blind" if colors_blind else "color-preserving"
    binary = compile_reducer(run_dir, flag_cpp, kind, colors_blind)
    output_root = run_dir / "census" / "reduced-buckets" / f"{kind}-{convention}"
    log_root = run_dir / "census" / "logs" / f"reduce-{kind}-{convention}"
    output_root.mkdir(parents=True, exist_ok=True)
    tasks = []
    for index, bucket in enumerate(buckets["buckets"]):
        tasks.append(
            (
                index,
                str(bucket_dir / bucket["filename"]),
                str(output_root / f"bucket-{index:04d}.txt.gz"),
                str(binary),
                str(log_root),
                str(scratch_root),
            )
        )
    context = multiprocessing.get_context("fork")
    results = []
    with context.Pool(processes=min(workers, len(tasks))) as pool:
        for completed, result in enumerate(pool.imap_unordered(reduce_bucket, tasks), start=1):
            results.append(result)
            print(
                f"reduced {kind} {convention} bucket {completed}/{len(tasks)}: "
                f"{result['class_count']:,} classes",
                flush=True,
            )
    final_dir = run_dir / "census" / "flags"
    final_dir.mkdir(parents=True, exist_ok=True)
    return concatenate_reduced(
        results,
        final_dir / f"{kind}-{convention}.txt.gz",
    )


def reduce_flags(
    run_dir: Path,
    flag_cpp: Path,
    kind: str,
    workers: int,
    sort_memory: str,
    scratch_root: Path,
) -> dict:
    sorted_summary = sort_fixed_rows(run_dir, kind, workers, sort_memory, scratch_root)
    fixed_bucket_dir = run_dir / "census" / "buckets" / f"{kind}-color-preserving"
    fixed_buckets = split_sorted_buckets(Path(sorted_summary["output_path"]), fixed_bucket_dir)
    fixed = reduce_variant(
        run_dir,
        flag_cpp,
        kind,
        False,
        fixed_buckets,
        fixed_bucket_dir,
        workers,
        scratch_root,
    )
    blind_bucket_dir = run_dir / "census" / "buckets" / f"{kind}-color-blind"
    blind_buckets = split_blind_buckets(Path(fixed["output_path"]), blind_bucket_dir)
    blind = reduce_variant(
        run_dir,
        flag_cpp,
        kind,
        True,
        blind_buckets,
        blind_bucket_dir,
        workers,
        scratch_root,
    )
    return {
        "labeled": sorted_summary,
        "color_preserving_buckets": fixed_buckets,
        "color_preserving": fixed,
        "color_blind_buckets": blind_buckets,
        "color_blind": blind,
    }


def generation_diagnostics(run_dir: Path) -> dict:
    summary_path = run_dir / "outputs" / "final_run_summary.json"
    if not summary_path.is_file():
        raise RuntimeError(f"drawing generation did not write {summary_path}")
    return load_json(summary_path)


def write_report(
    run_dir: Path,
    m: int,
    n: int,
    export: dict,
    crossing: dict,
    four: dict,
    generation: dict,
    manifest: dict,
) -> dict:
    uncolored = int(export["drawing_count"])
    fixed = int(export["fixed_by_side_swap"])
    colored = uncolored if m != n else 2 * uncolored - fixed
    summary = {
        "schema_version": 1,
        "graph": f"K({m},{n})",
        "completed_at": utc_now(),
        "strong_drawing_classes": {
            "color_preserving": colored,
            "color_blind": uncolored,
            "side_swap_profile_candidates": export["side_swap_profile_candidates"],
            "fixed_by_side_swap": fixed if m == n else None,
        },
        "crossing_pair_flag_classes": {
            "color_preserving": crossing["color_preserving"]["class_count"],
            "color_blind": crossing["color_blind"]["class_count"],
        },
        "four_graph_flag_classes": {
            "color_preserving": four["color_preserving"]["class_count"],
            "color_blind": four["color_blind"]["class_count"],
        },
        "labeled_rows_before_flag_isomorphism": {
            "crossing_pair": crossing["labeled"]["unique_labeled_rows"],
            "four_graph": four["labeled"]["unique_labeled_rows"],
        },
        "generation": generation,
        "pipeline_stages": manifest.get("stages", {}),
        "artifacts": {
            "full_drawings": "final reduced compact shards listed in coordinator.sqlite",
            "crossing_pair_color_preserving": crossing["color_preserving"],
            "crossing_pair_color_blind": crossing["color_blind"],
            "four_graph_color_preserving": four["color_preserving"],
            "four_graph_color_blind": four["color_blind"],
        },
    }
    atomic_json(run_dir / "census" / "summary.json", summary)
    stage_rows = generation.get("shards_by_stage") or []
    worker_stage_rows = generation.get("worker_io_by_stage") or []
    generation_attempts = manifest.get("stages", {}).get("drawing_generation", [])
    generation_attempt = next(
        (attempt for attempt in reversed(generation_attempts) if attempt.get("status") == "complete"),
        None,
    )
    expand_totals = (generation.get("worker_io_summary") or {}).get("expand") or {}
    markdown = [
        f"# Complete-bipartite census: K({m},{n})",
        "",
        "## Mathematical object counts",
        "",
        "| Object | Color-preserving | Non-color-preserving |",
        "|---|---:|---:|",
        f"| Strong drawing classes | {colored:,} | {uncolored:,} |",
        f"| Crossing-pair flags | {crossing['color_preserving']['class_count']:,} | {crossing['color_blind']['class_count']:,} |",
        f"| 4-graph flags | {four['color_preserving']['class_count']:,} | {four['color_blind']['class_count']:,} |",
        "",
        "## Generation diagnostics",
        "",
        f"- Raw drawing candidates generated across all blocks: {int(expand_totals.get('candidates_seen', 0)):,}",
        f"- Side-swap profile candidates: {export['side_swap_profile_candidates']:,}",
        f"- Strong classes fixed by a side swap: {fixed:,}" if m == n else "- Side swapping is impossible because the part sizes differ.",
        f"- Unique labeled crossing rows before flag isomorphism: {crossing['labeled']['unique_labeled_rows']:,}",
        f"- Unique labeled 4-graph rows before flag isomorphism: {four['labeled']['unique_labeled_rows']:,}",
        "",
    ]
    if generation_attempt is not None:
        markdown.extend(
            [
                f"- End-to-end drawing-generation wall time: {generation_attempt['wall_seconds']:.3f} seconds",
                f"- End-to-end cumulative child CPU time: {generation_attempt['children_cpu_seconds']:.3f} seconds",
                f"- Maximum resident set size reported for a child process: {int(generation_attempt['children_max_rss_kib']):,} KiB",
                "",
            ]
        )
    markdown.extend(
        [
            "### Worker diagnostics by construction or reduction stage",
            "",
            "The wall span runs from the estimated start of the first completed task to the completion of the last. Cumulative worker CPU is the sum of task-boundary process CPU measurements.",
            "",
            "| Stage | Kind | Tasks | Candidates/raw records | Submitted/reduced | Wall span (s) | Cumulative worker CPU (s) | Max worker RSS (KiB) |",
            "|---:|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in worker_stage_rows:
        first_count = row.get("candidates_seen", row.get("raw_records", 0))
        second_count = row.get("candidates_submitted", row.get("reduced_records", 0))
        markdown.append(
            f"| {row['stage_index']} | {row['kind']} | {row['tasks']:,} | {int(first_count):,} | "
            f"{int(second_count):,} | {row['wall_span_seconds']:.3f} | "
            f"{row['worker_cpu_seconds_sum']:.3f} | {int(row['worker_max_rss_kib']):,} |"
        )
    edge_step_rows = [
        (row, step)
        for row in worker_stage_rows
        if row.get("kind") == "expand"
        for step in row.get("block_step_totals", [])
    ]
    if edge_step_rows:
        markdown.extend(
            [
                "",
                "### Intermediate drawing counts inside vertex blocks",
                "",
                "| Block stage | Vertex | Edge step | Frontier in | Raw drawings generated | Kept after local exact dedupe | Removed locally |",
                "|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row, step in edge_step_rows:
            markdown.append(
                f"| {row['stage_index']} | {row.get('block_vertex')} | {step['block_step_index']} | "
                f"{step['frontier_in']:,} | {step['raw_outputs']:,} | {step['frontier_out']:,} | "
                f"{step['exact_dedupe_removed']:,} |"
            )
    markdown.extend(
        [
            "",
        "### Stored shard counts by construction stage",
        "",
        "| Stage | Kind | Shards | Records | Compressed bytes |",
        "|---:|---|---:|---:|---:|",
        ]
    )
    for row in stage_rows:
        markdown.append(
            f"| {row['stage_index']} | {row['kind']} | {row['shard_count']:,} | "
            f"{row['record_count']:,} | {row['byte_count']:,} |"
        )
    markdown.extend(
        [
            "",
            "The full generation diagnostics, cumulative worker I/O totals, stage timings, resource measurements, commands, hashes, and artifact paths are retained in `summary.json`, `run.json`, and the per-stage JSON sidecars in this directory.",
            "",
        ]
    )
    (run_dir / "census" / "report.md").write_text("\n".join(markdown), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("m", type=int)
    parser.add_argument("n", type=int)
    parser.add_argument("--run-id")
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--flag-cpp", type=Path, required=True)
    parser.add_argument("--source-revision")
    parser.add_argument("--sort-memory", default="25%")
    args = parser.parse_args()
    if args.m < 2 or args.n < 2:
        parser.error("m and n must both be at least 2")
    if args.m + args.n > 9:
        parser.error("flag.cpp text export currently supports at most nine vertices")
    if args.m == args.n and args.m < 3:
        parser.error("colored square counts currently require part size at least three")
    if args.workers < 1:
        parser.error("--workers must be positive")
    args.flag_cpp = args.flag_cpp.resolve()
    if not args.flag_cpp.is_file():
        parser.error(f"flag.cpp not found: {args.flag_cpp}")
    return args


def main() -> None:
    args = parse_args()
    source_revision = git_revision(args.source_revision)
    run_id = args.run_id or make_run_id(f"K{args.m}_{args.n}-census")
    run_dir = initialize_run(run_id, args.m, args.n, args.workers, source_revision)
    census_dir = run_dir / "census"
    (census_dir / "logs").mkdir(parents=True, exist_ok=True)
    manifest_path = census_dir / "run.json"
    if not manifest_path.is_file():
        atomic_json(
            manifest_path,
            {
                "schema_version": 1,
                "status": "running",
                "run_id": run_id,
                "graph": {"m": args.m, "n": args.n},
                "started_at": utc_now(),
                "host": socket.gethostname(),
                "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
                "workers": args.workers,
                "sort_memory": args.sort_memory,
                "flag_cpp": str(args.flag_cpp),
                "flag_cpp_sha256": sha256(args.flag_cpp),
                "source_revision": source_revision,
                "command": [sys.executable, *sys.argv],
                "stages": {},
            },
        )
    manifest = load_json(manifest_path)
    expected_graph = {"m": args.m, "n": args.n}
    if manifest.get("graph") != expected_graph:
        raise ValueError(f"existing census manifest is for {manifest.get('graph')}, not {expected_graph}")
    if manifest.get("flag_cpp_sha256") != sha256(args.flag_cpp):
        raise ValueError("flag.cpp changed since this census run was initialized")

    run_config = load_json(run_dir / "run_config.json")
    final_summary = run_dir / "outputs" / "final_run_summary.json"
    if not final_summary.is_file():
        print(f"Generating full strong drawings for K({args.m},{args.n}) with {args.workers} workers", flush=True)
        record_stage(
            manifest_path,
            "drawing_generation",
            run_local,
            run_id,
            int(run_config["total_steps"]),
            process_count=args.workers,
        )
    else:
        print("Drawing generation is already complete; resuming census postprocessing", flush=True)

    scratch_root = Path(os.environ.get("TMPDIR", str(config.SCRATCH_ROOT))) / "census" / run_id
    export_summary_path = census_dir / "export-summary.json"
    if export_summary_path.is_file():
        export = load_json(export_summary_path)
    else:
        export = record_stage(
            manifest_path,
            "drawing_export_and_side_swap_scan",
            export_all_shards,
            run_dir,
            run_config,
            args.m,
            args.n,
            args.workers,
        )

    crossing_summary_path = census_dir / "crossing-summary.json"
    if crossing_summary_path.is_file():
        crossing = load_json(crossing_summary_path)
    else:
        crossing = record_stage(
            manifest_path,
            "crossing_pair_reduction",
            reduce_flags,
            run_dir,
            args.flag_cpp,
            "crossing",
            args.workers,
            args.sort_memory,
            scratch_root,
        )
        atomic_json(crossing_summary_path, crossing)

    four_summary_path = census_dir / "four-summary.json"
    if four_summary_path.is_file():
        four = load_json(four_summary_path)
    else:
        four = record_stage(
            manifest_path,
            "four_graph_reduction",
            reduce_flags,
            run_dir,
            args.flag_cpp,
            "four",
            args.workers,
            args.sort_memory,
            scratch_root,
        )
        atomic_json(four_summary_path, four)

    manifest = load_json(manifest_path)
    summary = write_report(
        run_dir,
        args.m,
        args.n,
        export,
        crossing,
        four,
        generation_diagnostics(run_dir),
        manifest,
    )
    manifest = load_json(manifest_path)
    manifest.update({"status": "complete", "completed_at": utc_now(), "summary": "summary.json"})
    atomic_json(manifest_path, manifest)
    print(json.dumps(summary["strong_drawing_classes"], sort_keys=True), flush=True)
    print(f"Census report: {census_dir / 'report.md'}", flush=True)


if __name__ == "__main__":
    main()
