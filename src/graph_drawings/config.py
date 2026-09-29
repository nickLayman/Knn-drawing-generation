"""Runtime and algorithm settings for the portable census program."""

from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile


PROJECT = "complete-bipartite-drawing-census"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_TARGET = "portable"
PYTHON_BIN = Path(sys.executable).resolve()
SRC_ROOT = PROJECT_ROOT / "src"

# The command line configures these paths before starting or resuming a run.
# Defaults keep direct module use confined to the current working directory.
RESULTS_ROOT = Path.cwd() / "results"
RUNS_ROOT = RESULTS_ROOT
SCRATCH_ROOT = Path(os.environ.get("TMPDIR", tempfile.gettempdir())) / PROJECT
SHARED_ROOT = RESULTS_ROOT
CURRENT_RUN_FILE = RESULTS_ROOT / ".current_run_id"


def configure_runtime(*, runs_root: Path, scratch_root: Path) -> None:
    """Set the explicit output and scratch roots used by this process."""

    global RESULTS_ROOT, RUNS_ROOT, SCRATCH_ROOT, SHARED_ROOT, CURRENT_RUN_FILE
    RUNS_ROOT = Path(runs_root).resolve()
    RESULTS_ROOT = RUNS_ROOT
    SCRATCH_ROOT = Path(scratch_root).resolve()
    SHARED_ROOT = RUNS_ROOT
    CURRENT_RUN_FILE = RUNS_ROOT / ".current_run_id"

# Default target. Vertices are inferred from GRAPH_EDGES; disconnected graphs
# are intentionally unsupported.
GRAPH_NAME = "K3_5"
GRAPH_EDGES = [
    (u, v)
    for u in [0, 1, 2]
    for v in [3, 4, 5, 6, 7]
]
EXPORT_VERTEX_COLORS = [1, 1, 1, 2, 2, 2, 2, 2]  # Optional list, e.g. [1, 2, 2, 1].
FINAL_OUTPUT_MODE = "full_drawings"  # "full_drawings", "crossing_pair_flags", or "four_graph_flags".
FINAL_FLAG_CANONICALIZATION = "none"  # Reserved for future use; current final flag dedupe is exact-string only.

def graph_vertex_count(edges=None) -> int:
    configured_edges = GRAPH_EDGES if edges is None else edges
    return len({int(vertex) for edge in configured_edges for vertex in edge})


def validate_entrypoint_runtime(
    *,
    operation: str,
    require_compute_node: bool = False,
    worker_count: int | None = None,
    runtime_target: str | None = None,
    environment: dict[str, str] | None = None,
    edges=None,
) -> None:
    """Validate portable entry-point arguments at the external boundary."""

    del operation, require_compute_node, runtime_target, environment
    configured_edges = GRAPH_EDGES if edges is None else edges
    if not configured_edges:
        raise ValueError("the graph must contain at least one edge")
    if worker_count is not None and worker_count < 0:
        raise ValueError("worker_count cannot be negative")

# Cluster/local mode:
#   WORKER_COUNT > 0: submit Slurm coordinator + worker array.
#   WORKER_COUNT == 0: run locally without Slurm.
WORKER_COUNT = 30
LOCAL_PROCESS_COUNT = None  # None means max(1, os.cpu_count() - 1).

CANONICAL_BACKEND = "graph_automorphism_partition"
CANONICALIZATION_POLICY = "graph_automorphism_vertex_block_checkpoints"
CANONICAL_KEY_STRATEGY = "hierarchical_signature_prefilter_full_presentation"
STORAGE_MODE = "compact_sharded"
COMPACT_STORAGE_VERSION = 1
COMPACT_PRIMARY = "faces"
SHARD_COMPRESSION = "gzip"
DB_BLOB_COMPRESSION = "zlib"
DELETE_RAW_SHARDS_AFTER_REDUCE = True
COMPOUND_SHARDS = True
SHARD_PARTITION_PREFIX_HEX = 2
WORKER_SHARD_FLUSH_BYTES = 64 * 1024 * 1024
STAGE_BARRIERS = True
DEBUG_STORE_FULL_CANONICAL_KEYS = False
DEBUG_KEEP_REJECTED_COUNTS = False
PROFILE_OPERATIONS = True
COMPACT_RUN_DB_AFTER_COMPLETION = True
DELETE_TRANSIENT_ROWS_AFTER_COMPLETION = True
CHECKPOINT_SHARD_BATCH_RECORDS = 5000
CHECKPOINT_SHARD_BATCH_BYTES = 64 * 1024 * 1024
FINAL_STEP_LOCAL_DEDUPE = True
BLOCK_SEARCH_ORDER = "dfs"  # "dfs" or "bfs".
VERIFY_INTERMEDIATE_HASH_COLLISIONS = True

# Cheap bucket partitioning is only a reducer prefilter.  Exact report/work
# hashes still decide deduplication within each bucket.
SHARD_BY_CROSSING_COUNT = True
BUCKET_SIGNATURE_PROPERTIES = [
    # "vertex-count",
    # "path-count",
    # "crossing-count",
    # "face-count",
    "path-crossing-counts",
    "face-lengths",
    # "vertex-incident-path-crossing-counts",
    # "crossing-endpoint-degrees",
    # "min-path-crossing-count",
    # "max-path-crossing-count",
    # "min-face-length",
    # "max-face-length",
    # "min-path-crossing-count-multiplicity",
    # "max-path-crossing-count-multiplicity",
    # "min-face-length-multiplicity",
    # "max-face-length-multiplicity",
    # "distinct-path-crossing-count",
    # "distinct-face-length-count",
    # Redundant equivalents of path-crossing-count options:
    # "path-lengths",
    # "min-path-length",
    # "max-path-length",
    # "min-path-length-multiplicity",
    # "max-path-length-multiplicity",
    # "distinct-path-length-count",
]

COORDINATOR_HOST = "head"
COORDINATOR_PORT = 8776
COORDINATOR_CONNECT_TIMEOUT_SECONDS = 10
COORDINATOR_CONNECT_RETRY_SECONDS = 300
LOCAL_COORDINATOR_HOST = "127.0.0.1"
LOCAL_COORDINATOR_PORT = 0  # 0 means choose a free localhost port.
LEASE_SECONDS = 300
PROGRESS_LOG_EVERY_SECONDS = 60
WORKER_HEARTBEAT_SECONDS = 60
WORKER_IDLE_EXIT_SECONDS = 30

# inspect_db.py output controls.
INSPECT_DB_INCLUDE_JOB_COUNTS = True
INSPECT_DB_INCLUDE_STAGE_COUNTS = True
INSPECT_DB_INCLUDE_WORKER_HEARTBEATS = True
INSPECT_DB_INCLUDE_SHARDS = True
INSPECT_DB_INCLUDE_OPERATION_TIMINGS = True
INSPECT_DB_OPERATION_TIMING_LIMIT = 10
INSPECT_DB_WORKER_HEARTBEAT_LIMIT = 50
INSPECT_DB_WORKER_HEARTBEAT_FIELDS = [
    "worker_id",
    "hostname",
    # "current_job_key",
    "seconds_since_last_seen",
    "extension_jobs_done",
    "reduce_jobs_done",
    "jobs_done",
    "candidates_seen",
    "candidates_accepted",
    "control_action",
]
INSPECT_DB_OPERATION_TIMING_FIELDS = [
    "operation",
    "request_count",
    "total_seconds",
    "max_seconds",
]

# timing_diagnostics.py output controls.
TIMING_DIAGNOSTICS_INCLUDE_EVENTS = True
TIMING_DIAGNOSTICS_INCLUDE_STARTING_CONDITIONS = True
TIMING_DIAGNOSTICS_INCLUDE_CANONICALIZATION = True
TIMING_DIAGNOSTICS_INCLUDE_STORAGE = True
TIMING_DIAGNOSTICS_INCLUDE_PROGRESS = True
TIMING_DIAGNOSTICS_INCLUDE_WORKERS = True
TIMING_DIAGNOSTICS_INCLUDE_WORKER_HOSTS = True
TIMING_DIAGNOSTICS_INCLUDE_HEARTBEAT_RATES = True
TIMING_DIAGNOSTICS_INCLUDE_EXTENSION_RESULTS = True
TIMING_DIAGNOSTICS_INCLUDE_REDUCE_RESULTS = True
TIMING_DIAGNOSTICS_INCLUDE_WORKER_PROFILE = True
TIMING_DIAGNOSTICS_INCLUDE_OPERATION_TIMINGS = True
TIMING_DIAGNOSTICS_OPERATION_TIMING_LIMIT = 50
TIMING_DIAGNOSTICS_WORKER_LIMIT = 50
TIMING_DIAGNOSTICS_HIDE_ZERO_OPERATION_TIMINGS = True
TIMING_DIAGNOSTICS_STAGE_COUNT_COLUMNS = [
    "stage",
    "reported",
    "work",
    "work/report",
]
TIMING_DIAGNOSTICS_SHARD_COLUMNS = [
    "stage",
    "kind",
    "shards",
    "records",
    "bytes",
    "rec/shard",
    "bytes/shard",
    "bytes/rec",
    "max_rec",
]
TIMING_DIAGNOSTICS_WORKER_COLUMNS = [
    "worker",
    "host",
    "age_sec",
    # "current_job",
    "jobs_done",
    "seen",
    "accepted",
    "control",
]
TIMING_DIAGNOSTICS_WORKER_HOST_COLUMNS = [
    "host",
    "workers",
    "median_age",
    "jobs_done",
    "seen",
    "accepted",
    "median/sec",
]
TIMING_DIAGNOSTICS_HEARTBEAT_RATE_COLUMNS = [
    "worker",
    "samples",
    "median/sec",
    "latest_done",
    "latest_seen",
    "latest_accepted",
    # "latest_job",
]
TIMING_DIAGNOSTICS_EXTENSION_RESULT_COLUMNS = [
    "stage",
    "extension_jobs",
    "seen",
    "submitted",
    "reported",
    "work",
    "worker_sec",
    "seen/sec",
]
TIMING_DIAGNOSTICS_REDUCE_RESULT_COLUMNS = [
    "stage",
    "reduce_jobs",
    "raw",
    "reduced",
    "inserted_reps",
    "inserted_ext_jobs",
    "worker_sec",
    "raw/sec",
]
TIMING_DIAGNOSTICS_WORKER_TASK_TIMING_COLUMNS = [
    "operation",
    "total",
    "avg/ext_job",
]
TIMING_DIAGNOSTICS_CANONICAL_PREFILTER_COLUMNS = [
    "key",
    "perms",
    "full",
    "full/perms",
    "vertex",
    "cross",
    "path",
    "cheap_sec",
    "full_sec",
    "json_sec",
    "avg_full/key",
    "max_full",
]
TIMING_DIAGNOSTICS_BLOCK_STEP_COLUMNS = [
    "step",
    "extension_jobs",
    "seconds",
    "inputs",
    "raw",
    "kept",
    "seq",
    "cart",
    "unique",
    "json",
    "routes",
    "feasible",
    "infeasible",
]
TIMING_DIAGNOSTICS_OPERATION_TIMING_COLUMNS = [
    "operation",
    "count",
    "total",
    "wall_est",
    "per_worker",
    "avg",
    "max",
]

# Slurm settings consumed by scripts/launch_run.py. The distributed launcher
# is currently a Head-only path; Nova uses the allocation-local runner.
SLURM_COORDINATOR_PARTITION = "head"
SLURM_WORKER_PARTITION = "main,home,office"
SLURM_COORDINATOR_CPUS = 1
SLURM_COORDINATOR_MEM = "3G"
SLURM_WORKER_CPUS = 1
SLURM_WORKER_MEM = "3G"
SLURM_TIME = "0"
SLURM_EXCLUDED_NODES = []
SLURM_STARTUP_SLEEP_SECONDS = 5
COORDINATOR_JOB_NAME = "gd-coord"
WORKER_JOB_NAME = "gd-worker"
