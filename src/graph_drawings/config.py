"""Config for graph-drawings.

Edit the graph and worker settings below before launching a run. Runtime paths
come from the actual deployment source tree.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys


PROJECT = "graph-drawings"
RUN_ID = None  # Use None for GRAPH_NAME-YYYYMMDD-HHMMSS.

_SITE_ROOTS = {
    "office": Path("/home/nlayman/research"),
    "head": Path("/srv/cluster"),
    "nova": Path("/lustre/hdd/LAS/lidicky-lab/nlayman"),
}
_SITE_SCRATCH_ROOTS = {
    "office": Path("/scratch/nlayman/projects") / PROJECT,
    "head": Path("/scratch/nlayman/projects") / PROJECT,
}


def _project_root_for_site(site: str) -> Path:
    return _SITE_ROOTS[site] / "projects" / PROJECT


def detect_runtime_target(project_root: Path) -> str:
    """Return the deployment site represented by an actual source tree."""

    candidate = Path(project_root).resolve()
    for site in _SITE_ROOTS:
        if candidate == _project_root_for_site(site).resolve():
            return site
    expected = ", ".join(str(_project_root_for_site(site)) for site in _SITE_ROOTS)
    raise RuntimeError(
        f"unrecognized {PROJECT} source root {candidate}; expected one of: {expected}"
    )


def _nova_scratch_root(environment: dict[str, str] | None = None) -> Path:
    env = os.environ if environment is None else environment
    tmpdir = env.get("TMPDIR")
    if tmpdir:
        return Path(tmpdir) / PROJECT
    return Path("/ptmp/lidicky-lab/nlayman") / PROJECT


_SOURCE_PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_TARGET = detect_runtime_target(_SOURCE_PROJECT_ROOT)
PROJECT_ROOT = _SOURCE_PROJECT_ROOT
SHARED_ROOT = _SITE_ROOTS[RUNTIME_TARGET] / "shared" / PROJECT
RESULTS_ROOT = _SITE_ROOTS[RUNTIME_TARGET] / "data" / PROJECT
SCRATCH_ROOT = (
    _nova_scratch_root()
    if RUNTIME_TARGET == "nova"
    else _SITE_SCRATCH_ROOTS[RUNTIME_TARGET]
)


def _python_bin() -> Path:
    configured = os.environ.get("GD_PYTHON_BIN")
    if configured:
        path = Path(configured)
        if not path.is_absolute():
            raise RuntimeError("GD_PYTHON_BIN must be an absolute interpreter path")
        return path
    if RUNTIME_TARGET == "office":
        return Path("/home/nlayman/miniconda3/envs/research/bin/python")
    # Deployment environments are site-specific; use the interpreter that
    # loaded this source tree instead of inventing a machine-local path.
    return Path(sys.executable)


PYTHON_BIN = _python_bin()

SRC_ROOT = PROJECT_ROOT / "src"
RUNS_ROOT = RESULTS_ROOT / "runs"
CURRENT_RUN_FILE = SHARED_ROOT / "manifests" / "current_run_id.txt"

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

# K4,3 has seven vertices.  Larger production enumerations are Nova-compute
# work; the existing distributed Nova dispatch is intentionally not assumed to
# be configured by this source tree.
K43_VERTEX_COUNT = 7


def graph_vertex_count(edges=None) -> int:
    configured_edges = GRAPH_EDGES if edges is None else edges
    return len({int(vertex) for edge in configured_edges for vertex in edge})


def graph_requires_nova(edges=None) -> bool:
    configured_edges = GRAPH_EDGES if edges is None else edges
    return graph_vertex_count(configured_edges) > K43_VERTEX_COUNT or len(configured_edges) > 12


def validate_entrypoint_runtime(
    *,
    operation: str,
    require_compute_node: bool = False,
    worker_count: int | None = None,
    runtime_target: str | None = None,
    environment: dict[str, str] | None = None,
    edges=None,
) -> None:
    """Reject production launches on a site or node that cannot run them."""

    target = RUNTIME_TARGET if runtime_target is None else runtime_target
    if target not in _SITE_ROOTS:
        raise RuntimeError(f"{operation}: unsupported runtime target {target!r}")

    configured_edges = GRAPH_EDGES if edges is None else edges
    vertex_count = graph_vertex_count(configured_edges)
    edge_count = len(configured_edges)
    if target == "nova" and worker_count:
        raise RuntimeError(
            f"{operation}: Nova distributed Slurm dispatch is not configured for "
            "this project; use the allocation-local runner on a Nova compute node"
        )
    if not graph_requires_nova(configured_edges):
        return

    if target != "nova":
        raise RuntimeError(
            f"{operation}: graph has {vertex_count} vertices and {edge_count} edges "
            "(larger than the K4,3 boundary); large enumerations must run on Nova compute, "
            f"not {target}"
        )

    env = os.environ if environment is None else environment
    if require_compute_node and not env.get("SLURM_JOB_ID"):
        raise RuntimeError(
            f"{operation}: graph has {vertex_count} vertices and must run on a "
            "Nova compute allocation (SLURM_JOB_ID is unset)"
        )

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
