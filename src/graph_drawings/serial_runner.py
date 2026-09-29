"""Single-process drawing generation with no coordinator server or sockets."""

from __future__ import annotations

from . import config
from .coordinator import CoordinatorState
from .worker import load_run_context, process_job, process_reduce_job


def run_serial(run_id: str, total_steps: int) -> None:
    """Run every expansion and reduction task sequentially in this process."""

    run_dir = config.RUNS_ROOT / run_id
    state = CoordinatorState(run_id, total_steps, run_dir / "coordinator.sqlite")
    worker_id = "serial"
    try:
        if total_steps == 0:
            state.mark_run_complete()
            return
        run_context = load_run_context(run_id)
        while True:
            response = state.get_job(worker_id)
            job = response.get("job")
            if job is None:
                if response.get("global_done") or state.global_done():
                    return
                raise RuntimeError("serial scheduler has unfinished work but no available task")
            job["worker_id"] = worker_id
            if job.get("job_type") == "reduce":
                result = process_reduce_job(job, run_context)
                state.submit_reduction(
                    {
                        "worker_id": worker_id,
                        "job_key": result.job_key,
                        "stage_index": result.stage_index,
                        "elapsed_seconds": result.elapsed_seconds,
                        "summary": result.reduction_summary,
                        "profile": result.profile,
                    }
                )
            else:
                result = process_job(job, run_context)
                state.submit_shards(
                    {
                        "worker_id": worker_id,
                        "job_key": result.job_key,
                        "stage_index": result.stage_index,
                        "elapsed_seconds": result.elapsed_seconds,
                        "shards": result.shards,
                        "profile": result.profile,
                    }
                )
            state.close_job(
                {
                    "worker_id": worker_id,
                    "job_key": result.job_key,
                    "job_type": result.job_type,
                    "stage_index": result.stage_index,
                }
            )
    finally:
        state.shutdown()
