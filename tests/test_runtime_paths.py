from __future__ import annotations

from pathlib import Path

from graph_drawings import config
from graph_drawings.local_runner import local_process_count


def test_runtime_paths_are_explicit(tmp_path: Path) -> None:
    runs = tmp_path / "published-results"
    scratch = tmp_path / "temporary-work"
    config.configure_runtime(runs_root=runs, scratch_root=scratch)
    assert config.RUNS_ROOT == runs.resolve()
    assert config.SCRATCH_ROOT == scratch.resolve()


def test_allocation_local_runner_reserves_coordinator_cpu(monkeypatch) -> None:
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")
    monkeypatch.setattr(config, "LOCAL_PROCESS_COUNT", 2)
    assert local_process_count() == 2
    monkeypatch.setattr(config, "LOCAL_PROCESS_COUNT", 30)
    assert local_process_count() == 7


def test_portable_runtime_accepts_supported_graph() -> None:
    config.validate_entrypoint_runtime(
        operation="test",
        worker_count=4,
        edges=[(0, 2), (0, 3), (1, 2), (1, 3)],
    )
