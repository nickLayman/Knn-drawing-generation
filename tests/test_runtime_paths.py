from __future__ import annotations

import itertools
import json
from pathlib import Path

import pytest

from graph_drawings import config
from graph_drawings.local_runner import local_process_count
from graph_drawings.worker import load_run_context


def test_runtime_target_comes_from_source_location() -> None:
    assert config.detect_runtime_target(Path("/home/nlayman/research/projects/graph-drawings")) == "office"
    assert config.detect_runtime_target(Path("/srv/cluster/projects/graph-drawings")) == "head"
    assert config.detect_runtime_target(
        Path("/lustre/hdd/LAS/lidicky-lab/nlayman/projects/graph-drawings")
    ) == "nova"


def test_unknown_source_location_fails_clearly() -> None:
    with pytest.raises(RuntimeError, match="unrecognized graph-drawings source root"):
        config.detect_runtime_target(Path("/tmp/graph-drawings"))


def test_nova_scratch_prefers_tmpdir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TMPDIR", "/local/slurm-job")
    assert config._nova_scratch_root() == Path("/local/slurm-job/graph-drawings")


def test_nova_scratch_falls_back_to_ptmp(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TMPDIR", raising=False)
    assert config._nova_scratch_root() == Path("/ptmp/lidicky-lab/nlayman/graph-drawings")


def test_large_graph_is_rejected_office_and_head() -> None:
    with pytest.raises(RuntimeError, match="must run on Nova compute"):
        config.validate_entrypoint_runtime(
            operation="test",
            runtime_target="office",
            environment={},
        )
    with pytest.raises(RuntimeError, match="must run on Nova compute"):
        config.validate_entrypoint_runtime(
            operation="test",
            runtime_target="head",
            environment={},
        )


def test_large_graph_requires_a_nova_compute_allocation() -> None:
    with pytest.raises(RuntimeError, match="SLURM_JOB_ID is unset"):
        config.validate_entrypoint_runtime(
            operation="test",
            runtime_target="nova",
            require_compute_node=True,
            environment={},
        )

    config.validate_entrypoint_runtime(
        operation="test",
        runtime_target="nova",
        require_compute_node=True,
        environment={"SLURM_JOB_ID": "123"},
    )


def test_edge_count_also_marks_graph_as_large() -> None:
    edges = list(itertools.combinations(range(7), 2))[:13]
    assert config.graph_vertex_count(edges) == 7
    assert config.graph_requires_nova(edges)


def test_allocation_local_runner_reserves_coordinator_cpu(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLURM_CPUS_PER_TASK", "8")
    monkeypatch.setattr(config, "LOCAL_PROCESS_COUNT", 2)
    assert local_process_count() == 2
    monkeypatch.setattr(config, "LOCAL_PROCESS_COUNT", 30)
    assert local_process_count() == 7


def test_worker_uses_persisted_edges_for_runtime_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "saved-large"
    run_dir.mkdir()
    (run_dir / "run_config.json").write_text(
        json.dumps({"graph_edges": [list(edge) for edge in config.GRAPH_EDGES]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "RUNS_ROOT", tmp_path)
    monkeypatch.setattr(config, "GRAPH_EDGES", [(0, 1), (1, 2), (2, 3), (3, 0)])
    monkeypatch.setattr(config, "RUNTIME_TARGET", "office")
    with pytest.raises(RuntimeError, match="must run on Nova compute"):
        load_run_context("saved-large")


def test_small_graph_can_run_without_nova_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "GRAPH_EDGES", [(0, 1), (1, 2), (2, 3), (3, 0)])
    config.validate_entrypoint_runtime(
        operation="test",
        runtime_target="office",
        environment={},
    )
    config.validate_entrypoint_runtime(
        operation="test",
        runtime_target="nova",
        worker_count=0,
        environment={},
    )
    with pytest.raises(RuntimeError, match="distributed Slurm dispatch is not configured"):
        config.validate_entrypoint_runtime(
            operation="test",
            runtime_target="nova",
            worker_count=1,
            environment={},
        )
