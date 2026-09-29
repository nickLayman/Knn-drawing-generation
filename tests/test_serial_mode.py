from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from graph_drawings import resource_usage


def test_serial_k23_census(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "K2_3"
    environment = dict(os.environ)
    python_path = str(project_root / "src")
    if environment.get("PYTHONPATH"):
        python_path += os.pathsep + environment["PYTHONPATH"]
    environment["PYTHONPATH"] = python_path
    subprocess.run(
        [
            sys.executable,
            "-m",
            "graph_drawings",
            "2",
            "3",
            "--serial",
            "--output-dir",
            str(output_dir),
            "--scratch-dir",
            str(tmp_path / "scratch"),
        ],
        cwd=project_root,
        env=environment,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    summary = json.loads((output_dir / "census" / "summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((output_dir / "census" / "run.json").read_text(encoding="utf-8"))
    assert summary["strong_drawing_classes"] == {
        "color_blind": 6,
        "color_preserving": 6,
        "fixed_by_side_swap": None,
        "side_swap_profile_candidates": 0,
    }
    assert summary["exported_labeled_flag_rows"] == {
        "crossing_pair": 6,
        "four_graph": 6,
    }
    assert manifest["execution_mode"] == "serial"


def test_resource_measurements_have_a_windows_fallback(monkeypatch) -> None:
    monkeypatch.setattr(resource_usage, "resource", None)
    snapshot = resource_usage.usage_snapshot()
    assert snapshot["self_user_seconds"] >= 0
    assert snapshot["self_max_rss_kib"] == 0
    assert resource_usage.process_max_rss_kib() == 0


def test_macos_rss_bytes_are_normalized_to_kib(monkeypatch) -> None:
    monkeypatch.setattr(resource_usage.sys, "platform", "darwin")
    assert resource_usage._rss_kib(4096) == 4
