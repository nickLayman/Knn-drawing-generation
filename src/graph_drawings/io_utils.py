"""Run-directory and JSON helpers."""

from __future__ import annotations

import json
import time
from pathlib import Path

from . import config


def ensure_project_dirs() -> None:
    for path in [config.RUNS_ROOT, config.SCRATCH_ROOT]:
        path.mkdir(parents=True, exist_ok=True)


def make_run_id(prefix: str) -> str:
    return time.strftime(f"{prefix}-%Y%m%d-%H%M%S")


def create_run_dir(run_id: str) -> Path:
    ensure_project_dirs()
    run_dir = config.RUNS_ROOT / run_id
    for subdir in ["logs", "outputs", "debug"]:
        (run_dir / subdir).mkdir(parents=True, exist_ok=True)
    return run_dir


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")


def write_json(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, sort_keys=True)
        f.write("\n")


def live_db_path_file(run_id: str) -> Path:
    return config.RUNS_ROOT / run_id / "coordinator_live_db_path.txt"


def write_live_db_path(run_id: str, db_path: Path) -> None:
    live_db_path_file(run_id).write_text(str(db_path.resolve()) + "\n", encoding="utf-8")


def clear_live_db_path(run_id: str) -> None:
    marker = live_db_path_file(run_id)
    if marker.exists():
        marker.unlink()
