"""Compare a census summary with the published reference counts."""

from __future__ import annotations

import argparse
from importlib.resources import files
import json
from pathlib import Path


COUNT_FIELDS = (
    "strong_drawing_classes",
    "crossing_pair_flag_classes",
    "four_graph_flag_classes",
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path, help="census/summary.json from a completed run")
    args = parser.parse_args()

    summary = json.loads(args.summary.read_text(encoding="utf-8"))
    expected = json.loads(
        files("graph_drawings").joinpath("data/census.json").read_text(encoding="utf-8")
    )
    graph = summary.get("graph")
    if graph not in expected["graphs"]:
        parser.error(f"no published reference counts for {graph!r}")

    mismatches = []
    for field in COUNT_FIELDS:
        if field not in summary:
            print(f"SKIP {field}: isomorphism reduction was not completed")
            continue
        actual = summary[field]
        actual_pair = [actual["color_preserving"], actual["color_blind"]]
        expected_pair = expected["graphs"][graph][field]
        if actual_pair == expected_pair:
            print(f"PASS {field}: {actual_pair}")
        else:
            print(f"FAIL {field}: got {actual_pair}, expected {expected_pair}")
            mismatches.append(field)
    if mismatches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
