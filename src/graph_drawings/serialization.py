"""JSON serialization for drawings."""

from __future__ import annotations

import json

from .drawing import Crossing, Drawing, Face, Path, Point, normalize_crossing, unique_presentation


def point_to_data(point: Point):
    if isinstance(point, int):
        return point
    return [[point[0][0], point[0][1]], [point[1][0], point[1][1]]]


def point_from_data(data) -> Point:
    if isinstance(data, int):
        return data
    return normalize_crossing(((int(data[0][0]), int(data[0][1])), (int(data[1][0]), int(data[1][1]))))


def drawing_to_record(drawing: Drawing) -> dict:
    drawing = unique_presentation(drawing)
    return {
        "vertices": list(drawing.vertices),
        "crossings": [point_to_data(c) for c in drawing.crossings],
        "paths": [[point_to_data(p) for p in path] for path in drawing.paths],
        "faces": [[point_to_data(p) for p in face] for face in drawing.faces],
    }


def drawing_from_record(record: dict) -> Drawing:
    return unique_presentation(
        Drawing(
            vertices=tuple(int(v) for v in record["vertices"]),
            crossings=tuple(point_from_data(c) for c in record["crossings"]),  # type: ignore[arg-type]
            paths=tuple(tuple(point_from_data(p) for p in path) for path in record["paths"]),
            faces=tuple(tuple(point_from_data(p) for p in face) for face in record["faces"]),
        )
    )


def drawing_to_json(drawing: Drawing) -> str:
    return json.dumps(drawing_to_record(drawing), sort_keys=True, separators=(",", ":"))


def drawing_from_json(payload: str) -> Drawing:
    return drawing_from_record(json.loads(payload))


def drawing_summary(drawing: Drawing) -> dict:
    drawing = unique_presentation(drawing)
    return {
        "vertices": len(drawing.vertices),
        "edges": len(drawing.paths),
        "crossings": len(drawing.crossings),
        "faces": len(drawing.faces),
    }

