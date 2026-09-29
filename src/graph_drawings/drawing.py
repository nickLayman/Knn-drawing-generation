"""Drawing data model and normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from typing import Iterable, TypeAlias


Edge: TypeAlias = tuple[int, int]
Crossing: TypeAlias = tuple[Edge, Edge]
Point: TypeAlias = int | Crossing
Path: TypeAlias = tuple[Point, ...]
Face: TypeAlias = tuple[Point, ...]
Arc: TypeAlias = tuple[Point, Point]


@dataclass(frozen=True, slots=True)
class Drawing:
    vertices: tuple[int, ...]
    crossings: tuple[Crossing, ...]
    paths: tuple[Path, ...]
    faces: tuple[Face, ...]

    @property
    def arcs(self) -> tuple[Arc, ...]:
        arcs: set[Arc] = set()
        for path in self.paths:
            arcs.update(path_arcs(path))
        return tuple(sorted(arcs, key=point_pair_key))


def edge_key(edge: Edge) -> tuple[int, int]:
    return edge


def normalize_edge(edge: Iterable[int]) -> Edge:
    u, v = tuple(edge)
    if u == v:
        raise ValueError(f"loop edge is not allowed: {edge!r}")
    return (u, v) if u > v else (v, u)


def normalize_crossing(crossing: Iterable[Iterable[int]]) -> Crossing:
    e1, e2 = tuple(normalize_edge(e) for e in crossing)
    if set(e1) & set(e2):
        raise ValueError(f"crossing edges must be disjoint: {crossing!r}")
    return (e1, e2) if e1 > e2 else (e2, e1)


def point_key(point: Point):
    if isinstance(point, int):
        return (0, point)
    return (1, point)


def point_pair_key(pair: tuple[Point, Point]):
    return (point_key(pair[0]), point_key(pair[1]))


def normalize_arc(u: Point, v: Point) -> Arc:
    return (u, v) if point_key(u) > point_key(v) else (v, u)


def path_arcs(path: Path) -> tuple[Arc, ...]:
    return tuple(normalize_arc(path[i], path[i + 1]) for i in range(len(path) - 1))


def face_arcs(face: Face) -> tuple[Arc, ...]:
    return tuple(normalize_arc(face[i], face[i + 1]) for i in range(len(face) - 1))


def face_canonical_cycle(
    face: Face,
    *,
    point_keys: Mapping[Point, tuple] | None = None,
) -> Face:
    cycle = tuple(face[:-1]) if len(face) > 1 and face[0] == face[-1] else tuple(face)
    if not cycle:
        return tuple()
    keys = (
        tuple(point_key(point) for point in cycle)
        if point_keys is None
        else tuple(point_keys[point] for point in cycle)
    )
    reverse_cycle = cycle[::-1]
    reverse_keys = keys[::-1]
    max_key = max(keys)
    best_sequence = None
    best_keys = None
    best_index = 0
    cycle_length = len(cycle)

    for sequence, sequence_keys in ((cycle, keys), (reverse_cycle, reverse_keys)):
        doubled_keys = sequence_keys + sequence_keys
        for index, key in enumerate(sequence_keys):
            if key != max_key:
                continue
            candidate_keys = doubled_keys[index : index + cycle_length]
            if best_keys is None or candidate_keys > best_keys:
                best_sequence = sequence
                best_keys = candidate_keys
                best_index = index
    assert best_sequence is not None
    chosen = best_sequence[best_index:] + best_sequence[:best_index]
    return chosen + (chosen[0],)


def unique_presentation(drawing: Drawing) -> Drawing:
    vertices = tuple(sorted(drawing.vertices))
    crossings = tuple(sorted((normalize_crossing(c) for c in drawing.crossings), key=edge_key))
    crossing_set = set(crossings)

    paths = []
    for path in drawing.paths:
        fixed_path = []
        for point in path:
            fixed_path.append(normalize_crossing(point) if not isinstance(point, int) else point)
        path_tuple = tuple(fixed_path)
        if point_key(path_tuple[0]) < point_key(path_tuple[-1]):
            path_tuple = path_tuple[::-1]
        paths.append(path_tuple)
    paths = sorted(set(paths), key=lambda p: tuple(point_key(x) for x in p))

    faces = []
    seen_faces = set()
    for face in drawing.faces:
        normalized_face = tuple(
            normalize_crossing(point) if not isinstance(point, int) else point
            for point in face
        )
        canonical_face = face_canonical_cycle(normalized_face)
        if canonical_face not in seen_faces:
            seen_faces.add(canonical_face)
            faces.append(canonical_face)
    faces.sort(key=lambda f: tuple(point_key(x) for x in f))

    path_crossings = {
        point
        for path in paths
        for point in path
        if not isinstance(point, int)
    }
    all_crossings = tuple(sorted(crossing_set | path_crossings, key=edge_key))
    return Drawing(vertices, all_crossings, tuple(paths), tuple(faces))


def relabel_drawing(drawing: Drawing, relabel: dict[int, int]) -> Drawing:
    point_map: dict[Point, Point] = {v: relabel[v] for v in drawing.vertices}
    for crossing in drawing.crossings:
        e1, e2 = crossing
        point_map[crossing] = normalize_crossing(
            ((relabel[e1[0]], relabel[e1[1]]), (relabel[e2[0]], relabel[e2[1]]))
        )

    return unique_presentation(
        Drawing(
            vertices=tuple(point_map[v] for v in drawing.vertices),
            crossings=tuple(point_map[c] for c in drawing.crossings),  # type: ignore[misc]
            paths=tuple(tuple(point_map[p] for p in path) for path in drawing.paths),
            faces=tuple(tuple(point_map[p] for p in face) for face in drawing.faces),
        )
    )


def validate_drawing(drawing: Drawing) -> None:
    vertex_set = set(drawing.vertices)
    if len(vertex_set) != len(drawing.vertices):
        raise ValueError("duplicate vertices")
    for path in drawing.paths:
        if len(path) < 2:
            raise ValueError(f"path too short: {path!r}")
        if not isinstance(path[0], int) or not isinstance(path[-1], int):
            raise ValueError(f"path endpoints must be original vertices: {path!r}")
    for face in drawing.faces:
        if len(face) < 2 or face[0] != face[-1]:
            raise ValueError(f"face must be closed: {face!r}")


def base_k22_drawings(vertices: tuple[int, int, int, int] = (0, 1, 2, 3)) -> tuple[Drawing, Drawing]:
    a, b, c, d = vertices
    crossing = normalize_crossing(((d, c), (b, a)))
    planar = Drawing(
        vertices=vertices,
        crossings=tuple(),
        paths=((b, a), (c, b), (d, c), (d, a)),
        faces=((a, b, c, d, a), (a, d, c, b, a)),
    )
    crossed = Drawing(
        vertices=vertices,
        crossings=(crossing,),
        paths=((b, crossing, a), (c, b), (d, crossing, c), (d, a)),
        faces=((a, crossing, d, a), (b, c, crossing, b), (a, d, crossing, b, c, crossing, a)),
    )
    return unique_presentation(planar), unique_presentation(crossed)
