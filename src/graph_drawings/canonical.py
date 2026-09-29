"""Canonical keys for drawings."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import time

from . import config
from .automorphism import CanonicalContext
from .drawing import (
    Crossing,
    Drawing,
    Point,
    face_canonical_cycle,
    point_key,
)


HASH_ALGORITHM = "sha256"


@dataclass(frozen=True, slots=True)
class DrawingKeys:
    report_hash: str
    work_hash: str
    report_key_json: str | None = None
    work_key_json: str | None = None


def hash_key(key_json: str) -> str:
    return hashlib.sha256(key_json.encode("utf-8")).hexdigest()


BUCKET_PROPERTY_ALIASES = {
    "path-crossing-count": "path-crossing-counts",
    "vertex-incident-path-lengths": "vertex-incident-path-crossing-counts",
}


def normalize_bucket_properties(properties: list[str] | tuple[str, ...] | None = None) -> tuple[str, ...]:
    raw = properties if properties is not None else config.BUCKET_SIGNATURE_PROPERTIES
    return tuple(BUCKET_PROPERTY_ALIASES.get(str(name), str(name)) for name in raw)


AVAILABLE_BUCKET_SIGNATURE_PROPERTIES = (
    "vertex-count",
    "path-count",
    "crossing-count",
    "face-count",
    "vertex-incident-path-crossing-counts",
    "path-crossing-counts",
    "face-lengths",
    "crossing-endpoint-degrees",
    "min-path-crossing-count",
    "max-path-crossing-count",
    "min-face-length",
    "max-face-length",
    "min-path-crossing-count-multiplicity",
    "max-path-crossing-count-multiplicity",
    "min-face-length-multiplicity",
    "max-face-length-multiplicity",
    "distinct-path-crossing-count",
    "distinct-face-length-count",
    "path-lengths",
    "min-path-length",
    "max-path-length",
    "min-path-length-multiplicity",
    "max-path-length-multiplicity",
    "distinct-path-length-count",
)


def path_crossing_counts(drawing: Drawing) -> list[int]:
    return sorted(sum(1 for point in path if not isinstance(point, int)) for path in drawing.paths)


def path_lengths(drawing: Drawing) -> list[int]:
    return sorted(len(path) for path in drawing.paths)


def face_lengths(drawing: Drawing) -> list[int]:
    return sorted(max(0, len(face) - 1) for face in drawing.faces)


def vertex_path_crossing_signatures(drawing: Drawing) -> list[tuple[int, tuple[int, ...]]]:
    signatures = []
    for vertex in drawing.vertices:
        counts = [
            sum(1 for point in path if not isinstance(point, int))
            for path in drawing.paths
            if vertex in path
        ]
        signatures.append((len(counts), tuple(sorted(counts))))
    return sorted(signatures)


def drawing_bucket_component(drawing: Drawing, name: str):
    if name == "vertex-count":
        return len(drawing.vertices)
    if name == "path-count":
        return len(drawing.paths)
    if name == "crossing-count":
        return len(drawing.crossings)
    if name == "face-count":
        return len(drawing.faces)
    if name == "vertex-incident-path-crossing-counts":
        return tuple(vertex_path_crossing_signatures(drawing))
    if name == "path-crossing-counts":
        return tuple(path_crossing_counts(drawing))
    if name == "path-lengths":
        return tuple(path_lengths(drawing))
    if name == "face-lengths":
        return tuple(face_lengths(drawing))
    if name == "crossing-endpoint-degrees":
        crossing_endpoint_degrees = []
        vertex_degree = {vertex: 0 for vertex in drawing.vertices}
        for path in drawing.paths:
            vertex_degree[path[0]] += 1  # type: ignore[index]
            vertex_degree[path[-1]] += 1  # type: ignore[index]
        for e1, e2 in drawing.crossings:
            crossing_endpoint_degrees.append(
                tuple(sorted((vertex_degree[e1[0]], vertex_degree[e1[1]], vertex_degree[e2[0]], vertex_degree[e2[1]])))
            )
        return tuple(sorted(crossing_endpoint_degrees))
    if name in {
        "min-path-crossing-count",
        "max-path-crossing-count",
        "min-path-crossing-count-multiplicity",
        "max-path-crossing-count-multiplicity",
        "distinct-path-crossing-count",
    }:
        counts = path_crossing_counts(drawing)
        if name == "min-path-crossing-count":
            return min(counts, default=0)
        if name == "max-path-crossing-count":
            return max(counts, default=0)
        if name == "min-path-crossing-count-multiplicity":
            return counts.count(min(counts)) if counts else 0
        if name == "max-path-crossing-count-multiplicity":
            return counts.count(max(counts)) if counts else 0
        return len(set(counts))
    if name in {
        "min-path-length",
        "max-path-length",
        "min-path-length-multiplicity",
        "max-path-length-multiplicity",
        "distinct-path-length-count",
    }:
        lengths = path_lengths(drawing)
        if name == "min-path-length":
            return min(lengths, default=0)
        if name == "max-path-length":
            return max(lengths, default=0)
        if name == "min-path-length-multiplicity":
            return lengths.count(min(lengths)) if lengths else 0
        if name == "max-path-length-multiplicity":
            return lengths.count(max(lengths)) if lengths else 0
        return len(set(lengths))
    if name in {
        "min-face-length",
        "max-face-length",
        "min-face-length-multiplicity",
        "max-face-length-multiplicity",
        "distinct-face-length-count",
    }:
        lengths = face_lengths(drawing)
        if name == "min-face-length":
            return min(lengths, default=0)
        if name == "max-face-length":
            return max(lengths, default=0)
        if name == "min-face-length-multiplicity":
            return lengths.count(min(lengths)) if lengths else 0
        if name == "max-face-length-multiplicity":
            return lengths.count(max(lengths)) if lengths else 0
        return len(set(lengths))
    raise ValueError(f"unknown bucket signature property: {name!r}")


def drawing_bucket_key(drawing: Drawing, properties: list[str] | tuple[str, ...] | None = None) -> tuple:
    key = []
    for name in normalize_bucket_properties(properties):
        key.append((name, drawing_bucket_component(drawing, name)))
    return tuple(key)


def drawing_bucket_json(drawing: Drawing, properties: list[str] | tuple[str, ...] | None = None) -> str:
    return canonical_key_json(drawing_bucket_key(drawing, properties))


def drawing_bucket_hash(drawing: Drawing, properties: list[str] | tuple[str, ...] | None = None) -> str:
    return hash_key(drawing_bucket_json(drawing, properties))


def add_profile_value(profile: dict | None, key: str, value: float | int) -> None:
    if profile is None:
        return
    profile[key] = profile.get(key, 0) + value


def max_profile_value(profile: dict | None, key: str, value: float | int) -> None:
    if profile is None:
        return
    profile[key] = max(profile.get(key, 0), value)


def vertex_signature_by_vertex(drawing: Drawing) -> dict[int, tuple[int, tuple[int, ...]]]:
    signatures = {}
    for vertex in drawing.vertices:
        path_lengths = [len(path) for path in drawing.paths if vertex in path]
        signatures[vertex] = (len(path_lengths), tuple(sorted(path_lengths)))
    return signatures


def normalize_edge_values(u: int, v: int) -> tuple[int, int]:
    return (u, v) if u > v else (v, u)


def normalize_crossing_values(
    e1: tuple[int, int],
    e2: tuple[int, int],
) -> Crossing:
    edge1 = normalize_edge_values(e1[0], e1[1])
    edge2 = normalize_edge_values(e2[0], e2[1])
    return (edge1, edge2) if edge1 > edge2 else (edge2, edge1)


def encode_crossing(crossing: Crossing) -> tuple[tuple[int, int], tuple[int, int]]:
    return crossing


def encode_point(point: Point):
    if isinstance(point, int):
        return (0, point)
    return (1, encode_crossing(point))


def canonical_key_json(key_data) -> str:
    return json.dumps(key_data, sort_keys=True, separators=(",", ":"))


def select_best_perms(
    perms: tuple[tuple[int, ...], ...] | list[tuple[int, ...]],
    key_func,
) -> tuple[list[tuple[int, ...]], object]:
    best_key = None
    best_perms: list[tuple[int, ...]] = []
    for perm_tuple in perms:
        key = key_func(perm_tuple)
        if best_key is None or key > best_key:
            best_key = key
            best_perms = [perm_tuple]
        elif key == best_key:
            best_perms.append(perm_tuple)
    if best_key is None:
        raise ValueError("cannot canonicalize drawing with no permitted permutations")
    return best_perms, best_key


def vertex_tier_key_for_perm(
    drawing: Drawing,
    perm_tuple: tuple[int, ...],
    vertex_index: dict[int, int],
    vertex_signatures: dict[int, tuple[int, tuple[int, ...]]],
):
    return tuple(
        signature
        for _new_label, signature in sorted(
            (perm_tuple[vertex_index[vertex]], vertex_signatures[vertex])
            for vertex in drawing.vertices
        )
    )


def crossing_tier_key_for_perm(
    drawing: Drawing,
    perm_tuple: tuple[int, ...],
    vertex_index: dict[int, int],
):
    crossing_pairs = []
    for crossing in drawing.crossings:
        e1, e2 = crossing
        crossing_pairs.append(
            normalize_crossing_values(
                (perm_tuple[vertex_index[e1[0]]], perm_tuple[vertex_index[e1[1]]]),
                (perm_tuple[vertex_index[e2[0]]], perm_tuple[vertex_index[e2[1]]]),
            )
        )
    return tuple(sorted(crossing_pairs))


def path_tier_key_for_perm(
    drawing: Drawing,
    perm_tuple: tuple[int, ...],
    vertex_index: dict[int, int],
):
    return tuple(
        sorted(
            (
                normalize_edge_values(
                    perm_tuple[vertex_index[path[0]]],  # type: ignore[arg-type]
                    perm_tuple[vertex_index[path[-1]]],  # type: ignore[arg-type]
                ),
                len(path),
            )
            for path in drawing.paths
        )
    )


def presentation_key_for_perm(
    drawing: Drawing,
    perm_tuple: tuple[int, ...],
    vertex_index: dict[int, int],
    *,
    include_faces: bool = True,
):
    crossing_cache: dict[Crossing, Crossing] = {}

    def map_vertex(vertex: int) -> int:
        return perm_tuple[vertex_index[vertex]]

    def map_crossing(crossing: Crossing) -> Crossing:
        mapped = crossing_cache.get(crossing)
        if mapped is None:
            e1, e2 = crossing
            mapped = normalize_crossing_values(
                (map_vertex(e1[0]), map_vertex(e1[1])),
                (map_vertex(e2[0]), map_vertex(e2[1])),
            )
            crossing_cache[crossing] = mapped
        return mapped

    def map_point(point: Point) -> Point:
        if isinstance(point, int):
            return map_vertex(point)
        return map_crossing(point)

    vertices = tuple(sorted(map_vertex(vertex) for vertex in drawing.vertices))
    crossing_set = {map_crossing(crossing) for crossing in drawing.crossings}

    paths = set()
    for path in drawing.paths:
        mapped_path = tuple(map_point(point) for point in path)
        if point_key(mapped_path[0]) < point_key(mapped_path[-1]):
            mapped_path = mapped_path[::-1]
        paths.add(mapped_path)
        crossing_set.update(point for point in mapped_path if not isinstance(point, int))

    presentation = (
        vertices,
        tuple(sorted(encode_crossing(crossing) for crossing in crossing_set)),
        tuple(sorted(tuple(encode_point(point) for point in path) for path in paths)),
    )
    if not include_faces:
        return presentation
    faces = set()
    for face in drawing.faces:
        mapped_face = tuple(map_point(point) for point in face)
        faces.add(face_canonical_cycle(mapped_face))
    return (*presentation, tuple(sorted(tuple(encode_point(point) for point in face) for face in faces)))


def canonical_json_for_perms(
    drawing: Drawing,
    vertex_order: tuple[int, ...],
    perms: tuple[tuple[int, ...], ...],
    *,
    profile: dict | None = None,
    profile_prefix: str = "canonical",
    key_strategy: str = "full_presentation",
) -> str:
    if key_strategy not in {"full_presentation", "crossing_order_paths"}:
        raise ValueError(f"unknown canonical key strategy: {key_strategy!r}")
    tier_start = time.perf_counter()
    vertex_signatures = vertex_signature_by_vertex(drawing)
    vertex_index = {vertex: idx for idx, vertex in enumerate(vertex_order)}

    vertex_start = time.perf_counter()
    vertex_perms, vertex_key = select_best_perms(
        perms,
        lambda perm_tuple: vertex_tier_key_for_perm(drawing, perm_tuple, vertex_index, vertex_signatures),
    )
    add_profile_value(profile, f"{profile_prefix}_vertex_tier_seconds", time.perf_counter() - vertex_start)
    add_profile_value(profile, f"{profile_prefix}_vertex_tier_survivors", len(vertex_perms))
    max_profile_value(profile, f"{profile_prefix}_vertex_tier_max_survivors", len(vertex_perms))

    crossing_start = time.perf_counter()
    crossing_perms, crossing_key = select_best_perms(
        vertex_perms,
        lambda perm_tuple: crossing_tier_key_for_perm(drawing, perm_tuple, vertex_index),
    )
    add_profile_value(profile, f"{profile_prefix}_crossing_tier_seconds", time.perf_counter() - crossing_start)
    add_profile_value(profile, f"{profile_prefix}_crossing_tier_survivors", len(crossing_perms))
    max_profile_value(profile, f"{profile_prefix}_crossing_tier_max_survivors", len(crossing_perms))

    path_start = time.perf_counter()
    best_perm_tuples, path_key = select_best_perms(
        crossing_perms,
        lambda perm_tuple: path_tier_key_for_perm(drawing, perm_tuple, vertex_index),
    )
    add_profile_value(profile, f"{profile_prefix}_path_tier_seconds", time.perf_counter() - path_start)
    add_profile_value(profile, f"{profile_prefix}_path_tier_survivors", len(best_perm_tuples))
    max_profile_value(profile, f"{profile_prefix}_path_tier_max_survivors", len(best_perm_tuples))

    add_profile_value(profile, f"{profile_prefix}_perms_total", len(perms))
    add_profile_value(profile, f"{profile_prefix}_cheap_seconds", time.perf_counter() - tier_start)
    add_profile_value(profile, f"{profile_prefix}_full_evals", len(best_perm_tuples))
    max_profile_value(profile, f"{profile_prefix}_max_full_evals", len(best_perm_tuples))

    full_start = time.perf_counter()
    best_key = None
    for perm_tuple in best_perm_tuples:
        presentation = presentation_key_for_perm(
            drawing,
            perm_tuple,
            vertex_index,
            include_faces=key_strategy == "full_presentation",
        )
        if best_key is None or presentation > best_key:
            best_key = presentation
    add_profile_value(profile, f"{profile_prefix}_full_seconds", time.perf_counter() - full_start)
    if best_key is None:
        raise ValueError("cannot canonicalize drawing with no permitted permutations")
    json_start = time.perf_counter()
    key_json = canonical_key_json((vertex_key, crossing_key, path_key, best_key))
    add_profile_value(profile, f"{profile_prefix}_json_seconds", time.perf_counter() - json_start)
    add_profile_value(profile, f"{profile_prefix}_key_json_bytes", len(key_json))
    max_profile_value(profile, f"{profile_prefix}_max_key_json_bytes", len(key_json))
    return key_json


def drawing_keys(
    stage_index: int,
    drawing: Drawing,
    context: CanonicalContext,
    *,
    include_full_keys: bool = False,
    profile: dict | None = None,
    key_strategy: str = "full_presentation",
) -> DrawingKeys:
    stage = context.stages[stage_index]
    report_key_json = canonical_json_for_perms(
        drawing,
        context.vertex_order,
        stage.report_perms,
        profile=profile,
        profile_prefix="report_key",
        key_strategy=key_strategy,
    )
    if stage.report_perms == stage.work_perms:
        work_key_json = report_key_json
        add_profile_value(profile, "work_key_reused_report_key", 1)
    else:
        work_key_json = canonical_json_for_perms(
            drawing,
            context.vertex_order,
            stage.work_perms,
            profile=profile,
            profile_prefix="work_key",
            key_strategy=key_strategy,
        )
    return DrawingKeys(
        report_hash=hash_key(report_key_json),
        work_hash=hash_key(work_key_json),
        report_key_json=report_key_json if include_full_keys else None,
        work_key_json=work_key_json if include_full_keys else None,
    )
