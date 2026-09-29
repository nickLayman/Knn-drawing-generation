"""Compact face-primary drawing storage.

The compact format is intentionally small and reconstructable, not human-readable.
Expanded JSON is an export format only.
"""

from __future__ import annotations

from dataclasses import dataclass
import gzip
import hashlib
from pathlib import Path
import zlib

from .automorphism import CanonicalContext, context_from_record
from .drawing import (
    Arc,
    Crossing,
    Drawing,
    Edge,
    Face,
    Point,
    face_arcs,
    normalize_arc,
    normalize_crossing,
    normalize_edge,
    unique_presentation,
)


MAGIC = b"GD1"
VERSION = 1


@dataclass(frozen=True, slots=True)
class CompactContext:
    graph_vertices: tuple[int, ...]
    graph_edges: tuple[Edge, ...]
    canonical_context: CanonicalContext
    vertex_to_id: dict[int, int]
    edge_to_id: dict[Edge, int]


def context_from_run_config(run_config: dict) -> CompactContext:
    if "graph_vertices" in run_config:
        vertices = tuple(int(v) for v in run_config["graph_vertices"])
    else:
        vertices = tuple(int(v) for v in run_config["canonical_context"]["vertex_order"])
    edges = tuple(normalize_edge(edge) for edge in run_config["graph_edges"])
    return CompactContext(
        graph_vertices=vertices,
        graph_edges=edges,
        canonical_context=context_from_record(run_config["canonical_context"]),
        vertex_to_id={vertex: idx for idx, vertex in enumerate(vertices)},
        edge_to_id={edge: idx for idx, edge in enumerate(edges)},
    )


def write_varint(value: int, out: bytearray) -> None:
    if value < 0:
        raise ValueError(f"negative varint: {value}")
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)


def read_varint(data: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    value = 0
    while True:
        if offset >= len(data):
            raise ValueError("truncated varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def point_code(point: Point, context: CompactContext, crossing_to_index: dict[Crossing, int]) -> int:
    if isinstance(point, int):
        return 2 * context.vertex_to_id[point]
    return 2 * crossing_to_index[point] + 1


def point_from_code(code: int, context: CompactContext, crossings: tuple[Crossing, ...]) -> Point:
    if code % 2 == 0:
        vertex_id = code // 2
        if vertex_id >= len(context.graph_vertices):
            raise ValueError(f"vertex point code out of range: {code}")
        return context.graph_vertices[vertex_id]
    crossing_id = code // 2
    if crossing_id >= len(crossings):
        raise ValueError(f"crossing point code out of range: {code}")
    return crossings[crossing_id]


def encode_drawing(drawing: Drawing, stage_index: int, context: CompactContext) -> bytes:
    """Normalize a drawing at the input boundary, then encode it."""
    return encode_normalized_drawing(unique_presentation(drawing), stage_index, context)


def encode_normalized_drawing(drawing: Drawing, stage_index: int, context: CompactContext) -> bytes:
    """Encode a normalized drawing from a constructor or decoder without redoing their work."""
    crossings = drawing.crossings
    crossing_to_index = {crossing: idx for idx, crossing in enumerate(crossings)}
    out = bytearray(MAGIC)
    write_varint(VERSION, out)
    write_varint(stage_index, out)
    write_varint(len(crossings), out)
    for crossing in crossings:
        e1, e2 = crossing
        write_varint(context.edge_to_id[e1], out)
        write_varint(context.edge_to_id[e2], out)
    write_varint(len(drawing.faces), out)
    for face in drawing.faces:
        cycle = tuple(face[:-1]) if len(face) > 1 and face[0] == face[-1] else tuple(face)
        write_varint(len(cycle), out)
        for point in cycle:
            write_varint(point_code(point, context, crossing_to_index), out)
    return bytes(out)


def decode_header(payload: bytes) -> tuple[int, int]:
    if not payload.startswith(MAGIC):
        raise ValueError("not a graph-drawings compact payload")
    version, offset = read_varint(payload, len(MAGIC))
    if version != VERSION:
        raise ValueError(f"unsupported compact drawing version: {version}")
    stage_index, _ = read_varint(payload, offset)
    return version, stage_index


def decode_drawing(payload: bytes, context: CompactContext) -> Drawing:
    if not payload.startswith(MAGIC):
        raise ValueError("not a graph-drawings compact payload")
    version, offset = read_varint(payload, len(MAGIC))
    if version != VERSION:
        raise ValueError(f"unsupported compact drawing version: {version}")
    stage_index, offset = read_varint(payload, offset)
    crossing_count, offset = read_varint(payload, offset)
    crossings = []
    for _ in range(crossing_count):
        edge1_id, offset = read_varint(payload, offset)
        edge2_id, offset = read_varint(payload, offset)
        crossings.append(normalize_crossing((context.graph_edges[edge1_id], context.graph_edges[edge2_id])))
    crossings_tuple = tuple(crossings)
    face_count, offset = read_varint(payload, offset)
    faces: list[Face] = []
    for _ in range(face_count):
        length, offset = read_varint(payload, offset)
        cycle = []
        for _ in range(length):
            code, offset = read_varint(payload, offset)
            cycle.append(point_from_code(code, context, crossings_tuple))
        if not cycle:
            raise ValueError("empty face cycle in compact payload")
        faces.append(tuple(cycle + [cycle[0]]))
    if offset != len(payload):
        raise ValueError("trailing bytes in compact payload")

    stage = context.canonical_context.stages[stage_index]
    vertices = stage.vertices
    paths = recover_paths_from_faces(vertices, crossings_tuple, tuple(faces), stage.edges)
    return unique_presentation(Drawing(vertices, crossings_tuple, paths, tuple(faces)))


def point_incident_to_edge(point: Point, edge: Edge) -> bool:
    if isinstance(point, int):
        return point in edge
    return normalize_edge(edge) in {normalize_edge(point[0]), normalize_edge(point[1])}


def recover_paths_from_faces(
    vertices: tuple[int, ...],
    crossings: tuple[Crossing, ...],
    faces: tuple[Face, ...],
    stage_edges: tuple[Edge, ...],
) -> tuple[tuple[Point, ...], ...]:
    all_arcs = {arc for face in faces for arc in face_arcs(face)}
    paths = []
    crossing_set = set(crossings)
    consumed_arcs: set[Arc] = set()
    for edge in stage_edges:
        edge = normalize_edge(edge)
        edge_arcs = {
            arc
            for arc in all_arcs
            if point_incident_to_edge(arc[0], edge) and point_incident_to_edge(arc[1], edge)
        }
        if not edge_arcs:
            raise ValueError(f"missing planarized arcs for edge {edge}")
        endpoints = set(edge)
        adjacency: dict[Point, list[Point]] = {}
        for a, b in edge_arcs:
            adjacency.setdefault(a, []).append(b)
            adjacency.setdefault(b, []).append(a)
        for point, neighbors in adjacency.items():
            degree = len(neighbors)
            if isinstance(point, int) and point in endpoints:
                if degree != 1:
                    raise ValueError(f"endpoint {point!r} has degree {degree} on edge {edge}")
            else:
                if point not in crossing_set:
                    raise ValueError(f"unexpected non-crossing internal point {point!r} on edge {edge}")
                if degree != 2:
                    raise ValueError(f"internal point {point!r} has degree {degree} on edge {edge}")
        start, target = edge
        path = [start]
        previous: Point | None = None
        current: Point = start
        visited_arcs: set[Arc] = set()
        while current != target:
            next_points = [
                point
                for point in adjacency.get(current, [])
                if point != previous and normalize_arc(current, point) not in visited_arcs
            ]
            if len(next_points) != 1:
                raise ValueError(f"edge {edge} chain is not simple at {current!r}")
            nxt = next_points[0]
            visited_arcs.add(normalize_arc(current, nxt))
            path.append(nxt)
            previous = current
            current = nxt
            if len(path) > len(edge_arcs) + 1:
                raise ValueError(f"edge {edge} chain contains a cycle")
        if visited_arcs != edge_arcs:
            raise ValueError(f"edge {edge} chain did not consume all arcs")
        consumed_arcs.update(visited_arcs)
        paths.append(tuple(path))
    if consumed_arcs != all_arcs:
        extra = all_arcs - consumed_arcs
        missing = consumed_arcs - all_arcs
        raise ValueError(f"face arcs do not match recovered paths: extra={extra!r}, missing={missing!r}")
    used_crossings = {point for path in paths for point in path if not isinstance(point, int)}
    if used_crossings != crossing_set:
        raise ValueError(
            f"crossing table does not match recovered paths: "
            f"unused={crossing_set - used_crossings!r}, missing={used_crossings - crossing_set!r}"
        )
    return tuple(paths)


def compress_db_blob(payload: bytes) -> bytes:
    return zlib.compress(payload)


def decompress_db_blob(payload: bytes) -> bytes:
    return zlib.decompress(payload)


def compact_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def write_gzip_records(path: Path, records: list[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        for record in records:
            header = bytearray()
            write_varint(len(record), header)
            f.write(header)
            f.write(record)


def iter_gzip_records(path: Path):
    with gzip.open(path, "rb") as f:
        while True:
            first = f.read(1)
            if not first:
                return
            length = first[0] & 0x7F
            shift = 7
            while first[0] & 0x80:
                byte = f.read(1)
                if not byte:
                    raise ValueError(f"truncated gzip record length in {path}")
                length |= (byte[0] & 0x7F) << shift
                shift += 7
                if shift > 63:
                    raise ValueError(f"gzip record length varint too long in {path}")
                first = byte

            remaining = length
            chunks: list[bytes] = []
            while remaining:
                chunk = f.read(min(remaining, 1024 * 1024))
                if not chunk:
                    raise ValueError(f"truncated gzip record in {path}")
                chunks.append(chunk)
                remaining -= len(chunk)
            yield b"".join(chunks)
