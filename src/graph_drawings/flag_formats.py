"""Lossy flag-format exports for drawings."""

from __future__ import annotations

from collections.abc import Sequence
from itertools import combinations

from .drawing import Crossing, Drawing, Edge, normalize_crossing, normalize_edge


def flag_vertex(vertex: int, offset: int) -> int:
    label = vertex + offset
    if label < 0 or label > 9:
        raise ValueError(
            "flag export currently supports single-digit vertex labels only; "
            f"got vertex {vertex!r} with offset {offset}"
        )
    return label


def edge_digits(edge: Edge, offset: int) -> tuple[int, int]:
    return tuple(sorted((flag_vertex(edge[0], offset), flag_vertex(edge[1], offset))))  # type: ignore[return-value]


def crossing_digits(crossing: Crossing, offset: int) -> str:
    edges = sorted((edge_digits(crossing[0], offset), edge_digits(crossing[1], offset)))
    return "".join(str(vertex) for edge in edges for vertex in edge)


def four_graph_digits(crossing: Crossing, offset: int) -> str:
    vertices = sorted(flag_vertex(vertex, offset) for edge in crossing for vertex in edge)
    return "".join(str(vertex) for vertex in vertices)


def crossing_label_table(
    edges: Sequence[Edge], *, four_graph: bool, vertex_label_offset: int = 1,
) -> dict[Crossing, str]:
    """Precompute the label of every possible crossing in a fixed graph."""
    label = four_graph_digits if four_graph else crossing_digits
    result = {}
    for first, second in combinations(sorted({normalize_edge(edge) for edge in edges}), 2):
        if set(first).isdisjoint(second):
            crossing = normalize_crossing((first, second))
            result[crossing] = label(crossing, vertex_label_offset)
    return result


def flag_prefix(drawing: Drawing, vertex_colors: Sequence[int] | None = None) -> list[str]:
    parts = [str(len(drawing.vertices)), "0"]
    if vertex_colors is not None:
        if len(vertex_colors) != len(drawing.vertices):
            raise ValueError(
                f"expected {len(drawing.vertices)} vertex colors, got {len(vertex_colors)}"
            )
        parts.extend(str(color) for color in vertex_colors)
    return parts


def crossing_pair_flag(
    drawing: Drawing,
    *,
    vertex_label_offset: int = 1,
    vertex_colors: Sequence[int] | None = None,
) -> str:
    crossing_pairs = sorted({crossing_digits(crossing, vertex_label_offset) for crossing in drawing.crossings})
    return " ".join([*flag_prefix(drawing, vertex_colors), str(len(crossing_pairs)), *crossing_pairs])


def four_graph_flag(
    drawing: Drawing,
    *,
    vertex_label_offset: int = 1,
    vertex_colors: Sequence[int] | None = None,
) -> str:
    four_sets = sorted({four_graph_digits(crossing, vertex_label_offset) for crossing in drawing.crossings})
    return " ".join([*flag_prefix(drawing, vertex_colors), str(len(four_sets)), *four_sets])
