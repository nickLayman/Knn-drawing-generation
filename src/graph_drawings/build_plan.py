"""Build-step derivation from a configured graph."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import permutations

from .drawing import Drawing, base_k22_drawings, normalize_edge
from .extensions import BuildStep


@dataclass(frozen=True, slots=True)
class BuildBlock:
    kind: str  # "vertex" or "edge_completion"
    steps: tuple[BuildStep, ...]
    vertex: int | None = None


def infer_vertices(edges: list[tuple[int, int]]) -> tuple[int, ...]:
    return tuple(sorted({int(v) for edge in edges for v in edge}))


def normalize_graph(edges: list[tuple[int, int]]) -> tuple[tuple[int, ...], set[tuple[int, int]]]:
    edge_set = {normalize_edge(edge) for edge in edges}
    vertex_tuple = infer_vertices(list(edge_set))
    return vertex_tuple, edge_set


def validate_connected(vertices: tuple[int, ...], edges: set[tuple[int, int]]) -> None:
    if not vertices:
        raise ValueError("GRAPH_EDGES must contain at least one edge")
    adjacency = {v: set() for v in vertices}
    for u, v in edges:
        adjacency[u].add(v)
        adjacency[v].add(u)
    seen = {vertices[0]}
    stack = [vertices[0]]
    while stack:
        current = stack.pop()
        for neighbor in adjacency[current]:
            if neighbor not in seen:
                seen.add(neighbor)
                stack.append(neighbor)
    if seen != set(vertices):
        raise ValueError("disconnected graphs are not supported")


def find_base_cycle(vertices: tuple[int, ...], edges: set[tuple[int, int]]) -> tuple[int, int, int, int]:
    edge_lookup = {frozenset(edge) for edge in edges}
    for cycle in permutations(vertices, 4):
        needed = [
            frozenset((cycle[0], cycle[1])),
            frozenset((cycle[1], cycle[2])),
            frozenset((cycle[2], cycle[3])),
            frozenset((cycle[3], cycle[0])),
        ]
        if all(edge in edge_lookup for edge in needed):
            return cycle  # type: ignore[return-value]
    raise ValueError("could not find a 4-cycle to seed the K2,2 base")


def derive_build_plan(edges: list[tuple[int, int]]):
    vertex_tuple, edge_set = normalize_graph(edges)
    validate_connected(vertex_tuple, edge_set)
    base_cycle = find_base_cycle(vertex_tuple, edge_set)
    base_edges = {
        normalize_edge((base_cycle[0], base_cycle[1])),
        normalize_edge((base_cycle[1], base_cycle[2])),
        normalize_edge((base_cycle[2], base_cycle[3])),
        normalize_edge((base_cycle[3], base_cycle[0])),
    }
    remaining_edges = set(edge_set - base_edges)
    built = set(base_cycle)
    steps: list[BuildStep] = []

    while built != set(vertex_tuple):
        candidates = []
        for vertex in sorted(set(vertex_tuple) - built):
            neighbors = sorted(
                n
                for edge in remaining_edges
                for n in edge
                if vertex in edge and n != vertex and n in built
            )
            if neighbors:
                candidates.append((vertex, neighbors))
        if not candidates:
            raise ValueError("could not derive a connected vertex-addition plan")
        vertex, neighbors = candidates[0]
        first_neighbor = neighbors[0]
        first_edge = normalize_edge((vertex, first_neighbor))
        steps.append(BuildStep("add_vertex_edge", first_edge))
        remaining_edges.remove(first_edge)
        built.add(vertex)
        for neighbor in neighbors[1:]:
            edge = normalize_edge((vertex, neighbor))
            if edge in remaining_edges:
                steps.append(BuildStep("add_edge", edge))
                remaining_edges.remove(edge)

    for edge in sorted(remaining_edges):
        steps.append(BuildStep("add_edge", edge))

    return base_cycle, steps


def derive_build_blocks(base_cycle: tuple[int, int, int, int], steps: list[BuildStep] | tuple[BuildStep, ...]) -> tuple[BuildBlock, ...]:
    built = set(base_cycle)
    blocks: list[BuildBlock] = []
    current_vertex: int | None = None
    current_steps: list[BuildStep] = []
    trailing_steps: list[BuildStep] = []

    def close_vertex_block() -> None:
        nonlocal current_vertex, current_steps
        if current_steps:
            blocks.append(BuildBlock("vertex", tuple(current_steps), current_vertex))
        current_vertex = None
        current_steps = []

    for step in steps:
        if step.kind == "add_vertex_edge":
            close_vertex_block()
            u, v = step.edge
            current_vertex = u if u not in built else v
            built.add(current_vertex)
            current_steps = [step]
            continue
        if current_vertex is not None and current_vertex in step.edge:
            current_steps.append(step)
        else:
            close_vertex_block()
            trailing_steps.append(step)
    close_vertex_block()
    if trailing_steps:
        blocks.append(BuildBlock("edge_completion", tuple(trailing_steps), None))
    return tuple(blocks)


def base_drawings_for_cycle(base_cycle: tuple[int, int, int, int]) -> tuple[Drawing, Drawing]:
    return base_k22_drawings(base_cycle)
