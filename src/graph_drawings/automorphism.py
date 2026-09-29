"""Graph automorphisms and build-stage canonicalization contexts."""

from __future__ import annotations

from dataclasses import dataclass

from .drawing import Edge, normalize_edge
from .build_plan import BuildBlock
from .extensions import BuildStep


@dataclass(frozen=True, slots=True)
class StageContext:
    stage_index: int
    vertices: tuple[int, ...]
    edges: tuple[Edge, ...]
    report_perms: tuple[tuple[int, ...], ...]
    work_perms: tuple[tuple[int, ...], ...]


@dataclass(frozen=True, slots=True)
class CanonicalContext:
    vertex_order: tuple[int, ...]
    full_automorphisms: tuple[tuple[int, ...], ...]
    stages: tuple[StageContext, ...]


def edge_set(edges: list[tuple[int, int]] | tuple[tuple[int, int], ...]) -> set[Edge]:
    return {normalize_edge(edge) for edge in edges}


def apply_perm_to_edge(edge: Edge, perm: dict[int, int]) -> Edge:
    return normalize_edge((perm[edge[0]], perm[edge[1]]))


def perm_tuple_to_map(vertex_order: tuple[int, ...], perm_tuple: tuple[int, ...]) -> dict[int, int]:
    return dict(zip(vertex_order, perm_tuple))


def graph_automorphisms(vertices: tuple[int, ...], edges: set[Edge]) -> tuple[tuple[int, ...], ...]:
    degrees = {v: 0 for v in vertices}
    for u, v in edges:
        degrees[u] += 1
        degrees[v] += 1
    vertices_by_degree = {
        vertex: tuple(candidate for candidate in vertices if degrees[candidate] == degrees[vertex])
        for vertex in vertices
    }
    edge_frozensets = {frozenset(edge) for edge in edges}
    search_order = tuple(sorted(vertices, key=lambda v: (-degrees[v], v)))
    automorphisms = []

    def compatible(old_vertex: int, new_vertex: int, current: dict[int, int]) -> bool:
        for assigned_old, assigned_new in current.items():
            old_has_edge = frozenset((old_vertex, assigned_old)) in edge_frozensets
            new_has_edge = frozenset((new_vertex, assigned_new)) in edge_frozensets
            if old_has_edge != new_has_edge:
                return False
        return True

    def search(index: int, current: dict[int, int], used_images: set[int]) -> None:
        if index == len(search_order):
            automorphisms.append(tuple(current[v] for v in vertices))
            return
        old_vertex = search_order[index]
        for new_vertex in vertices_by_degree[old_vertex]:
            if new_vertex in used_images or not compatible(old_vertex, new_vertex, current):
                continue
            current[old_vertex] = new_vertex
            used_images.add(new_vertex)
            search(index + 1, current, used_images)
            used_images.remove(new_vertex)
            del current[old_vertex]

    search(0, {}, set())
    return tuple(sorted(automorphisms))


def build_stage_edges(base_cycle: tuple[int, int, int, int], blocks: tuple[BuildBlock, ...]) -> tuple[set[Edge], ...]:
    current = {
        normalize_edge((base_cycle[0], base_cycle[1])),
        normalize_edge((base_cycle[1], base_cycle[2])),
        normalize_edge((base_cycle[2], base_cycle[3])),
        normalize_edge((base_cycle[3], base_cycle[0])),
    }
    stages = [set(current)]
    for block in blocks:
        current = set(current)
        for step in block.steps:
            current.add(normalize_edge(step.edge))
        stages.append(set(current))
    return tuple(stages)


def block_edges(blocks: tuple[BuildBlock, ...]) -> tuple[tuple[Edge, ...], ...]:
    return tuple(tuple(normalize_edge(step.edge) for step in block.steps) for block in blocks)


def build_blocks_from_records(records: list[dict]) -> tuple[BuildBlock, ...]:
    blocks = []
    for record in records:
        steps = tuple(BuildStep(str(step["kind"]), normalize_edge(step["edge"])) for step in record["steps"])
        vertex = record.get("vertex")
        blocks.append(BuildBlock(str(record["kind"]), steps, None if vertex is None else int(vertex)))
    return tuple(blocks)


def build_block_to_record(block: BuildBlock) -> dict:
    return {
        "kind": block.kind,
        "vertex": block.vertex,
        "steps": [{"kind": step.kind, "edge": list(step.edge)} for step in block.steps],
    }


def build_steps_from_records(records: list[dict]) -> tuple[BuildStep, ...]:
    return tuple(BuildStep(str(record["kind"]), normalize_edge(record["edge"])) for record in records)


def flatten_blocks(blocks: tuple[BuildBlock, ...]) -> tuple[BuildStep, ...]:
    return tuple(step for block in blocks for step in block.steps)


def preserves_edges(perm_tuple: tuple[int, ...], vertex_order: tuple[int, ...], edges: set[Edge]) -> bool:
    perm = perm_tuple_to_map(vertex_order, perm_tuple)
    return {apply_perm_to_edge(edge, perm) for edge in edges} == edges


def fixes_edges_individually(
    perm_tuple: tuple[int, ...],
    vertex_order: tuple[int, ...],
    edges: tuple[Edge, ...],
) -> bool:
    perm = perm_tuple_to_map(vertex_order, perm_tuple)
    return all(apply_perm_to_edge(edge, perm) == edge for edge in edges)


def make_canonical_context(
    vertices: tuple[int, ...],
    graph_edges: set[Edge],
    base_cycle: tuple[int, int, int, int],
    blocks: tuple[BuildBlock, ...],
) -> CanonicalContext:
    vertex_order = tuple(sorted(vertices))
    full_autos = graph_automorphisms(vertex_order, graph_edges)
    stage_edges = build_stage_edges(base_cycle, blocks)
    stage_contexts = []
    for stage_index, edges in enumerate(stage_edges):
        stage_vertices = tuple(sorted({v for edge in edges for v in edge}))
        stage_indices = tuple(vertex_order.index(v) for v in stage_vertices)
        actions = {}
        for perm in full_autos:
            if preserves_edges(perm, vertex_order, edges):
                # Unbuilt vertices do not occur in a drawing's canonical key.
                action = tuple(perm[i] for i in stage_indices)
                actions.setdefault(action, perm)
        report_perms = tuple(actions.values())
        work_perms = report_perms
        if not report_perms:
            identity = tuple(vertex_order)
            report_perms = (identity,)
        if not work_perms:
            identity = tuple(vertex_order)
            work_perms = (identity,)
        stage_contexts.append(
            StageContext(
                stage_index=stage_index,
                vertices=stage_vertices,
                edges=tuple(sorted(edges)),
                report_perms=report_perms,
                work_perms=work_perms,
            )
        )
    return CanonicalContext(vertex_order, full_autos, tuple(stage_contexts))


def context_to_record(context: CanonicalContext) -> dict:
    return {
        "vertex_order": list(context.vertex_order),
        "full_automorphisms": [list(perm) for perm in context.full_automorphisms],
        "stages": [
            {
                "stage_index": stage.stage_index,
                "vertices": list(stage.vertices),
                "edges": [list(edge) for edge in stage.edges],
                "report_perms": [list(perm) for perm in stage.report_perms],
                "work_perms": [list(perm) for perm in stage.work_perms],
            }
            for stage in context.stages
        ],
    }


def context_from_record(record: dict) -> CanonicalContext:
    return CanonicalContext(
        vertex_order=tuple(int(v) for v in record["vertex_order"]),
        full_automorphisms=tuple(tuple(int(v) for v in perm) for perm in record["full_automorphisms"]),
        stages=tuple(
            StageContext(
                stage_index=int(stage["stage_index"]),
                vertices=tuple(int(v) for v in stage["vertices"]),
                edges=tuple(normalize_edge(edge) for edge in stage["edges"]),
                report_perms=tuple(tuple(int(v) for v in perm) for perm in stage["report_perms"]),
                work_perms=tuple(tuple(int(v) for v in perm) for perm in stage["work_perms"]),
            )
            for stage in record["stages"]
        ),
    )
