"""Drawing extension operations."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from time import perf_counter

from .drawing import (
    Arc,
    Crossing,
    Drawing,
    Edge,
    Face,
    Point,
    base_k22_drawings,
    face_arcs,
    face_canonical_cycle,
    normalize_crossing,
    normalize_edge,
    path_arcs,
    point_key,
)


@dataclass(frozen=True, slots=True)
class BuildStep:
    kind: str  # "add_vertex_edge" or "add_edge"
    edge: tuple[int, int]


@dataclass(frozen=True, slots=True)
class DrawingIndices:
    arc_edge: dict[Arc, tuple[int, int]]
    arc_faces: dict[Arc, list[Face]]
    path_arcs: dict[tuple[Point, ...], tuple[Arc, ...]]
    face_arcs: dict[Face, tuple[Arc, ...]]
    point_keys: dict[Point, tuple]


@dataclass(frozen=True, slots=True)
class _PreparedRoute:
    arc_insertions: dict[Arc, Point]
    new_crossings: tuple[Crossing, ...]
    crossed_edges: frozenset[Edge]


def drawing_indices(drawing: Drawing) -> DrawingIndices:
    arc_edge: dict[Arc, tuple[int, int]] = {}
    arc_faces: dict[Arc, list[Face]] = {}
    cached_paths = {path: path_arcs(path) for path in drawing.paths}
    cached_faces = {face: face_arcs(face) for face in drawing.faces}
    for path in drawing.paths:
        edge = normalize_edge((path[0], path[-1]))  # type: ignore[arg-type]
        for arc in cached_paths[path]:
            arc_edge[arc] = edge
    for face in drawing.faces:
        for arc in cached_faces[face]:
            arc_faces.setdefault(arc, []).append(face)
    return DrawingIndices(
        arc_edge, arc_faces, cached_paths, cached_faces,
        {point: point_key(point) for point in (*drawing.vertices, *drawing.crossings)},
    )


def split_cycle_between(cycle: list[Point], start: Point, end: Point) -> list[list[list[Point]]]:
    choices = []
    length = len(cycle)
    for _ in range(length - 1):
        cycle = cycle[1:] + [cycle[1]]
        if cycle[0] == start:
            for end_idx in range(length - 1):
                if cycle[end_idx] == end:
                    choices.append([cycle[: end_idx + 1] + [start], cycle[end_idx:] + [end]])
    return choices


def insert_crossings(
    points: tuple[Point, ...], arcs: tuple[Arc, ...], arc_insertions: dict[Arc, Point],
) -> list[Point]:
    result = [points[0]]
    for point, arc in zip(points[1:], arcs):
        if arc in arc_insertions:
            result.append(arc_insertions[arc])
        result.append(point)
    return result


def _prepare_route(
    vertex: int,
    neighbor: int,
    sequence: tuple,
    indices: DrawingIndices,
) -> _PreparedRoute:
    new_edge = normalize_edge((vertex, neighbor))
    arc_insertions: dict[Arc, Point] = {}
    new_crossings = []
    crossed_edges = set()
    for idx in range(1, len(sequence), 2):
        other_edge = indices.arc_edge[sequence[idx]]
        crossed_edges.add(other_edge)
        crossing = normalize_crossing((new_edge, other_edge))
        new_crossings.append(crossing)
        arc_insertions[sequence[idx]] = crossing
    return _PreparedRoute(arc_insertions, tuple(new_crossings), frozenset(crossed_edges))


def _new_path(vertex: int, neighbor: int, sequence: tuple, arc_insertions: dict[Arc, Point]) -> tuple[Point, ...]:
    new_path: tuple[Point, ...] = (vertex,)
    for idx in range(1, len(sequence), 2):
        new_path += (arc_insertions[sequence[idx]],)
    new_path += (neighbor,)
    if vertex < neighbor:
        new_path = new_path[::-1]
    return new_path


def _compatible_face_choices(
    drawing: Drawing,
    sequence: tuple,
    indices: DrawingIndices,
    arc_insertions: dict[Arc, Point],
    *,
    vertex: int,
    neighbor: int,
    add_new_vertex: bool,
    profile: dict | None = None,
) -> dict[Face, list] | None:
    """Return all face choices that survive the route's split constraints.

    A face can occur more than once in a route.  In that case each later split
    is applied to every still-compatible breakdown from the earlier visit.
    Keeping this stateful compatibility phase shared is important: a dual
    trail is not sufficient by itself to produce a drawing.
    """
    face_start = perf_counter() if profile is not None else 0.0
    face_choices: dict[Face, list[list[list[Point] | Face]]] = {}
    route_faces = set(sequence[::2])
    for face in drawing.faces:
        face_choices[face] = [[insert_crossings(face, indices.face_arcs[face], arc_insertions)
                               if face in route_faces else face]]

    def finish(result):
        if profile is not None:
            profile["face_choice_seconds"] = profile.get("face_choice_seconds", 0.0) + (
                perf_counter() - face_start
            )
        return result

    if len(sequence) == 1:
        affected_face = face_choices[sequence[0]][0][0]
        choices = []
        if add_new_vertex:
            for _ in range(len(affected_face) - 1):
                affected_face = affected_face[1:] + [affected_face[1]]
                if affected_face[0] == neighbor:
                    choices.append([affected_face[:1] + [vertex] + affected_face[0:]])
        else:
            for split in split_cycle_between(affected_face, vertex, neighbor):
                choices.append(split)
        face_choices[sequence[0]] = choices
        if not choices:
            return finish(None)
    else:
        first_face = face_choices[sequence[0]][0][0]
        first_crossing = arc_insertions[sequence[1]]
        choices = []
        if add_new_vertex:
            for _ in range(len(first_face) - 1):
                first_face = first_face[1:] + [first_face[1]]
                if first_face[0] == first_crossing:
                    choices.append([first_face[:1] + [vertex] + first_face[0:]])
        else:
            for split in split_cycle_between(first_face, vertex, first_crossing):
                choices.append(split)
        face_choices[sequence[0]] = choices
        if not choices:
            return finish(None)

        for idx in range(2, len(sequence) - 2, 2):
            crossing_1 = arc_insertions[sequence[idx - 1]]
            crossing_2 = arc_insertions[sequence[idx + 1]]
            choices_for_face = face_choices[sequence[idx]]
            kept_choices = []
            for current in choices_for_face:
                expanded = False
                for cycle_idx, cycle in enumerate(current):
                    if crossing_1 in cycle and crossing_2 in cycle:
                        for split in split_cycle_between(list(cycle), crossing_1, crossing_2):
                            kept_choices.append(current[:cycle_idx] + split + current[cycle_idx + 1:])
                        expanded = True
                        break
                if not expanded:
                    continue
            if not kept_choices:
                return finish(None)
            face_choices[sequence[idx]] = kept_choices

        last_face = sequence[-1]
        last_crossing = arc_insertions[sequence[-2]]
        kept_choices = []
        for current in face_choices[last_face]:
            expanded = False
            for cycle_idx, cycle in enumerate(current):
                if last_crossing in cycle and neighbor in cycle:
                    for split in split_cycle_between(list(cycle), last_crossing, neighbor):
                        kept_choices.append(current[:cycle_idx] + split + current[cycle_idx + 1:])
                    expanded = True
                    break
            if not expanded:
                continue
        if not kept_choices:
            return finish(None)
        face_choices[last_face] = kept_choices
    return finish(face_choices)


def make_extended_drawings(
    drawing: Drawing,
    vertex: int,
    neighbor: int,
    sequence: tuple,
    *,
    add_new_vertex: bool,
    indices: DrawingIndices | None = None,
    profile: dict | None = None,
) -> list[Drawing]:
    return list(
        iter_extended_drawings(
            drawing,
            vertex,
            neighbor,
            sequence,
            add_new_vertex=add_new_vertex,
            indices=indices,
            profile=profile,
        )
    )


def iter_extended_drawings(
    drawing: Drawing,
    vertex: int,
    neighbor: int,
    sequence: tuple,
    *,
    add_new_vertex: bool,
    indices: DrawingIndices | None = None,
    profile: dict | None = None,
):
    """Extend a normalized parent, yielding normalized drawings in route order.

    Importers, decoders, and seed constructors establish the parent invariant.
    Only new crossings and changed face cycles need normalization here.
    """
    active_seconds = 0.0
    active_start = perf_counter() if profile is not None else 0.0
    index_start = perf_counter() if profile is not None else 0.0
    shared = indices if indices is not None else drawing_indices(drawing)
    if profile is not None:
        profile["make_extended_calls"] = profile.get("make_extended_calls", 0) + 1
        if indices is None:
            profile["drawing_indices_seconds"] = profile.get("drawing_indices_seconds", 0.0) + (perf_counter() - index_start)
        profile["sequence_arc_steps"] = profile.get("sequence_arc_steps", 0) + max(0, (len(sequence) - 1) // 2)
    prepared = _prepare_route(vertex, neighbor, sequence, shared)
    arc_insertions = prepared.arc_insertions
    new_crossings = prepared.new_crossings
    crossed_edges = prepared.crossed_edges
    face_choices = _compatible_face_choices(
        drawing,
        sequence,
        shared,
        arc_insertions,
        vertex=vertex,
        neighbor=neighbor,
        add_new_vertex=add_new_vertex,
        profile=profile,
    )
    if face_choices is None:
        return

    construct_start = perf_counter() if profile is not None else 0.0
    new_path = _new_path(vertex, neighbor, sequence, arc_insertions)
    result_vertices = tuple(sorted(set(drawing.vertices) | ({vertex} if add_new_vertex else set())))
    result_crossings = drawing.crossings + new_crossings
    result_paths = [new_path]
    for path in drawing.paths:
        if (path[0], path[-1]) not in crossed_edges:
            result_paths.append(path)
            continue
        result_paths.append(tuple(insert_crossings(path, shared.path_arcs[path], arc_insertions)))
    if profile is not None:
        profile["path_construction_seconds"] = profile.get("path_construction_seconds", 0.0) + (perf_counter() - construct_start)

    # Every face combination for this route shares these drawing components.
    unique_start = perf_counter() if profile is not None else 0.0
    route_faces = set(sequence[::2])
    point_keys = shared.point_keys.copy()
    if add_new_vertex:
        point_keys[vertex] = point_key(vertex)
    point_keys.update((point, point_key(point)) for point in new_crossings)
    result_crossings = tuple(sorted(result_crossings))
    result_paths = tuple(sorted(result_paths, key=lambda path: tuple(map(point_keys.__getitem__, path))))
    normalized_choices = []
    for face, choices in face_choices.items():
        if face not in route_faces:
            normalized_choices.append([(face,)])
        else:
            normalized_choices.append([
                tuple(face_canonical_cycle(tuple(cycle), point_keys=point_keys) for cycle in breakdown)
                for breakdown in choices
            ])
    # Face alternatives recur across combinations; retain their ordering keys
    # only for this route, not across drawings.
    face_keys = {}
    for choices in normalized_choices:
        for breakdown in choices:
            for face in breakdown:
                if face not in face_keys:
                    face_keys[face] = tuple(map(point_keys.__getitem__, face))
    unique_seconds = perf_counter() - unique_start if profile is not None else 0.0
    product_counts = [len(choices) for choices in normalized_choices]
    product_bound = 1
    for count in product_counts:
        product_bound *= count
    product_start = perf_counter() if profile is not None else 0.0
    face_products = product(*normalized_choices)
    product_seconds = perf_counter() - product_start if profile is not None else 0.0
    output_count = 0
    for face_breakdowns in face_products:
        unique_start = perf_counter() if profile is not None else 0.0
        faces = tuple(sorted(
            {face for breakdown in face_breakdowns for face in breakdown},
            key=face_keys.__getitem__,
        ))
        candidate = Drawing(
            result_vertices, result_crossings, result_paths, faces,
        )
        if profile is not None:
            unique_seconds += perf_counter() - unique_start
            active_seconds += perf_counter() - active_start
        yield candidate
        if profile is not None:
            active_start = perf_counter()
        output_count += 1
    if profile is not None:
        profile["cartesian_product_seconds"] = profile.get("cartesian_product_seconds", 0.0) + product_seconds
        profile["unique_presentation_seconds"] = profile.get("unique_presentation_seconds", 0.0) + unique_seconds
        profile["cartesian_product_bound"] = profile.get("cartesian_product_bound", 0) + product_bound
        profile["cartesian_product_outputs"] = profile.get("cartesian_product_outputs", 0) + output_count
        profile["max_cartesian_product_bound"] = max(profile.get("max_cartesian_product_bound", 0), product_bound)
        profile["make_extended_output_drawings"] = profile.get("make_extended_output_drawings", 0) + output_count
        active_seconds += perf_counter() - active_start
        profile["make_extended_seconds"] = profile.get("make_extended_seconds", 0.0) + active_seconds


def enumerate_sequences(
    drawing: Drawing,
    vertex: int,
    neighbor: int,
    *,
    add_new_vertex: bool,
    indices: DrawingIndices | None = None,
):
    shared = indices if indices is not None else drawing_indices(drawing)
    arc_edge, arc_faces = shared.arc_edge, shared.arc_faces

    # These choices depend only on the input drawing, not on a route history.
    # Keep one entry per face-arc occurrence so repeated arcs retain their
    # original traversal order.
    edge_bits: dict[tuple[int, int], int] = {}
    for edge in arc_edge.values():
        if edge not in edge_bits:
            edge_bits[edge] = 1 << len(edge_bits)
    route_options: dict[Face, tuple[tuple[Arc, Face, int], ...]] = {}
    for face in drawing.faces:
        options = []
        for arc in shared.face_arcs[face]:
            edge = arc_edge[arc]
            if vertex in edge or neighbor in edge:
                continue
            next_face = next(
                (candidate for candidate in arc_faces.get(arc, []) if candidate != face),
                None,
            )
            if next_face is not None:
                options.append((arc, next_face, edge_bits[edge]))
        route_options[face] = tuple(options)

    for face in drawing.faces:
        if not add_new_vertex and vertex not in face:
            continue
        stack = [(face, 0, (face,))]
        if neighbor in face:
            yield (face,)
        while stack:
            current_face, used_edges, current = stack.pop()
            for arc, next_face, edge_bit in route_options[current_face]:
                if used_edges & edge_bit:
                    continue
                next_sequence = current + (arc, next_face)
                if neighbor in next_face:
                    yield next_sequence
                stack.append((next_face, used_edges | edge_bit, next_sequence))


def add_vertex_edge(drawing: Drawing, vertex: int, neighbor: int) -> list[Drawing]:
    results = []
    indices = drawing_indices(drawing)
    for sequence in enumerate_sequences(drawing, vertex, neighbor, add_new_vertex=True, indices=indices):
        results.extend(
            make_extended_drawings(
                drawing, vertex, neighbor, sequence, add_new_vertex=True, indices=indices
            )
        )
    return results


def add_edge(drawing: Drawing, vertex: int, neighbor: int) -> list[Drawing]:
    results = []
    indices = drawing_indices(drawing)
    for sequence in enumerate_sequences(drawing, vertex, neighbor, add_new_vertex=False, indices=indices):
        results.extend(
            make_extended_drawings(
                drawing, vertex, neighbor, sequence, add_new_vertex=False, indices=indices
            )
        )
    return results


def apply_step(drawing: Drawing, step: BuildStep) -> list[Drawing]:
    u, v = step.edge
    if step.kind == "add_vertex_edge":
        vertex = u if u not in drawing.vertices else v
        neighbor = v if vertex == u else u
        return add_vertex_edge(drawing, vertex, neighbor)
    if step.kind == "add_edge":
        return add_edge(drawing, u, v)
    raise ValueError(f"unknown build step kind: {step.kind!r}")


def apply_step_profiled(
    drawing: Drawing,
    step: BuildStep,
    *,
    profile_operations: bool = True,
) -> tuple[list[Drawing], dict]:
    candidates, profile = iter_step_profiled(drawing, step, profile_operations=profile_operations)
    return list(candidates), profile


def _step_route_parameters(drawing: Drawing, step: BuildStep) -> tuple[int, int, bool]:
    u, v = step.edge
    if step.kind == "add_vertex_edge":
        vertex = u if u not in drawing.vertices else v
        neighbor = v if vertex == u else u
        return vertex, neighbor, True
    if step.kind == "add_edge":
        return u, v, False
    raise ValueError(f"unknown build step kind: {step.kind!r}")


def iter_step_profiled(
    drawing: Drawing,
    step: BuildStep,
    *,
    profile_operations: bool = True,
):
    vertex, neighbor, add_new_vertex = _step_route_parameters(drawing, step)

    profile = {
        "kind": step.kind,
        "edge": step.edge,
        "profile_operations": profile_operations,
        "input_vertices": len(drawing.vertices),
        "input_edges": len(drawing.paths),
        "input_crossings": len(drawing.crossings),
        "input_faces": len(drawing.faces),
    }
    setup_start = perf_counter()
    indices = drawing_indices(drawing)
    setup_seconds = perf_counter() - setup_start
    if profile_operations:
        profile["drawing_indices_seconds"] = setup_seconds

    def iterator():
        make_seconds = 0.0
        sequence_count = 0
        max_sequence_length = 0
        enumerate_seconds = 0.0
        output_count = 0
        active_seconds = setup_seconds
        active_start = perf_counter()
        enum_start = perf_counter() if profile_operations else 0.0
        for sequence in enumerate_sequences(drawing, vertex, neighbor, add_new_vertex=add_new_vertex, indices=indices):
            if profile_operations:
                enumerate_seconds += perf_counter() - enum_start
            sequence_count += 1
            max_sequence_length = max(max_sequence_length, len(sequence))
            make_start = perf_counter() if profile_operations else 0.0
            for candidate in iter_extended_drawings(
                    drawing,
                    vertex,
                    neighbor,
                    sequence,
                    add_new_vertex=add_new_vertex,
                    indices=indices,
                    profile=profile if profile_operations else None,
            ):
                output_count += 1
                if profile_operations:
                    make_seconds += perf_counter() - make_start
                active_seconds += perf_counter() - active_start
                yield candidate
                active_start = perf_counter()
                if profile_operations:
                    make_start = active_start
            if profile_operations:
                make_seconds += perf_counter() - make_start
                enum_start = perf_counter()
        if profile_operations:
            profile["enumerate_sequences_seconds"] = enumerate_seconds
        profile["sequence_count"] = sequence_count
        profile["max_sequence_length"] = max_sequence_length
        if profile_operations:
            profile["make_extended_total_seconds"] = make_seconds
        profile["output_count"] = output_count
        active_seconds += perf_counter() - active_start
        profile["total_seconds"] = active_seconds

    return iterator(), profile


def iter_final_route_crossings_profiled(
    drawing: Drawing,
    step: BuildStep,
    *,
    profile_operations: bool = True,
):
    """Yield parent-plus-route crossings for feasible final routes only.

    The route's crossing set is independent of which compatible face choices
    are selected.  The compatibility helper is still required because some
    enumerated dual trails cannot be realized by splitting the visited faces.
    """
    vertex, neighbor, add_new_vertex = _step_route_parameters(drawing, step)
    profile = {
        "kind": step.kind,
        "edge": step.edge,
        "output_kind": "feasible_routes",
        "profile_operations": profile_operations,
        "input_vertices": len(drawing.vertices),
        "input_edges": len(drawing.paths),
        "input_crossings": len(drawing.crossings),
        "input_faces": len(drawing.faces),
    }
    setup_start = perf_counter()
    indices = drawing_indices(drawing)
    setup_seconds = perf_counter() - setup_start
    if profile_operations:
        profile["drawing_indices_seconds"] = setup_seconds

    def iterator():
        route_count = 0
        feasible_route_count = 0
        max_sequence_length = 0
        enumerate_seconds = 0.0
        route_preparation_seconds = 0.0
        active_seconds = setup_seconds
        active_start = perf_counter()
        enum_start = perf_counter() if profile_operations else 0.0
        for sequence in enumerate_sequences(
            drawing,
            vertex,
            neighbor,
            add_new_vertex=add_new_vertex,
            indices=indices,
        ):
            if profile_operations:
                enumerate_seconds += perf_counter() - enum_start
            route_count += 1
            max_sequence_length = max(max_sequence_length, len(sequence))
            route_start = perf_counter() if profile_operations else 0.0
            prepared = _prepare_route(vertex, neighbor, sequence, indices)
            if profile_operations:
                route_preparation_seconds += perf_counter() - route_start
            compatible_choices = _compatible_face_choices(
                drawing,
                sequence,
                indices,
                prepared.arc_insertions,
                vertex=vertex,
                neighbor=neighbor,
                add_new_vertex=add_new_vertex,
                profile=profile if profile_operations else None,
            )
            if compatible_choices is None:
                if profile_operations:
                    enum_start = perf_counter()
                continue
            feasible_route_count += 1
            active_seconds += perf_counter() - active_start
            yield drawing.crossings + prepared.new_crossings
            active_start = perf_counter()
            if profile_operations:
                enum_start = perf_counter()
        if profile_operations:
            enumerate_seconds += perf_counter() - enum_start
            profile["enumerate_sequences_seconds"] = enumerate_seconds
            profile["route_preparation_seconds"] = route_preparation_seconds
        profile["sequence_count"] = route_count
        profile["route_count"] = route_count
        profile["feasible_route_count"] = feasible_route_count
        profile["infeasible_route_count"] = route_count - feasible_route_count
        profile["max_sequence_length"] = max_sequence_length
        profile["flag_route_emissions"] = feasible_route_count
        active_seconds += perf_counter() - active_start
        profile["total_seconds"] = active_seconds

    return iterator(), profile
