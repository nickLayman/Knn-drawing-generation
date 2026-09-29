"""Complete-bipartite helpers for the drawing census pipeline."""

from __future__ import annotations

from itertools import permutations
from pathlib import Path
from typing import Iterable, Sequence

from .drawing import Drawing, normalize_edge


def complete_bipartite_graph(m: int, n: int) -> tuple[tuple[int, ...], tuple[tuple[int, int], ...], tuple[int, ...]]:
    if m < 2 or n < 2:
        raise ValueError("complete-bipartite census requires m,n >= 2")
    a_vertices = tuple(range(m))
    b_vertices = tuple(range(m, m + n))
    edges = tuple((a, b) for a in a_vertices for b in b_vertices)
    colors = (1,) * m + (2,) * n
    return a_vertices + b_vertices, edges, colors


def bipartition(m: int, n: int) -> tuple[tuple[int, ...], tuple[int, ...]]:
    if m < 2 or n < 2:
        raise ValueError("complete-bipartite census requires m,n >= 2")
    return tuple(range(m)), tuple(range(m, m + n))


def drawing_crossing_orders(
    drawing: Drawing,
    a_vertices: Sequence[int],
    b_vertices: Sequence[int],
) -> tuple[tuple[int, ...], ...]:
    """Return crossing partners along every edge, oriented from A to B."""
    a_set = set(a_vertices)
    b_set = set(b_vertices)
    if a_set & b_set:
        raise ValueError("bipartition classes overlap")
    edge_to_id = {
        normalize_edge((a, b)): i * len(b_vertices) + j
        for i, a in enumerate(a_vertices)
        for j, b in enumerate(b_vertices)
    }
    orders: list[tuple[int, ...] | None] = [None] * len(edge_to_id)
    for path in drawing.paths:
        first = int(path[0])
        last = int(path[-1])
        if first in a_set and last in b_set:
            points = path[1:-1]
        elif first in b_set and last in a_set:
            points = path[-2:0:-1]
        else:
            raise ValueError(f"path endpoints do not cross the bipartition: {path!r}")
        edge = normalize_edge((first, last))
        if edge not in edge_to_id:
            raise ValueError(f"path is not an edge of the complete bipartite graph: {path!r}")
        partners = []
        for point in points:
            if isinstance(point, int):
                raise ValueError(f"internal path point is not a crossing: {path!r}")
            first_edge = normalize_edge(point[0])
            second_edge = normalize_edge(point[1])
            if edge == first_edge:
                partner = second_edge
            elif edge == second_edge:
                partner = first_edge
            else:
                raise ValueError(f"crossing {point!r} is not incident with path edge {edge!r}")
            try:
                partners.append(edge_to_id[partner])
            except KeyError as error:
                raise ValueError(f"crossing partner is not bipartite: {partner!r}") from error
        edge_id = edge_to_id[edge]
        if orders[edge_id] is not None:
            raise ValueError(f"duplicate path for edge {edge!r}")
        orders[edge_id] = tuple(partners)
    if any(order is None for order in orders):
        raise ValueError("drawing is missing one or more complete-bipartite edge paths")
    return tuple(order for order in orders if order is not None)


def _compatible_permutations(
    source_profiles: Sequence[tuple[int, ...]],
    target_profiles: Sequence[tuple[int, ...]],
) -> tuple[tuple[int, ...], ...]:
    return tuple(
        perm
        for perm in permutations(range(len(source_profiles)))
        if all(source_profiles[index] == target_profiles[image] for index, image in enumerate(perm))
    )


def side_swap_analysis(
    drawing: Drawing,
    a_vertices: Sequence[int],
    b_vertices: Sequence[int],
) -> tuple[bool, bool]:
    """Return ``(profile_candidate, fixed)`` for a square drawing.

    For K(n,n), crossing orders along the oriented graph edges determine the
    strong drawing class once n >= 3.  A side swap reverses each oriented path.
    """
    size = len(a_vertices)
    if size != len(b_vertices):
        return False, False
    if size == 2:
        # The two hard-coded K(2,2) sphere drawings (zero or one crossing) are
        # each fixed by an interchange of the two bipartition classes.
        return True, True
    if size < 2:
        raise ValueError("side-swap testing requires equal parts of size at least 2")
    orders = drawing_crossing_orders(drawing, a_vertices, b_vertices)
    lengths = tuple(len(order) for order in orders)
    a_profiles = tuple(
        tuple(sorted(lengths[a * size + b] for b in range(size)))
        for a in range(size)
    )
    b_profiles = tuple(
        tuple(sorted(lengths[a * size + b] for a in range(size)))
        for b in range(size)
    )
    a_to_b_options = _compatible_permutations(a_profiles, b_profiles)
    if not a_to_b_options:
        return False, False
    b_to_a_options = _compatible_permutations(b_profiles, a_profiles)
    for a_to_b in a_to_b_options:
        for b_to_a in b_to_a_options:
            edge_map = tuple(
                b_to_a[b] * size + a_to_b[a]
                for a in range(size)
                for b in range(size)
            )
            if all(
                tuple(edge_map[partner] for partner in reversed(orders[old_edge]))
                == orders[edge_map[old_edge]]
                for old_edge in range(size * size)
            ):
                return True, True
    return True, False


def has_side_swapping_strong_automorphism(
    drawing: Drawing,
    a_vertices: Sequence[int],
    b_vertices: Sequence[int],
) -> bool:
    """Test whether a square bipartite drawing is fixed by swapping its sides."""
    return side_swap_analysis(drawing, a_vertices, b_vertices)[1]


def flag_invariant_bucket(flag: str, *, colors_blind: bool, prefix_hex: int = 2) -> str:
    """Return a safe, inexpensive bucket for an exact flag.cpp reduction."""
    import hashlib

    fields = flag.split()
    if len(fields) < 4:
        raise ValueError("flag line is too short")
    vertex_count = int(fields[0])
    if fields[1] != "0":
        raise ValueError("census flags must be unrooted")
    colors = tuple(int(value) for value in fields[2 : 2 + vertex_count])
    count_position = 2 + vertex_count
    object_count = int(fields[count_position])
    tokens = fields[count_position + 1 :]
    if len(tokens) != object_count:
        raise ValueError("flag object count does not match its tokens")
    degrees = [0] * vertex_count
    for token in tokens:
        if len(token) != 4 or not token.isdigit():
            raise ValueError(f"malformed four-vertex token: {token!r}")
        for label in set(int(character) for character in token):
            if label < 1 or label > vertex_count:
                raise ValueError(f"flag token label is out of range: {token!r}")
            degrees[label - 1] += 1
    color_profiles = [
        (color, tuple(sorted(degrees[index] for index, value in enumerate(colors) if value == color)))
        for color in sorted(set(colors))
    ]
    if colors_blind:
        profile = tuple(sorted(values for _, values in color_profiles))
    else:
        profile = tuple(color_profiles)
    payload = repr((object_count, profile)).encode("ascii")
    return hashlib.sha256(payload).hexdigest()[:prefix_hex]


def line_count(path: Path) -> int:
    with path.open("rb") as source:
        return sum(block.count(b"\n") for block in iter(lambda: source.read(1024 * 1024), b""))


def sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_color_orientations(colors: Iterable[int], m: int, n: int) -> tuple[int, ...]:
    values = tuple(int(value) for value in colors)
    expected = (1,) * m + (2,) * n
    if values != expected:
        raise ValueError(f"expected vertex colors {expected!r}, got {values!r}")
    return values
