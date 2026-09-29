from itertools import product

from graph_drawings.drawing import face_canonical_cycle


def _reference(face, point_keys=None):
    cycle = tuple(face[:-1]) if len(face) > 1 and face[0] == face[-1] else tuple(face)
    if not cycle:
        return tuple()

    def key(point):
        if point_keys is not None:
            return point_keys[point]
        return (0, point) if isinstance(point, int) else (1, point)

    candidates = []
    for sequence in (cycle, cycle[::-1]):
        for index, point in enumerate(sequence):
            if key(point) == max(map(key, cycle)):
                rotated = sequence[index:] + sequence[:index]
                candidates.append((tuple(map(key, rotated)), rotated))
    chosen = max(candidates, key=lambda candidate: candidate[0])[1]
    return chosen + (chosen[0],)


def test_face_canonical_cycle_matches_reference_on_repeated_open_and_closed_cycles():
    crossing = ((3, 2), (1, 0))
    points = (0, 1, 2, crossing)
    faces = [(), (0,), (2, 0, 2, 1), (2, 0, 2, 1, 2), (crossing, 1, 0, crossing)]
    faces.extend(tuple(row) for row in product(points[:3], repeat=4))
    for face in faces:
        assert face_canonical_cycle(face) == _reference(face)


def test_face_canonical_cycle_uses_supplied_point_keys():
    face = (0, 1, 2, 0)
    point_keys = {0: (1, 0), 1: (1, 2), 2: (1, 1)}
    assert face_canonical_cycle(face, point_keys=point_keys) == _reference(face, point_keys)
