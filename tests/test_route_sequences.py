import unittest

from graph_drawings.drawing import base_k22_drawings, face_arcs
from graph_drawings.extensions import (
    add_edge,
    add_vertex_edge,
    drawing_indices,
    enumerate_sequences,
    make_extended_drawings,
)


def reference_enumerate_sequences(
    drawing,
    vertex,
    neighbor,
    *,
    add_new_vertex,
    indices=None,
):
    shared = indices if indices is not None else drawing_indices(drawing)
    arc_edge, arc_faces = shared.arc_edge, shared.arc_faces
    for face in drawing.faces:
        if not add_new_vertex and vertex not in face:
            continue
        stack = [(face,)]
        if neighbor in face:
            yield (face,)
        while stack:
            current = stack.pop()
            used_arcs = current[1::2]
            current_face = current[-1]
            for arc in face_arcs(current_face):
                suitable = True
                for used_arc in used_arcs:
                    if arc == used_arc or arc_edge[arc] == arc_edge[used_arc]:
                        suitable = False
                        break
                if suitable and (vertex in arc_edge[arc] or neighbor in arc_edge[arc]):
                    suitable = False
                if not suitable:
                    continue
                incident_faces = arc_faces.get(arc, [])
                next_faces = [candidate for candidate in incident_faces if candidate != current_face]
                if not next_faces:
                    continue
                next_sequence = current + (arc, next_faces[0])
                if neighbor in next_faces[0]:
                    yield next_sequence
                stack.append(next_sequence)


def reference_extensions(drawing, vertex, neighbor, *, add_new_vertex):
    indices = drawing_indices(drawing)
    return [
        candidate
        for sequence in reference_enumerate_sequences(
            drawing,
            vertex,
            neighbor,
            add_new_vertex=add_new_vertex,
            indices=indices,
        )
        for candidate in make_extended_drawings(
            drawing,
            vertex,
            neighbor,
            sequence,
            add_new_vertex=add_new_vertex,
            indices=indices,
        )
    ]


class RouteSequenceTest(unittest.TestCase):
    def test_sequence_lists_match_reference_on_initial_seeds(self):
        for seed in base_k22_drawings():
            for vertex, neighbor in ((4, 1), (4, 0)):
                indices = drawing_indices(seed)
                actual = list(
                    enumerate_sequences(
                        seed,
                        vertex,
                        neighbor,
                        add_new_vertex=True,
                        indices=indices,
                    )
                )
                expected = list(
                    reference_enumerate_sequences(
                        seed,
                        vertex,
                        neighbor,
                        add_new_vertex=True,
                        indices=indices,
                    )
                )
                self.assertEqual(actual, expected)

    def test_k23_candidates_match_reference_for_selected_edges(self):
        for seed in base_k22_drawings():
            k23_candidates = add_vertex_edge(seed, 4, 1)
            self.assertTrue(k23_candidates)
            for drawing in k23_candidates:
                for vertex, neighbor, add_new_vertex in (
                    (4, 3, False),
                    (5, 0, True),
                    (5, 1, True),
                ):
                    indices = drawing_indices(drawing)
                    actual = list(
                        enumerate_sequences(
                            drawing,
                            vertex,
                            neighbor,
                            add_new_vertex=add_new_vertex,
                            indices=indices,
                        )
                    )
                    expected = list(
                        reference_enumerate_sequences(
                            drawing,
                            vertex,
                            neighbor,
                            add_new_vertex=add_new_vertex,
                            indices=indices,
                        )
                    )
                    self.assertEqual(actual, expected)

                    actual_extensions = (
                        add_vertex_edge(drawing, vertex, neighbor)
                        if add_new_vertex
                        else add_edge(drawing, vertex, neighbor)
                    )
                    expected_extensions = reference_extensions(
                        drawing,
                        vertex,
                        neighbor,
                        add_new_vertex=add_new_vertex,
                    )
                    self.assertEqual(actual_extensions, expected_extensions)


if __name__ == "__main__":
    unittest.main()
