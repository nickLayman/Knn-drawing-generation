"""Bounded checks for complete-bipartite census helpers."""

import unittest

from graph_drawings.complete_bipartite_census import (
    complete_bipartite_graph,
    drawing_crossing_orders,
    flag_invariant_bucket,
    has_side_swapping_strong_automorphism,
)
from graph_drawings.canonical import canonical_json_for_perms
from graph_drawings.drawing import Drawing, normalize_crossing


class CompleteBipartiteCensusTests(unittest.TestCase):
    def test_graph_layout(self):
        vertices, edges, colors = complete_bipartite_graph(3, 4)
        self.assertEqual(vertices, tuple(range(7)))
        self.assertEqual(len(edges), 12)
        self.assertEqual(colors, (1, 1, 1, 2, 2, 2, 2))

    def test_crossing_orders_are_oriented_from_first_side_to_second(self):
        crossing = normalize_crossing(((0, 4), (1, 3)))
        paths = []
        for a in range(3):
            for b in range(3, 6):
                path = (a, b)
                if (a, b) == (0, 4):
                    path = (0, crossing, 4)
                elif (a, b) == (1, 3):
                    path = (3, crossing, 1)
                paths.append(path)
        drawing = Drawing(tuple(range(6)), (crossing,), tuple(paths), tuple())
        orders = drawing_crossing_orders(drawing, (0, 1, 2), (3, 4, 5))
        self.assertEqual(orders[1], (3,))
        self.assertEqual(orders[3], (1,))
        self.assertTrue(has_side_swapping_strong_automorphism(drawing, (0, 1, 2), (3, 4, 5)))

    def test_both_k22_base_drawings_are_fixed_by_a_side_swap(self):
        from graph_drawings.build_plan import base_drawings_for_cycle

        for drawing in base_drawings_for_cycle((0, 2, 1, 3)):
            self.assertTrue(has_side_swapping_strong_automorphism(drawing, (0, 1), (2, 3)))

    def test_flag_bucket_respects_color_permutations(self):
        original = "6 0 1 1 1 2 2 2 2 1423 1526"
        relabeled_within_colors = "6 0 1 1 1 2 2 2 2 2513 2614"
        swapped_colors = "6 0 2 2 2 1 1 1 2 1423 1526"
        self.assertEqual(
            flag_invariant_bucket(original, colors_blind=False),
            flag_invariant_bucket(relabeled_within_colors, colors_blind=False),
        )
        self.assertEqual(
            flag_invariant_bucket(original, colors_blind=True),
            flag_invariant_bucket(swapped_colors, colors_blind=True),
        )

    def test_crossing_order_key_omits_planarization_face_presentation(self):
        paths = ((0, 3), (0, 4), (0, 5), (1, 3), (1, 4), (1, 5), (2, 3), (2, 4), (2, 5))
        first = Drawing(tuple(range(6)), tuple(), paths, ((0, 3, 1, 4, 0),))
        second = Drawing(tuple(range(6)), tuple(), paths, ((0, 3, 2, 4, 0),))
        identity = (tuple(range(6)),)
        self.assertNotEqual(
            canonical_json_for_perms(first, tuple(range(6)), identity),
            canonical_json_for_perms(second, tuple(range(6)), identity),
        )
        self.assertEqual(
            canonical_json_for_perms(
                first,
                tuple(range(6)),
                identity,
                key_strategy="crossing_order_paths",
            ),
            canonical_json_for_perms(
                second,
                tuple(range(6)),
                identity,
                key_strategy="crossing_order_paths",
            ),
        )


if __name__ == "__main__":
    unittest.main()
