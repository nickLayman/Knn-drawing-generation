"""Bounded label-table checks; these construct no graph drawings."""
import unittest

from graph_drawings.drawing import normalize_crossing
from graph_drawings.flag_formats import crossing_digits, crossing_label_table, four_graph_digits


class FlagLabelTests(unittest.TestCase):
    def test_all_labels_match_existing_formatters(self):
        for left, right, count in ((5, 4, 120), (6, 3, 90)):
            edges = [(a, b) for a in range(left) for b in range(left, left + right)]
            for four_graph, formatter in ((False, crossing_digits), (True, four_graph_digits)):
                table = crossing_label_table(edges, four_graph=four_graph)
                self.assertEqual(len(table), count)
                for crossing, label in table.items():
                    self.assertEqual(label, formatter(crossing, 1))
                if four_graph:
                    self.assertEqual(len(set(table.values())), count // 2)

    def test_reversed_edges_and_label_offsets(self):
        edges = [(0, 1), (3, 2), (1, 0)]
        crossing = normalize_crossing(((0, 1), (2, 3)))
        for four_graph in (False, True):
            self.assertEqual(crossing_label_table(edges, four_graph=four_graph, vertex_label_offset=0),
                             {crossing: '0123'})
        with self.assertRaises(ValueError):
            crossing_label_table([(0, 1), (8, 9)], four_graph=False)


if __name__ == '__main__':
    unittest.main()
