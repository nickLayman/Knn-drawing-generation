"""Exact parity with the pre-optimization constructor from Git, on tiny inputs.

Run in the source checkout: the reference is pinned to the pilot source commit.
"""
import subprocess
import sys
import types
import unittest

from graph_drawings import extensions
from graph_drawings.automorphism import context_to_record, make_canonical_context
from graph_drawings.build_plan import derive_build_blocks, derive_build_plan
from graph_drawings.compact import context_from_run_config, decode_drawing, encode_drawing, encode_normalized_drawing
from graph_drawings.drawing import Drawing, base_k22_drawings, normalize_edge, unique_presentation
from graph_drawings.flag_formats import crossing_pair_flag, four_graph_flag


class NormalizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = subprocess.check_output([
            'git', 'show', 'be571b8:src/graph_drawings/extensions.py'
        ], text=True)
        cls.reference = types.ModuleType('graph_drawings._normalization_reference')
        sys.modules[cls.reference.__name__] = cls.reference
        exec(compile(source, 'reference_extensions_be571b8.py', 'exec'), cls.reference.__dict__)
        source = subprocess.check_output([
            'git', 'show', 'be571b8:src/graph_drawings/compact.py'
        ], text=True)
        cls.reference_compact = types.ModuleType('graph_drawings._compact_reference')
        sys.modules[cls.reference_compact.__name__] = cls.reference_compact
        exec(compile(source, 'reference_compact_be571b8.py', 'exec'), cls.reference_compact.__dict__)

    def test_ordered_extensions_and_flags_match_reference(self):
        for seed in base_k22_drawings():
            first = self.reference.add_vertex_edge(seed, 4, 1)
            cases = [(seed, 4, 1, True)]
            for pendant in first:
                cases.append((pendant, 4, 3, False))
                # A few K3,2 parents also exercise longer face walks.
                for parent in self.reference.add_edge(pendant, 4, 3)[:3]:
                    cases.append((parent, 5, 0, True))
            for drawing, u, v, new_vertex in cases:
                old = self.reference.add_vertex_edge if new_vertex else self.reference.add_edge
                new = extensions.add_vertex_edge if new_vertex else extensions.add_edge
                expected = old(drawing, u, v)
                actual = new(drawing, u, v)
                self.assertEqual(actual, expected)
                for candidate in actual:
                    self.assertEqual(candidate, unique_presentation(candidate))
                for export in (crossing_pair_flag, four_graph_flag):
                    self.assertEqual([export(d) for d in actual], [export(d) for d in expected])

    def test_compact_bytes_and_boundary_normalization_match_reference(self):
        edges = [(a, b) for a in (0, 2, 4) for b in (1, 3)]
        cycle, steps = derive_build_plan(edges)
        blocks = derive_build_blocks(cycle, steps)
        canonical = make_canonical_context(tuple(range(5)), {normalize_edge(e) for e in edges}, cycle, blocks)
        context = context_from_run_config(dict(graph_vertices=list(range(5)), graph_edges=edges,
                                              canonical_context=context_to_record(canonical)))
        for seed in base_k22_drawings():
            cases = [(0, seed)]
            for pendant in self.reference.add_vertex_edge(seed, 4, 1):
                cases.extend((1, child) for child in self.reference.add_edge(pendant, 4, 3))
            for stage, drawing in cases:
                expected = self.reference_compact.encode_drawing(drawing, stage, context)
                self.assertEqual(encode_normalized_drawing(drawing, stage, context), expected)
                self.assertEqual(decode_drawing(expected, context), drawing)

                def reverse_point(point):
                    return point if isinstance(point, int) else (point[1][::-1], point[0][::-1])

                raw = Drawing(drawing.vertices[::-1], tuple(reverse_point(c) for c in drawing.crossings[::-1]),
                              tuple(tuple(reverse_point(p) for p in path[::-1]) for path in drawing.paths[::-1]),
                              tuple(tuple(reverse_point(p) for p in face[::-1]) for face in drawing.faces[::-1]))
                self.assertEqual(encode_drawing(raw, stage, context), expected)
        context.vertex_to_id.clear()
        with self.assertRaises(KeyError):
            encode_normalized_drawing(base_k22_drawings()[0], 0, context)


if __name__ == '__main__':
    unittest.main()
