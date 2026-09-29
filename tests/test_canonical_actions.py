"""Canonical keys agree before/after removing actions on unbuilt vertices."""
import unittest

from graph_drawings.automorphism import make_canonical_context, preserves_edges
from graph_drawings.build_plan import derive_build_blocks, derive_build_plan, base_drawings_for_cycle
from graph_drawings.canonical import canonical_json_for_perms, drawing_keys
from graph_drawings.drawing import normalize_edge, relabel_drawing
from graph_drawings.worker import apply_block


class CanonicalActionTests(unittest.TestCase):
    def test_stage_keys_equal_full_permutation_reference(self):
        # Only construct seeds and their K3,2 extensions, not all K4,3 drawings.
        edges = [(a, b) for a in range(4) for b in range(4, 7)]
        cycle, steps = derive_build_plan(edges)
        blocks = derive_build_blocks(cycle, steps)
        ctx = make_canonical_context(tuple(range(7)), {normalize_edge(e) for e in edges}, cycle, blocks)
        seeds = base_drawings_for_cycle(cycle)
        children = [child for seed in seeds for child in apply_block(seed, blocks[0])[0]]
        for stage, drawings in zip(ctx.stages, (seeds, children)):
            original = tuple(p for p in ctx.full_automorphisms
                             if preserves_edges(p, ctx.vertex_order, set(stage.edges)))
            self.assertLessEqual(len(stage.report_perms), len(original))
            if stage.stage_index == 0:
                self.assertLess(len(stage.report_perms), len(original))
            indices = [ctx.vertex_order.index(v) for v in stage.vertices]
            self.assertEqual(len(stage.report_perms), len({tuple(p[i] for i in indices) for p in original}))
            for drawing in drawings:
                expected = canonical_json_for_perms(drawing, ctx.vertex_order, original)
                self.assertEqual(expected, canonical_json_for_perms(drawing, ctx.vertex_order, stage.report_perms))
                mapping = dict(zip(ctx.vertex_order, original[-1]))
                renamed = relabel_drawing(drawing, mapping)
                self.assertEqual(drawing_keys(stage.stage_index, drawing, ctx),
                                 drawing_keys(stage.stage_index, renamed, ctx))
        self.assertEqual(ctx.stages[-1].report_perms, ctx.full_automorphisms)


if __name__ == '__main__':
    unittest.main()
