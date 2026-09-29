"""Exact tiny-set checks for streamed final flags; no large enumeration."""
import base64
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from graph_drawings import config
from graph_drawings.automorphism import build_block_to_record, context_to_record, make_canonical_context
from graph_drawings.build_plan import derive_build_blocks, derive_build_plan, base_drawings_for_cycle
from graph_drawings.compact import compress_db_blob, encode_drawing
from graph_drawings.drawing import base_k22_drawings, normalize_edge
from graph_drawings.flag_formats import (
    crossing_digits,
    crossing_pair_flag,
    four_graph_digits,
    four_graph_flag,
)
from graph_drawings.extensions import (
    BuildStep,
    add_vertex_edge,
    drawing_indices,
    enumerate_sequences,
    iter_final_route_crossings_profiled,
    iter_step_profiled,
)
from graph_drawings.shards import iter_compound_flag_shard
from graph_drawings.worker import (ShardBuffer, apply_block, load_run_context,
                                  make_checkpoint_writer, process_job)
from graph_drawings.reducer import reduce_bucket


class StreamingFlagTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.roots = patch.object(config, 'RUNS_ROOT', Path(self.tmp.name))
        self.roots.start()
        self.addCleanup(self.roots.stop)
        self.edges = [(u, v) for u in (0, 2) for v in (1, 3, 4)]
        self.colors = (1, 2, 1, 2, 2)
        cycle, steps = derive_build_plan(self.edges)
        self.blocks = derive_build_blocks(cycle, steps)
        self.parents = base_drawings_for_cycle(cycle)
        self.canonical = make_canonical_context(tuple(range(5)),
            {normalize_edge(e) for e in self.edges}, cycle, self.blocks)

    def context(self, name, mode, compound=True, batch=1000, profiling=True):
        path = config.RUNS_ROOT / name
        path.mkdir()
        (path / 'run_config.json').write_text(json.dumps({
            'graph_vertices': list(range(5)), 'graph_edges': self.edges,
            'build_blocks': [build_block_to_record(b) for b in self.blocks],
            'canonical_context': context_to_record(self.canonical),
            'total_steps': len(self.blocks), 'final_output_mode': mode,
            'export_vertex_colors': self.colors, 'compound_shards': compound,
            'checkpoint_shard_batch_records': batch, 'worker_shard_flush_bytes': 256,
            'profile_operations': profiling,
        }))
        return load_run_context(name)

    def test_both_flag_sets_match_full_drawing_projection(self):
        drawings = [child for parent in self.parents
                    for child in apply_block(parent, self.blocks[0])[0]]
        for mode, export in [('crossing_pair_flags', crossing_pair_flag),
                             ('four_graph_flags', four_graph_flag)]:
            expected = {export(d, vertex_colors=self.colors) for d in drawings}
            for compound in (False, True):
                for profiling in (False, True):
                    with self.subTest(mode=mode, compound=compound, profiling=profiling):
                        name = f'{mode}-{compound}-{profiling}'
                        ctx = self.context(name, mode, compound, profiling=profiling)
                        buffer = ShardBuffer(ctx, 'test') if compound else None
                        shards = []
                        for i, parent in enumerate(self.parents):
                            payload = compress_db_blob(encode_drawing(parent, 0, ctx.compact_context))
                            job = {'stage_index': 0, 'job_key': str(i),
                                   'compact_blob_b64': base64.b64encode(payload).decode()}
                            with patch('graph_drawings.worker.apply_block', side_effect=AssertionError('materialized flag path')), \
                                 patch('graph_drawings.flag_formats.crossing_digits', side_effect=AssertionError('reformatted crossing')), \
                                 patch('graph_drawings.flag_formats.four_graph_digits', side_effect=AssertionError('reformatted four-set')):
                                result = process_job(job, ctx, buffer)
                                final_profile = result.profile["block"]["steps"][-1]
                                self.assertEqual(result.profile["candidate_kind"], "route")
                                self.assertIn("checkpoint_routes_seen", result.profile)
                                self.assertNotIn("checkpoint_candidates_before_local_reduce", result.profile)
                                self.assertEqual(final_profile["count_kind"], "routes")
                                self.assertNotIn("raw_outputs", final_profile)
                            shards.extend(result.shards)
                            if not profiling:
                                self.assertEqual(result.profile['timings'], {})
                                self.assertFalse(any(key.endswith('_seconds')
                                    for step in result.profile['block']['steps'] for key in step))
                        if buffer:
                            shards.extend(buffer.flush())
                        actual = {r.flag for shard in shards for r in
                                  iter_compound_flag_shard(Path(ctx.run_dir) / shard['shard_path'])}
                        self.assertEqual(actual, expected)
                        # Authoritative reduction agrees, for either shard layout.
                        reduced = set()
                        for bucket in {Path(ctx.run_dir, s['shard_path']).parent for s in shards}:
                            summary = reduce_bucket(name, 1, bucket)
                            reduced.update(r.flag for r in iter_compound_flag_shard(
                                Path(ctx.run_dir) / summary.reduced_shard_path))
                        self.assertEqual(reduced, expected)

    def test_batch_dedup_and_record_limit(self):
        ctx = self.context('dedup', 'four_graph_flags', compound=False, batch=2)
        emit, flush, state = make_checkpoint_writer(ctx, 0, {'job_key': 'repeat'})
        by_flag = {four_graph_flag(d, vertex_colors=self.colors): d
                   for parent in self.parents for d in apply_block(parent, self.blocks[0])[0]}
        first, second = list(by_flag.values())[:2]
        for _ in range(100):
            emit(crossings=first.crossings)
        self.assertEqual(state['buffered_records'], 1)
        self.assertEqual(len(state['seen_flags']), 1)
        emit(crossings=second.crossings)
        self.assertEqual(state['flush_count'], 1)
        self.assertEqual(state['buffered_records'], 0)
        self.assertEqual(state['seen_flags'], set())
        flush()
        self.assertEqual(state['total_written_records'], 2)
        self.assertEqual(state['max_batch_records'], 2)

    def test_full_drawing_dfs_still_matches_bfs(self):
        ctx = self.context('drawings', 'full_drawings', compound=False, batch=2)
        expected = set()
        shards = []
        for i, parent in enumerate(self.parents):
            expected.update(apply_block(parent, self.blocks[0])[0])
            job = {'stage_index': 0, 'job_key': str(i), 'compact_blob_b64':
                   base64.b64encode(compress_db_blob(encode_drawing(parent, 0, ctx.compact_context))).decode()}
            shards.extend(process_job(job, ctx).shards)
        from graph_drawings.shards import iter_compound_raw_shard
        from graph_drawings.compact import decode_drawing
        actual = {decode_drawing(r.compact_payload, ctx.compact_context)
                  for s in shards for r in iter_compound_raw_shard(
                      Path(ctx.run_dir) / s['shard_path'], s['bucket_hash'])}
        self.assertEqual(actual, expected)

    def test_final_route_flags_match_full_projection_and_retain_compatibility(self):
        route_parents = [
            drawing
            for seed in base_k22_drawings()
            for drawing in add_vertex_edge(seed, 4, 1)
        ]

        def direct_flag(parent, crossings, four_graph):
            labeler = (four_graph_digits if four_graph else crossing_digits)
            labels = sorted({labeler(crossing, 1) for crossing in crossings})
            return " ".join([str(len(parent.vertices) + 1), "0", str(len(labels)), *labels])

        cases = [
            # One route has two compatible face arrangements; both must yield
            # one crossing-set flag.
            (route_parents[0], BuildStep("add_vertex_edge", (5, 1)), False),
            # This parent has repeated-face routes and infeasible dual trails.
            (route_parents[1], BuildStep("add_vertex_edge", (5, 4)), True),
        ]
        for parent, step, check_repeated in cases:
            old_iter, old_profile = iter_step_profiled(parent, step)
            old_drawings = list(old_iter)
            indices = drawing_indices(parent)
            sequences = list(
                enumerate_sequences(
                    parent,
                    5,
                    step.edge[1],
                    add_new_vertex=True,
                    indices=indices,
                )
            )
            if check_repeated:
                self.assertTrue(any(len(set(sequence[::2])) < len(sequence[::2]) for sequence in sequences))

            for four_graph in (False, True):
                with self.subTest(step=step.edge, four_graph=four_graph):
                    with patch("graph_drawings.extensions.Drawing", side_effect=AssertionError("final route constructed a drawing")), \
                         patch("graph_drawings.extensions.face_canonical_cycle", side_effect=AssertionError("final route canonicalized a face")):
                        new_iter, new_profile = iter_final_route_crossings_profiled(parent, step)
                        new_crossings = list(new_iter)
                    expected = {
                        crossing_pair_flag(drawing) if not four_graph else four_graph_flag(drawing)
                        for drawing in old_drawings
                    }
                    actual = {
                        direct_flag(parent, crossings, four_graph)
                        for crossings in new_crossings
                    }
                    self.assertEqual(actual, expected)
                    self.assertEqual(new_profile["route_count"], old_profile["sequence_count"])
                    self.assertLessEqual(new_profile["feasible_route_count"], len(old_drawings))
                    self.assertEqual(
                        new_profile["infeasible_route_count"],
                        new_profile["route_count"] - new_profile["feasible_route_count"],
                    )
            if step.edge == (5, 1):
                self.assertEqual(len(sequences), 1)
                self.assertEqual(len(old_drawings), 2)
                self.assertEqual(new_profile["feasible_route_count"], 1)
            else:
                self.assertEqual(new_profile["feasible_route_count"], len(old_drawings))
                self.assertGreater(new_profile["infeasible_route_count"], 0)


if __name__ == '__main__':
    unittest.main()
