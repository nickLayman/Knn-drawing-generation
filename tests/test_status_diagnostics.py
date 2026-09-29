"""Checks for durable task-boundary generation diagnostics."""

import json
import sqlite3
import unittest

from graph_drawings.status import init_schema, worker_io_by_stage


class StatusDiagnosticsTests(unittest.TestCase):
    def test_worker_diagnostics_are_aggregated_by_stage(self):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        init_schema(db)
        payloads = [
            {
                "stage_index": 1,
                "candidates_seen": 10,
                "candidates_submitted": 7,
                "elapsed_seconds": 4.0,
                "profile": {
                    "worker_cpu_seconds": 3.5,
                    "worker_max_rss_kib": 100,
                    "block": {
                        "kind": "vertex_block",
                        "vertex": 6,
                        "total_seconds": 3.0,
                        "steps": [
                            {
                                "block_step_index": 0,
                                "frontier_in": 2,
                                "raw_outputs": 9,
                                "frontier_out": 7,
                                "exact_dedupe_removed": 2,
                            }
                        ],
                    },
                },
            },
            {
                "stage_index": 1,
                "candidates_seen": 12,
                "candidates_submitted": 8,
                "elapsed_seconds": 3.0,
                "profile": {
                    "worker_cpu_seconds": 2.5,
                    "worker_max_rss_kib": 120,
                    "block": {
                        "kind": "vertex_block",
                        "vertex": 6,
                        "total_seconds": 2.0,
                        "steps": [
                            {
                                "block_step_index": 0,
                                "frontier_in": 3,
                                "raw_outputs": 11,
                                "frontier_out": 8,
                                "exact_dedupe_removed": 3,
                            }
                        ],
                    },
                },
            },
        ]
        for timestamp, payload in zip((10.0, 12.0), payloads):
            db.execute(
                "INSERT INTO events(timestamp, event_type, payload_json) VALUES (?, 'job_result', ?)",
                (timestamp, json.dumps(payload)),
            )
        rows = worker_io_by_stage(db)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["tasks"], 2)
        self.assertEqual(row["candidates_seen"], 22)
        self.assertEqual(row["candidates_submitted"], 15)
        self.assertEqual(row["worker_cpu_seconds_sum"], 6.0)
        self.assertEqual(row["worker_max_rss_kib"], 120)
        self.assertEqual(row["wall_span_seconds"], 6.0)
        self.assertEqual(row["block_wall_seconds_sum"], 5.0)
        self.assertEqual(
            row["block_step_totals"],
            [
                {
                    "block_step_index": 0,
                    "tasks": 2,
                    "frontier_in": 5,
                    "raw_outputs": 20,
                    "frontier_out": 15,
                    "exact_dedupe_removed": 5,
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
