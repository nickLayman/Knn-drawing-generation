import unittest
from unittest.mock import patch

from graph_drawings.drawing import base_k22_drawings
from graph_drawings.extensions import BuildStep, apply_step_profiled, iter_step_profiled


class _Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        value = self.value
        self.value += 1.0
        return value

    def advance(self, seconds):
        self.value += seconds


class ExtensionTimingTest(unittest.TestCase):
    def test_profile_operations_switch_preserves_completions(self):
        base = base_k22_drawings()[0]
        pendant_step = BuildStep("add_vertex_edge", (4, 1))
        pendant, pendant_profile = apply_step_profiled(base, pendant_step)
        self.assertTrue(pendant)

        cases = [
            (base, pendant_step),
            (pendant[0], BuildStep("add_edge", (4, 3))),
        ]
        for drawing, step in cases:
            detailed, detailed_profile = apply_step_profiled(drawing, step)
            coarse, coarse_profile = apply_step_profiled(
                drawing, step, profile_operations=False
            )
            self.assertEqual(coarse, detailed)
            self.assertEqual(coarse_profile["sequence_count"], detailed_profile["sequence_count"])
            self.assertEqual(coarse_profile["output_count"], len(coarse))
            self.assertFalse(
                any(
                    key.endswith("_seconds") and key != "total_seconds"
                    for key in coarse_profile
                )
            )

    def test_detailed_timers_exclude_consumer_delay(self):
        drawing = base_k22_drawings()[0]
        step = BuildStep("add_vertex_edge", (4, 1))

        def run(consumer_delay):
            clock = _Clock()
            with patch("graph_drawings.extensions.perf_counter", side_effect=clock):
                candidates, profile = iter_step_profiled(drawing, step)
                first = next(candidates)
                clock.advance(consumer_delay)
                self.assertEqual(list(candidates), [])
            return first, profile

        without_delay, profile_without_delay = run(0.0)
        with_delay, profile_with_delay = run(1000.0)
        self.assertEqual(without_delay, with_delay)
        self.assertEqual(profile_without_delay, profile_with_delay)


if __name__ == "__main__":
    unittest.main()
