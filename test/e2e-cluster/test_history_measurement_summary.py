"""Accounting regressions; these fixtures are not performance measurements."""
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class HistorySummaryTest(unittest.TestCase):
    def test_final_heap_observation_and_unavailable_counters(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'sample.jsonl'
            rows = [
                {'phase': 'whole_cluster_start', 'wall_seconds': 1, 'resources': {
                    '1': {'sampled_peak_heap_bytes': 100, 'heap_bytes': 140,
                          'process_read_bytes': 0},
                    '2': {'sampled_peak_heap_bytes': 120, 'heap_bytes': 80}}},
                {'phase': 'whole_cluster_start', 'wall_seconds': 2, 'resources': {
                    '1': {'sampled_peak_heap_bytes': 0}}},
                {'phase': 'interrupted_start_then_restart', 'wall_seconds': 3,
                 'resources_last_process_only': {'1': {'heap_bytes': 90}}},
            ]
            source.write_text('\n'.join(json.dumps(row) for row in rows), encoding='utf-8')
            subprocess.run([sys.executable, str(Path(__file__).with_name('summarize_history_measure.py')),
                            str(source)], check=True, capture_output=True)
            with source.with_suffix('.summary.csv').open(encoding='utf-8', newline='') as file:
                summary = list(csv.DictReader(file))
            self.assertEqual(summary[0]['max_node_sampled_peak_heap_bytes'], '140')
            self.assertEqual(summary[0]['process_read_bytes'], '0')
            self.assertEqual(summary[0]['process_write_bytes'], '')
            self.assertEqual(summary[1]['max_node_sampled_peak_heap_bytes'], '')
            self.assertEqual(summary[2]['max_node_sampled_peak_heap_bytes'], '90')
            self.assertEqual(summary[2]['resource_scope'], 'last process only')


if __name__ == '__main__':
    unittest.main()
