from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

import bench_results
from bench_common import QueryMeta, ResolvedDatasetRun


class BenchResultsTests(unittest.TestCase):
    def test_write_summary_csv_uses_total_median_repetition_for_complete_success(self) -> None:
        query = QueryMeta(
            dataset="job",
            query_id="q1",
            query_path="job/q1.sql",
            join_size=4,
        )
        spec = ResolvedDatasetRun(
            dataset="job",
            db="bench_job",
            variants=("dp",),
        )
        summary_acc = {
            ("job", "q1", "dp"): [
                {
                    "rep": 1,
                    "status": "ok",
                    "planning_ms": 90.0,
                    "execution_ms": 10.0,
                    "total_ms": 100.0,
                    "plan_total_cost": 300.0,
                },
                {
                    "rep": 2,
                    "status": "ok",
                    "planning_ms": 1.0,
                    "execution_ms": 19.0,
                    "total_ms": 20.0,
                    "plan_total_cost": 100.0,
                },
                {
                    "rep": 3,
                    "status": "ok",
                    "planning_ms": 4.0,
                    "execution_ms": 6.0,
                    "total_ms": 10.0,
                    "plan_total_cost": 200.0,
                },
            ]
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.csv"
            with patch.object(bench_results, "select_queries", Mock(return_value=[query])):
                bench_results.write_summary_csv(
                    summary_path,
                    resolved_runs=[spec],
                    summary_acc=summary_acc,
                    measured_reps=3,
                )

            with summary_path.open(newline="") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["planning_ms_median"], "1.000")
        self.assertEqual(rows[0]["execution_ms_median"], "19.000")
        self.assertEqual(rows[0]["total_ms_median"], "20.000")
        self.assertEqual(rows[0]["plan_total_cost_median"], "100.000")
        self.assertEqual(rows[0]["ok_reps"], "3")
        self.assertEqual(rows[0]["timeout_reps"], "0")
        self.assertEqual(rows[0]["error_reps"], "0")

    def test_write_summary_csv_omits_metrics_for_partial_success(self) -> None:
        query = QueryMeta(
            dataset="job",
            query_id="q1",
            query_path="job/q1.sql",
            join_size=4,
        )
        spec = ResolvedDatasetRun(
            dataset="job",
            db="bench_job",
            variants=("dp",),
        )
        summary_acc = {
            ("job", "q1", "dp"): [
                {
                    "rep": 1,
                    "status": "ok",
                    "planning_ms": 1.0,
                    "execution_ms": 10.0,
                    "total_ms": 11.0,
                    "plan_total_cost": 100.0,
                },
                {
                    "rep": 2,
                    "status": "ok",
                    "planning_ms": 3.0,
                    "execution_ms": 30.0,
                    "total_ms": 33.0,
                    "plan_total_cost": 300.0,
                },
                {"status": "timeout"},
            ]
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.csv"
            with patch.object(bench_results, "select_queries", Mock(return_value=[query])):
                bench_results.write_summary_csv(
                    summary_path,
                    resolved_runs=[spec],
                    summary_acc=summary_acc,
                    measured_reps=3,
                )

            with summary_path.open(newline="") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["planning_ms_median"], "")
        self.assertEqual(rows[0]["execution_ms_median"], "")
        self.assertEqual(rows[0]["total_ms_median"], "")
        self.assertEqual(rows[0]["plan_total_cost_median"], "")
        self.assertEqual(rows[0]["ok_reps"], "2")
        self.assertEqual(rows[0]["timeout_reps"], "1")
        self.assertEqual(rows[0]["error_reps"], "0")


if __name__ == "__main__":
    unittest.main()
