from __future__ import annotations

import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_review_tables import write_review_tables


class BenchReviewTablesTests(unittest.TestCase):
    def make_run_dir(self, tmpdir: str) -> Path:
        run_dir = Path(tmpdir) / "outputs" / "run1"
        run_dir.mkdir(parents=True)
        (run_dir / "run.json").write_text(
            json.dumps(
                {
                    "run_id": "run1",
                    "scenario": "main",
                    "variants": [
                        {"name": "dp", "label": "Dynamic Programming"},
                        {"name": "my_algo", "label": "My Algorithm"},
                    ],
                    "datasets": [{"dataset": "job"}],
                }
            )
            + "\n"
        )
        (run_dir / "summary.csv").write_text(
            "\n".join(
                [
                    "dataset,query_id,join_size,variant,planning_ms_median,execution_ms_median,total_ms_median,plan_total_cost_median,ok_reps,err_reps",
                    "job,10a,7,dp,2.000,100.000,102.000,1000.000,3,0",
                    "job,10a,7,my_algo,1.000,80.000,81.000,900.000,3,0",
                    "job,2a,12,dp,4.000,200.000,204.000,1000.000,3,0",
                    "job,2a,12,my_algo,8.000,500.000,508.000,900.000,3,0",
                    "",
                ]
            )
        )
        return run_dir

    def test_renders_workbook_and_metric_csv_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = self.make_run_dir(tmpdir)

            paths = write_review_tables(
                run_dir=run_dir,
                datasets=[],
            )

            self.assertEqual(len(paths), 3)
            workbook_path = run_dir / "review_tables" / "review_job.xlsx"
            execution_csv = (run_dir / "review_tables" / "review_job_execution.csv").read_text()
            planning_csv = (run_dir / "review_tables" / "review_job_planning.csv").read_text()
            with zipfile.ZipFile(workbook_path) as zf:
                workbook_xml = zf.read("xl/workbook.xml").decode()
                styles_xml = zf.read("xl/styles.xml").decode()

        self.assertIn("job execution", workbook_xml)
        self.assertIn("job planning", workbook_xml)
        self.assertIn("B7E1CD", styles_xml)
        self.assertIn("F4CCCC", styles_xml)
        self.assertIn("2a", execution_csv)
        self.assertLess(execution_csv.index("2a"), execution_csv.index("10a"))
        self.assertIn("SUM", planning_csv)
        self.assertIn("my_algo_to_dp", planning_csv)


if __name__ == "__main__":
    unittest.main()
