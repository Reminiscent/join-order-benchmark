from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_review_tables import ReviewTableCell, ratio_css_class, write_review_tables, xlsx_format_key


HAS_XLSXWRITER = importlib.util.find_spec("xlsxwriter") is not None


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
                    "datasets": [{"dataset": "job"}, {"dataset": "job_complex"}],
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
                    "job_complex,1a,9,dp,3.000,300.000,303.000,1000.000,3,0",
                    "job_complex,1a,9,my_algo,6.000,450.000,456.000,900.000,3,0",
                    "",
                ]
            )
        )
        return run_dir

    @unittest.skipUnless(HAS_XLSXWRITER, "XlsxWriter is optional and only needed for reviewer XLSX tables")
    def test_renders_workbook_and_metric_csv_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = self.make_run_dir(tmpdir)

            paths = write_review_tables(
                run_dir=run_dir,
                datasets=[],
            )

            self.assertEqual(len(paths), 3)
            workbook_path = run_dir / "review_tables" / "review.xlsx"
            execution_csv = (run_dir / "review_tables" / "review_execution.csv").read_text()
            planning_csv = (run_dir / "review_tables" / "review_planning.csv").read_text()
            with zipfile.ZipFile(workbook_path) as zf:
                workbook_xml = zf.read("xl/workbook.xml").decode()
                styles_xml = zf.read("xl/styles.xml").decode()

        self.assertIn("execution", workbook_xml)
        self.assertIn("planning", workbook_xml)
        self.assertIn("6AA84F", styles_xml)
        self.assertIn("CC0000", styles_xml)
        self.assertIn("dataset,query,join_size", execution_csv)
        self.assertIn("2a", execution_csv)
        self.assertIn("job_complex,1a", execution_csv)
        self.assertLess(execution_csv.index("2a"), execution_csv.index("10a"))
        self.assertIn("SUM", planning_csv)
        self.assertIn("my_algo_to_dp", planning_csv)

    def test_ratio_color_thresholds_match_documented_scale(self) -> None:
        cases = [
            (None, "missing"),
            (0.49, "ratio ratio-fast-strong"),
            (0.50, "ratio ratio-fast"),
            (0.79, "ratio ratio-fast"),
            (0.80, "ratio ratio-neutral"),
            (1.19, "ratio ratio-neutral"),
            (1.20, "ratio ratio-slow"),
            (1.99, "ratio ratio-slow"),
            (2.00, "ratio ratio-slower"),
            (9.99, "ratio ratio-slower"),
            (10.00, "ratio ratio-worst"),
        ]
        for value, expected in cases:
            self.assertEqual(ratio_css_class(value), expected)

    def test_xlsx_format_key_keeps_slow_and_slower_buckets_distinct(self) -> None:
        slow = ReviewTableCell(text="1.5", raw=1.5, css_class="ratio ratio-slow")
        slower = ReviewTableCell(text="3", raw=3.0, css_class="ratio ratio-slower")

        self.assertEqual(xlsx_format_key(slow), "ratio_slow")
        self.assertEqual(xlsx_format_key(slower), "ratio_slower")


if __name__ == "__main__":
    unittest.main()
