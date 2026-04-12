from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

import bench_exec
import bench_run
from bench_common import QueryMeta, ResolvedDatasetRun, Scenario, Variant


FIXED_NOW = datetime(2026, 4, 11, 9, 0, 0, tzinfo=timezone.utc)


def write_summary_csv_stub(summary_path: Path, **_: object) -> None:
    summary_path.write_text(
        (
            "run_id,scenario,dataset,db,variant,query_id,query_label,query_path,join_size,"
            "planning_ms_median,execution_ms_median,total_ms_median,plan_total_cost_median,ok_reps,err_reps\n"
        )
    )


def write_public_reports_stub(
    *,
    markdown_path: Path,
    json_path: Path,
    **_: object,
) -> dict[str, object]:
    markdown_path.write_text("# Public Benchmark Report\n")
    json_path.write_text("{}\n")
    return {}


class RunScenarioTests(unittest.TestCase):
    def make_scenario(self) -> Scenario:
        return Scenario(
            name="main",
            description="test scenario",
            default_variants=("dp",),
            reps=1,
            statement_timeout_ms=1000,
            stabilize="none",
            variant_order_mode="fixed",
            session_gucs=(),
            datasets=(),
        )

    def make_variant_registry(self) -> dict[str, Variant]:
        return {"dp": Variant(name="dp", label="DP", session_gucs=())}

    def make_resolved_runs(self) -> list[ResolvedDatasetRun]:
        return [
            ResolvedDatasetRun(
                dataset="job",
                db="bench_job",
                min_join=None,
                max_join=None,
                max_queries=1,
                variants=("dp",),
            )
        ]

    def make_query(self) -> QueryMeta:
        return QueryMeta(
            dataset="job",
            query_id="q1",
            query_path="job/q1.sql",
            query_label="Q1",
            join_size=4,
        )

    def make_query_with_id(self, query_id: str) -> QueryMeta:
        return QueryMeta(
            dataset="job",
            query_id=query_id,
            query_path=f"job/{query_id}.sql",
            query_label=query_id.upper(),
            join_size=4,
        )

    def patch_run_environment(
        self,
        stack: ExitStack,
        outputs_dir: Path,
        run_one_side_effect: object,
        *,
        queries: list[QueryMeta] | None = None,
    ) -> Mock:
        stack.enter_context(patch.object(bench_run, "OUTPUTS_DIR", outputs_dir))
        stack.enter_context(patch.object(bench_run, "utc_now", Mock(return_value=FIXED_NOW)))
        stack.enter_context(patch.object(bench_run, "ensure_databases_reachable", Mock()))
        stack.enter_context(patch.object(bench_run, "validate_required_gucs", Mock()))
        stack.enter_context(patch.object(bench_run, "resolved_variant_session_gucs", Mock(return_value=())))
        stack.enter_context(patch.object(bench_run, "stabilize_db", Mock()))
        stack.enter_context(patch.object(bench_run, "select_queries", Mock(return_value=queries or [self.make_query()])))
        stack.enter_context(patch.object(bench_run, "load_sql_for_query", Mock(return_value="SELECT 1")))
        stack.enter_context(patch.object(bench_run, "build_statement", Mock(side_effect=lambda _dataset, sql: sql)))
        stack.enter_context(patch.object(bench_run, "write_summary_csv", Mock(side_effect=write_summary_csv_stub)))
        stack.enter_context(patch.object(bench_run, "write_public_reports", Mock(side_effect=write_public_reports_stub)))
        run_one_mock = Mock(side_effect=run_one_side_effect)
        stack.enter_context(patch.object(bench_run, "run_one", run_one_mock))
        return run_one_mock

    def only_run_dir(self, outputs_dir: Path) -> Path:
        run_dirs = [path for path in outputs_dir.iterdir() if path.is_dir()]
        self.assertEqual(len(run_dirs), 1)
        return run_dirs[0]

    def read_raw_rows(self, run_dir: Path) -> list[dict[str, str]]:
        with (run_dir / "raw.csv").open(newline="") as f:
            return list(csv.DictReader(f))

    def read_run_context(self, run_dir: Path) -> dict[str, object]:
        return json.loads((run_dir / "run.json").read_text())

    def test_warmup_timeout_is_skipped_even_with_fail_on_error(self) -> None:
        metrics = bench_exec.RunMetrics(planning_ms=1.0, execution_ms=2.0, total_ms=3.0, plan_total_cost=4.0)
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            self.patch_run_environment(
                stack,
                outputs_dir,
                [
                    bench_exec.StatementTimeoutError("ERROR: canceling statement due to statement timeout"),
                    metrics,
                ],
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                reps=1,
                statement_timeout_ms=1000,
                stabilize="none",
                variant_order_mode="fixed",
                warmup_runs=1,
                skip_measured_after_warmup_timeout=False,
                resume_run_id=None,
                tag="",
                fail_on_error=True,
            )

            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["status"], "ok")
            self.assertEqual(run_context["warmup_failures"][0]["category"], "statement_timeout")
            self.assertNotIn("termination", run_context)

    def test_measured_timeout_does_not_trigger_fail_on_error_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            self.patch_run_environment(
                stack,
                outputs_dir,
                bench_exec.StatementTimeoutError("ERROR: canceling statement due to statement timeout"),
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                reps=1,
                statement_timeout_ms=1000,
                stabilize="none",
                variant_order_mode="fixed",
                warmup_runs=0,
                skip_measured_after_warmup_timeout=False,
                resume_run_id=None,
                tag="",
                fail_on_error=True,
            )

            run_dir = self.only_run_dir(outputs_dir)
            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["status"], "timeout")

    def test_warmup_error_writes_outputs_before_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            self.patch_run_environment(
                stack,
                outputs_dir,
                RuntimeError("ERROR: planner blew up"),
            )

            with self.assertRaises(SystemExit) as ctx:
                bench_run.run_scenario(
                    self.make_scenario(),
                    self.make_variant_registry(),
                    ("dp",),
                    self.make_resolved_runs(),
                    conn=None,
                    reps=1,
                    statement_timeout_ms=1000,
                    stabilize="none",
                    variant_order_mode="fixed",
                    warmup_runs=1,
                    skip_measured_after_warmup_timeout=False,
                    resume_run_id=None,
                    tag="",
                    fail_on_error=True,
                )

            self.assertEqual(ctx.exception.code, 1)
            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            self.assertEqual(run_context["termination"]["phase"], "warmup")
            self.assertEqual(run_context["termination"]["category"], "error")
            self.assertTrue((run_dir / "summary.csv").is_file())
            self.assertTrue((run_dir / "public_report.md").is_file())

    def test_measured_non_timeout_error_still_exits_non_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            self.patch_run_environment(
                stack,
                outputs_dir,
                RuntimeError("ERROR: planner blew up"),
            )

            with self.assertRaises(SystemExit) as ctx:
                bench_run.run_scenario(
                    self.make_scenario(),
                    self.make_variant_registry(),
                    ("dp",),
                    self.make_resolved_runs(),
                    conn=None,
                    reps=1,
                    statement_timeout_ms=1000,
                    stabilize="none",
                    variant_order_mode="fixed",
                    warmup_runs=0,
                    skip_measured_after_warmup_timeout=False,
                    resume_run_id=None,
                    tag="",
                    fail_on_error=True,
                )

            self.assertEqual(ctx.exception.code, 1)
            run_dir = self.only_run_dir(outputs_dir)
            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["status"], "error")

    def test_skip_measured_after_warmup_timeout_records_timeout_rows_without_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            run_one_mock = self.patch_run_environment(
                stack,
                outputs_dir,
                bench_exec.StatementTimeoutError("ERROR: canceling statement due to statement timeout"),
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                reps=2,
                statement_timeout_ms=1000,
                stabilize="none",
                variant_order_mode="fixed",
                warmup_runs=1,
                skip_measured_after_warmup_timeout=True,
                resume_run_id=None,
                tag="",
                fail_on_error=True,
            )

            self.assertEqual(run_one_mock.call_count, 1)
            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(len(raw_rows), 2)
            self.assertTrue(all(row["status"] == "timeout" for row in raw_rows))
            self.assertTrue(
                all(
                    row["error"].startswith("skipped measured run after warmup timeout:")
                    for row in raw_rows
                )
            )
            self.assertTrue(run_context["protocol"]["skip_measured_after_warmup_timeout"])

    def test_query_group_warmup_runs_before_same_query_measured_reps(self) -> None:
        q1 = self.make_query_with_id("q1")
        q2 = self.make_query_with_id("q2")
        metrics = bench_exec.RunMetrics(planning_ms=1.0, execution_ms=2.0, total_ms=3.0, plan_total_cost=4.0)

        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            run_one_mock = self.patch_run_environment(
                stack,
                outputs_dir,
                [metrics] * 6,
                queries=[q1, q2],
            )
            stack.enter_context(
                patch.object(bench_run, "load_sql_for_query", Mock(side_effect=lambda q: q.query_id))
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                reps=2,
                statement_timeout_ms=1000,
                stabilize="none",
                variant_order_mode="fixed",
                warmup_runs=1,
                skip_measured_after_warmup_timeout=True,
                resume_run_id=None,
                tag="",
                fail_on_error=True,
            )

            self.assertEqual(run_one_mock.call_count, 6)
            query_order = [call.args[3] for call in run_one_mock.call_args_list]
            self.assertEqual(
                query_order,
                ["q1", "q1", "q1", "q2", "q2", "q2"],
            )

            raw_rows = self.read_raw_rows(self.only_run_dir(outputs_dir))
            self.assertEqual(
                [(row["query_id"], row["rep"]) for row in raw_rows],
                [("q1", "1"), ("q1", "2"), ("q2", "1"), ("q2", "2")],
            )

    def test_resume_run_id_continues_from_next_measured_group_boundary(self) -> None:
        q1 = self.make_query_with_id("q1")
        q2 = self.make_query_with_id("q2")
        with tempfile.TemporaryDirectory() as tmpdir:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            with ExitStack() as stack:
                run_one_mock = self.patch_run_environment(
                    stack,
                    outputs_dir,
                    [
                        bench_exec.RunMetrics(planning_ms=1.0, execution_ms=2.0, total_ms=3.0, plan_total_cost=4.0),
                        KeyboardInterrupt(),
                    ],
                    queries=[q1, q2],
                )
                with self.assertRaises(KeyboardInterrupt):
                    bench_run.run_scenario(
                        self.make_scenario(),
                        self.make_variant_registry(),
                        ("dp",),
                        self.make_resolved_runs(),
                        conn=None,
                        reps=1,
                        statement_timeout_ms=1000,
                        stabilize="none",
                        variant_order_mode="fixed",
                        warmup_runs=0,
                        skip_measured_after_warmup_timeout=True,
                        resume_run_id=None,
                        tag="resume",
                        fail_on_error=True,
                    )
                self.assertEqual(run_one_mock.call_count, 2)

            run_dir = self.only_run_dir(outputs_dir)
            self.assertEqual([row["query_id"] for row in self.read_raw_rows(run_dir)], ["q1"])

            with ExitStack() as stack:
                run_one_mock = self.patch_run_environment(
                    stack,
                    outputs_dir,
                    lambda *args, **kwargs: bench_exec.RunMetrics(
                        planning_ms=5.0,
                        execution_ms=6.0,
                        total_ms=11.0,
                        plan_total_cost=7.0,
                    ),
                    queries=[q1, q2],
                )
                bench_run.run_scenario(
                    self.make_scenario(),
                    self.make_variant_registry(),
                    ("dp",),
                    self.make_resolved_runs(),
                    conn=None,
                    reps=1,
                    statement_timeout_ms=1000,
                    stabilize="none",
                    variant_order_mode="fixed",
                    warmup_runs=0,
                    skip_measured_after_warmup_timeout=True,
                    resume_run_id=run_dir.name,
                    tag="resume",
                    fail_on_error=True,
                )
                self.assertEqual(run_one_mock.call_count, 1)

            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual([row["query_id"] for row in raw_rows], ["q1", "q2"])
            run_context = self.read_run_context(run_dir)
            self.assertTrue(run_context["progress"]["completed"])

    def test_resume_run_id_restores_warmup_timeout_skip_state(self) -> None:
        q1 = self.make_query_with_id("q1")
        q2 = self.make_query_with_id("q2")
        with tempfile.TemporaryDirectory() as tmpdir:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            with ExitStack() as stack:
                run_one_mock = self.patch_run_environment(
                    stack,
                    outputs_dir,
                    [
                        bench_exec.StatementTimeoutError("ERROR: canceling statement due to statement timeout"),
                        KeyboardInterrupt(),
                    ],
                    queries=[q1, q2],
                )
                with self.assertRaises(KeyboardInterrupt):
                    bench_run.run_scenario(
                        self.make_scenario(),
                        self.make_variant_registry(),
                        ("dp",),
                        self.make_resolved_runs(),
                        conn=None,
                        reps=1,
                        statement_timeout_ms=1000,
                        stabilize="none",
                        variant_order_mode="fixed",
                        warmup_runs=1,
                        skip_measured_after_warmup_timeout=True,
                        resume_run_id=None,
                        tag="resume-warmup",
                        fail_on_error=True,
                    )
                self.assertEqual(run_one_mock.call_count, 2)

            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            self.assertEqual(
                run_context["progress"]["completed_warmup_groups"],
                [{"warmup_pass": 1, "dataset": "job", "query_id": "q1"}],
            )

            with ExitStack() as stack:
                run_one_mock = self.patch_run_environment(
                    stack,
                    outputs_dir,
                    lambda *args, **kwargs: bench_exec.RunMetrics(
                        planning_ms=9.0,
                        execution_ms=10.0,
                        total_ms=19.0,
                        plan_total_cost=11.0,
                    ),
                    queries=[q1, q2],
                )
                bench_run.run_scenario(
                    self.make_scenario(),
                    self.make_variant_registry(),
                    ("dp",),
                    self.make_resolved_runs(),
                    conn=None,
                    reps=1,
                    statement_timeout_ms=1000,
                    stabilize="none",
                    variant_order_mode="fixed",
                    warmup_runs=1,
                    skip_measured_after_warmup_timeout=True,
                    resume_run_id=run_dir.name,
                    tag="resume-warmup",
                    fail_on_error=True,
                )
                self.assertEqual(run_one_mock.call_count, 2)

            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(len(raw_rows), 2)
            self.assertEqual(raw_rows[0]["query_id"], "q1")
            self.assertEqual(raw_rows[0]["status"], "timeout")
            self.assertTrue(raw_rows[0]["error"].startswith("skipped measured run after warmup timeout:"))
            self.assertEqual(raw_rows[1]["query_id"], "q2")


if __name__ == "__main__":
    unittest.main()
