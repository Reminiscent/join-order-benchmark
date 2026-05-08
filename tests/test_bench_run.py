from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stderr
from datetime import datetime, timezone
from io import StringIO
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
            "dataset,query_id,join_size,variant,"
            "planning_ms_median,execution_ms_median,total_ms_median,plan_total_cost_median,"
            "ok_reps,timeout_reps,error_reps\n"
        )
    )


class RunScenarioTests(unittest.TestCase):
    def make_scenario(self) -> Scenario:
        return Scenario(
            name="main",
            description="test scenario",
            default_variants=("dp",),
            datasets=(),
        )

    def make_run_session_gucs(self) -> tuple[tuple[str, object], ...]:
        return (("statement_timeout", 1000), ("work_mem", "1GB"))

    def make_variant_registry(self) -> dict[str, Variant]:
        return {"dp": Variant(name="dp", label="dp", session_gucs=())}

    def make_resolved_runs(self) -> list[ResolvedDatasetRun]:
        return [
            ResolvedDatasetRun(
                dataset="job",
                db="bench_job",
                variants=("dp",),
            )
        ]

    def make_query(self) -> QueryMeta:
        return QueryMeta(
            dataset="job",
            query_id="q1",
            query_path="job/q1.sql",
            join_size=4,
        )

    def make_query_with_id(self, query_id: str) -> QueryMeta:
        return QueryMeta(
            dataset="job",
            query_id=query_id,
            query_path=f"job/{query_id}.sql",
            join_size=4,
        )

    def patch_run_environment(
        self,
        stack: ExitStack,
        outputs_dir: Path,
        run_one_side_effect: object,
        *,
        queries: list[QueryMeta] | None = None,
        measured_reps: int = 1,
        warmup_runs: int = 0,
    ) -> tuple[Mock, Mock]:
        stack.enter_context(patch.object(bench_run, "OUTPUTS_DIR", outputs_dir))
        stack.enter_context(patch.object(bench_run, "utc_now", Mock(return_value=FIXED_NOW)))
        stack.enter_context(patch.object(bench_run, "MEASURED_REPS", measured_reps))
        stack.enter_context(patch.object(bench_run, "WARMUP_RUNS", warmup_runs))
        stack.enter_context(patch.object(bench_run, "ensure_databases_reachable", Mock()))
        stack.enter_context(patch.object(bench_run, "validate_session_gucs", Mock()))
        stabilize_mock = Mock()
        stack.enter_context(patch.object(bench_run, "stabilize_db", stabilize_mock))
        stack.enter_context(
            patch.object(
                bench_run,
                "select_queries",
                Mock(return_value=queries or [self.make_query()]),
            )
        )
        stack.enter_context(
            patch.object(bench_run, "load_sql_for_query", Mock(return_value="SELECT 1"))
        )
        stack.enter_context(
            patch.object(
                bench_run,
                "build_statement",
                Mock(side_effect=lambda _dataset, sql: sql),
            )
        )
        stack.enter_context(
            patch.object(
                bench_run,
                "write_summary_csv",
                Mock(side_effect=write_summary_csv_stub),
            )
        )
        run_one_mock = Mock(side_effect=run_one_side_effect)
        stack.enter_context(patch.object(bench_run, "run_one_statement", run_one_mock))
        return run_one_mock, stabilize_mock

    def only_run_dir(self, outputs_dir: Path) -> Path:
        run_dirs = [path for path in outputs_dir.iterdir() if path.is_dir()]
        self.assertEqual(len(run_dirs), 1)
        return run_dirs[0]

    def read_raw_rows(self, run_dir: Path) -> list[dict[str, str]]:
        with (run_dir / "raw.csv").open(newline="") as f:
            return list(csv.DictReader(f))

    def read_run_context(self, run_dir: Path) -> dict[str, object]:
        return json.loads((run_dir / "run.json").read_text())

    def test_warmup_timeout_is_skipped_without_treating_timeout_as_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            _, stabilize_mock = self.patch_run_environment(
                stack,
                outputs_dir,
                bench_exec.StatementTimeoutError(
                    "ERROR: canceling statement due to statement timeout"
                ),
                warmup_runs=1,
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                run_session_gucs=self.make_run_session_gucs(),
                tag="",
            )

            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["status"], "timeout")
            self.assertTrue(
                raw_rows[0]["error"].startswith(
                    "skipped measured run after warmup timeout:"
                )
            )
            self.assertEqual(run_context["warmup_failures"][0]["category"], "statement_timeout")
            self.assertNotIn("termination", run_context)
            stabilize_mock.assert_called_once_with("bench_job", None)

    def test_measured_timeout_does_not_abort_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            self.patch_run_environment(
                stack,
                outputs_dir,
                bench_exec.StatementTimeoutError(
                    "ERROR: canceling statement due to statement timeout"
                ),
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                run_session_gucs=self.make_run_session_gucs(),
                tag="",
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
                warmup_runs=1,
            )

            with self.assertRaises(SystemExit) as ctx:
                bench_run.run_scenario(
                    self.make_scenario(),
                    self.make_variant_registry(),
                    ("dp",),
                    self.make_resolved_runs(),
                    conn=None,
                    run_session_gucs=self.make_run_session_gucs(),
                    tag="",
                )

            self.assertEqual(ctx.exception.code, 1)
            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            self.assertEqual(run_context["termination"]["phase"], "warmup")
            self.assertEqual(run_context["termination"]["category"], "error")
            self.assertTrue((run_dir / "summary.csv").is_file())
            self.assertTrue((run_dir / "run.json").is_file())

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
                    run_session_gucs=self.make_run_session_gucs(),
                    tag="",
                )

            self.assertEqual(ctx.exception.code, 1)
            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(run_context["termination"]["phase"], "measured")
            self.assertEqual(run_context["termination"]["category"], "error")
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["status"], "error")

    def test_measured_non_timeout_error_stops_current_group(self) -> None:
        metrics = bench_exec.RunMetrics(
            planning_ms=1.0,
            execution_ms=2.0,
            total_ms=3.0,
            plan_total_cost=4.0,
        )
        variants = {
            "dp": Variant(name="dp", label="dp", session_gucs=()),
            "geqo": Variant(name="geqo", label="GEQO", session_gucs=()),
        }
        resolved_runs = [
            ResolvedDatasetRun(
                dataset="job",
                db="bench_job",
                variants=("dp", "geqo"),
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            run_one_mock, _ = self.patch_run_environment(
                stack,
                outputs_dir,
                [RuntimeError("ERROR: planner blew up"), metrics],
            )

            with self.assertRaises(SystemExit) as ctx:
                bench_run.run_scenario(
                    self.make_scenario(),
                    variants,
                    ("dp", "geqo"),
                    resolved_runs,
                    conn=None,
                    run_session_gucs=self.make_run_session_gucs(),
                    tag="",
                )

            self.assertEqual(ctx.exception.code, 1)
            self.assertEqual(run_one_mock.call_count, 1)
            run_dir = self.only_run_dir(outputs_dir)
            run_context = self.read_run_context(run_dir)
            raw_rows = self.read_raw_rows(run_dir)
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["variant"], "dp")
            self.assertEqual(raw_rows[0]["status"], "error")

    def test_warmup_timeout_records_measured_timeout_rows_without_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            run_one_mock, _ = self.patch_run_environment(
                stack,
                outputs_dir,
                bench_exec.StatementTimeoutError(
                    "ERROR: canceling statement due to statement timeout"
                ),
                measured_reps=2,
                warmup_runs=1,
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                run_session_gucs=self.make_run_session_gucs(),
                tag="",
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
            self.assertEqual(
                run_context["session_gucs"],
                [{"statement_timeout": 1000}, {"work_mem": "1GB"}],
            )
            self.assertEqual(
                run_context["protocol"],
                {
                    "measured_reps": 2,
                    "warmup_runs": 1,
                    "timing": "off",
                    "variant_order": "rotate_by_query_and_rep",
                    "stats_refresh": "before_run",
                },
            )

    def test_reuse_stats_skips_database_stabilization(self) -> None:
        metrics = bench_exec.RunMetrics(
            planning_ms=1.0,
            execution_ms=2.0,
            total_ms=3.0,
            plan_total_cost=4.0,
        )

        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            _, stabilize_mock = self.patch_run_environment(
                stack,
                outputs_dir,
                [metrics],
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                run_session_gucs=self.make_run_session_gucs(),
                tag="",
                reuse_stats=True,
            )

            stabilize_mock.assert_not_called()
            run_context = self.read_run_context(self.only_run_dir(outputs_dir))
            self.assertEqual(run_context["protocol"]["stats_refresh"], "reuse_existing")

    def test_query_selection_failure_does_not_refresh_stats(self) -> None:
        metrics = bench_exec.RunMetrics(
            planning_ms=1.0,
            execution_ms=2.0,
            total_ms=3.0,
            plan_total_cost=4.0,
        )

        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            _, stabilize_mock = self.patch_run_environment(
                stack,
                outputs_dir,
                [metrics],
            )
            stack.enter_context(
                patch.object(bench_run, "select_queries", Mock(side_effect=SystemExit(1)))
            )

            with self.assertRaises(SystemExit):
                bench_run.run_scenario(
                    self.make_scenario(),
                    self.make_variant_registry(),
                    ("dp",),
                    self.make_resolved_runs(),
                    conn=None,
                    run_session_gucs=self.make_run_session_gucs(),
                    tag="",
                )

            stabilize_mock.assert_not_called()

    def test_sql_load_failure_does_not_refresh_stats(self) -> None:
        metrics = bench_exec.RunMetrics(
            planning_ms=1.0,
            execution_ms=2.0,
            total_ms=3.0,
            plan_total_cost=4.0,
        )

        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            _, stabilize_mock = self.patch_run_environment(
                stack,
                outputs_dir,
                [metrics],
            )
            stack.enter_context(
                patch.object(bench_run, "load_sql_for_query", Mock(side_effect=SystemExit(1)))
            )

            with self.assertRaises(SystemExit):
                bench_run.run_scenario(
                    self.make_scenario(),
                    self.make_variant_registry(),
                    ("dp",),
                    self.make_resolved_runs(),
                    conn=None,
                    run_session_gucs=self.make_run_session_gucs(),
                    tag="",
                )

            stabilize_mock.assert_not_called()

    def test_guc_validation_only_sets_selected_variants(self) -> None:
        run_session_gucs = (("join_collapse_limit", 100),)
        variants = {
            "dp": Variant(name="dp", label="dp", session_gucs=(("geqo_threshold", 100),)),
            "unused": Variant(
                name="unused",
                label="Unused",
                session_gucs=(("missing_unused_guc", "on"),),
            ),
        }
        psql_mock = Mock(return_value=Mock(returncode=0, stdout="", stderr=""))

        with patch.object(bench_exec, "psql_sql_raw", psql_mock):
            bench_exec.validate_session_gucs("bench_db", None, run_session_gucs, variants, ("dp",))

        scripts = [call.args[1] for call in psql_mock.call_args_list]
        self.assertTrue(any("SET geqo_threshold = 100;" in script for script in scripts))
        self.assertFalse(any("missing_unused_guc" in script for script in scripts))

    def test_guc_validation_rejects_invalid_setting_values(self) -> None:
        run_session_gucs = (("work_mem", "not-a-size"),)
        variants = {"dp": Variant(name="dp", label="dp", session_gucs=())}

        with patch.object(
            bench_exec,
            "psql_sql_raw",
            Mock(return_value=Mock(returncode=1, stdout="", stderr="ERROR: invalid value")),
        ):
            with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
                bench_exec.validate_session_gucs("bench_db", None, run_session_gucs, variants, ("dp",))

    def test_query_group_warmup_runs_before_same_query_measured_reps(self) -> None:
        q1 = self.make_query_with_id("q1")
        q2 = self.make_query_with_id("q2")
        metrics = bench_exec.RunMetrics(
            planning_ms=1.0,
            execution_ms=2.0,
            total_ms=3.0,
            plan_total_cost=4.0,
        )

        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            run_one_mock, _ = self.patch_run_environment(
                stack,
                outputs_dir,
                [metrics] * 6,
                queries=[q1, q2],
                measured_reps=2,
                warmup_runs=1,
            )
            stack.enter_context(
                patch.object(
                    bench_run,
                    "load_sql_for_query",
                    Mock(side_effect=lambda q: q.query_id),
                )
            )

            bench_run.run_scenario(
                self.make_scenario(),
                self.make_variant_registry(),
                ("dp",),
                self.make_resolved_runs(),
                conn=None,
                run_session_gucs=self.make_run_session_gucs(),
                tag="",
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

    def test_variant_order_rotates_across_queries_and_reps(self) -> None:
        q1 = self.make_query_with_id("q1")
        q2 = self.make_query_with_id("q2")
        metrics = bench_exec.RunMetrics(
            planning_ms=1.0,
            execution_ms=2.0,
            total_ms=3.0,
            plan_total_cost=4.0,
        )
        variants = {
            "dp": Variant(name="dp", label="dp", session_gucs=()),
            "geqo": Variant(name="geqo", label="GEQO", session_gucs=()),
        }
        resolved_runs = [
            ResolvedDatasetRun(
                dataset="job",
                db="bench_job",
                variants=("dp", "geqo"),
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir, ExitStack() as stack:
            outputs_dir = Path(tmpdir) / "outputs"
            outputs_dir.mkdir()
            run_one_mock, _ = self.patch_run_environment(
                stack,
                outputs_dir,
                [metrics] * 12,
                queries=[q1, q2],
                measured_reps=2,
                warmup_runs=1,
            )
            stack.enter_context(
                patch.object(
                    bench_run,
                    "load_sql_for_query",
                    Mock(side_effect=lambda q: q.query_id),
                )
            )

            bench_run.run_scenario(
                self.make_scenario(),
                variants,
                ("dp", "geqo"),
                resolved_runs,
                conn=None,
                run_session_gucs=self.make_run_session_gucs(),
                tag="",
            )

            observed_order = [
                (call.args[3], call.args[2].name)
                for call in run_one_mock.call_args_list
            ]
            self.assertEqual(
                observed_order,
                [
                    ("q1", "dp"),
                    ("q1", "geqo"),
                    ("q1", "dp"),
                    ("q1", "geqo"),
                    ("q1", "geqo"),
                    ("q1", "dp"),
                    ("q2", "geqo"),
                    ("q2", "dp"),
                    ("q2", "geqo"),
                    ("q2", "dp"),
                    ("q2", "dp"),
                    ("q2", "geqo"),
                ],
            )

if __name__ == "__main__":
    unittest.main()
