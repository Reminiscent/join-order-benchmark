"""Matrix scheduling, result classification, and reporting contracts.

The tests keep PostgreSQL mocked and focus on request validation, DP
eligibility, timeout/error policy, provenance, durable outputs, and
common-success report aggregation.
"""

from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "bench"))

from bench_common import Variant
from synthetic_join_order.compile import compile_graph
from synthetic_join_order.graph import GenerationSpec, generate_graph
from synthetic_join_order.matrix import (
    DP_MAX_SIZE,
    MAIN_CARDINALITY_SEED_START,
    MAIN_GRAPH_SEED_START,
    MAIN_INSTANCES,
    MAIN_SIZES,
    matrix_specs,
    run_matrix,
    validate_matrix_request,
)
from synthetic_join_order.plan import PlanEvidence
from synthetic_join_order.report import build_matrix_reports, render_matrix_report
from synthetic_join_order.run import (
    VariantResult,
    count_dp_anomalies,
    run_variants,
    validate_run_results,
)
from tools import run_synthetic_join_order_matrix as matrix_cli


class SyntheticJoinOrderMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.graph = generate_graph(
            GenerationSpec(n=2, graph_seed=7, cardinality_seed=1003)
        )
        self.case = compile_graph(self.graph)

    def test_variant_runner_scores_success_and_preserves_timeout(self) -> None:
        variants = (
            Variant("dp", "DP", (("geqo_threshold", 100),), True),
            Variant("goo", "GOO", (("geqo_threshold", 2),)),
        )

        def fake_plan(*_args, variant_gucs=(), **_kwargs):
            cost = 10.0 if dict(variant_gucs)["geqo_threshold"] == 100 else 12.0
            return PlanEvidence(
                explain={"Plan": {"Total Cost": cost}},
                audit={},
                stats=(),
            )

        with (
            patch("synthetic_join_order.run.plan_case", side_effect=fake_plan),
            patch(
                "synthetic_join_order.run.qualify_postgres",
                side_effect=lambda _graph, _case, evidence: SimpleNamespace(
                    root_total_cost=evidence.explain["Plan"]["Total Cost"],
                ),
            ),
        ):
            results = run_variants("db", self.graph, self.case, variants)

        self.assertEqual([result.quality_ratio for result in results], [1.0, 1.2])
        self.assertEqual(count_dp_anomalies(results), 0)

        timeout = VariantResult(
            self.case.instance_id,
            "dp",
            "timeout",
            None,
            None,
            None,
            None,
            "statement timeout",
            None,
        )
        validate_run_results((timeout,))

    def test_report_uses_common_successes_and_keeps_timeout_coverage(self) -> None:
        rows = (
            {"n": 10, "instance_id": "a", "variant": "dp", "status": "ok", "quality_ratio": 1.0},
            {"n": 10, "instance_id": "a", "variant": "goo", "status": "ok", "quality_ratio": 1.2},
            {"n": 10, "instance_id": "b", "variant": "dp", "status": "timeout", "quality_ratio": ""},
            {"n": 10, "instance_id": "b", "variant": "goo", "status": "ok", "quality_ratio": 1.0},
        )

        quality, coverage = build_matrix_reports(rows, (10,), ("dp", "goo"))
        by_variant = {row["variant"]: row for row in quality}
        self.assertEqual(by_variant["dp"]["quality_samples"], 1)
        self.assertEqual(by_variant["goo"]["p50_q"], 1.2)
        dp_coverage = next(row for row in coverage if row["variant"] == "dp")
        self.assertEqual((dp_coverage["planned"], dp_coverage["timeout"]), (1, 1))

    def test_markdown_report_marks_partial_runs_and_timeout_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            rows = ({
                "n": 10,
                "instance_id": "a",
                "variant": "dp",
                "status": "timeout",
                "quality_ratio": "",
            },)
            with (output / "quality.csv").open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=tuple(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            (output / "run.json").write_text(json.dumps({
                "requested_sizes": [10, 20],
                "sizes": [10],
                "skipped_sizes": [{
                    "n": 20,
                    "eligible_variants": ["dp"],
                    "reason": "fewer than two eligible variants",
                }],
                "variants": ["dp"],
                "instance_count_per_size": 1,
                "instances": 1,
                "processed_instances": 1,
                "complete": False,
                "abort_error": "stopped",
                "status_counts": {"ok": 0, "timeout": 1, "error": 0},
            }))
            report = output / "report.md"
            render_matrix_report(output, report)
            text = report.read_text()

            run = json.loads((output / "run.json").read_text())
            run["complete"] = "false"
            (output / "run.json").write_text(json.dumps(run))
            with self.assertRaisesRegex(ValueError, "complete.*boolean"):
                render_matrix_report(output, report)

            del run["complete"]
            (output / "run.json").write_text(json.dumps(run))
            with self.assertRaises(KeyError):
                render_matrix_report(output, report)

        self.assertIn("Matrix: INCOMPLETE", text)
        self.assertIn("## Incomplete coverage", text)
        self.assertIn("Skipped sizes: 20 (dp)", text)
        self.assertIn("### Common-success sample size", text)
        self.assertIn("| 10 | 0 |", text)
        self.assertIn("N/A", text)

    def test_matrix_schedules_dp_only_inside_its_boundary(self) -> None:
        variants = (
            Variant("dp", "DP", (("geqo_threshold", 100),), True),
            Variant("geqo", "GEQO", (("geqo_threshold", 2),), True),
            Variant("goo", "GOO", (("geqo_threshold", 2),)),
        )
        scheduled: list[tuple[str, ...]] = []

        def fake_run(_db, _graph, case, selected, *_args, **_kwargs):
            scheduled.append(tuple(variant.name for variant in selected))
            return tuple(
                VariantResult(
                    case.instance_id,
                    variant.name,
                    "ok",
                    10.0,
                    1.0,
                    10.0,
                    variant.name,
                    "",
                    None,
                )
                for variant in selected
            )

        with (
            tempfile.TemporaryDirectory() as temp,
            patch("synthetic_join_order.matrix.generate_graph", return_value=self.graph),
            patch("synthetic_join_order.matrix.compile_graph", return_value=self.case),
            patch("synthetic_join_order.matrix.install_case"),
            patch("synthetic_join_order.matrix.uninstall_case"),
            patch("synthetic_join_order.matrix.run_variants", side_effect=fake_run),
        ):
            run_matrix("db", (20, 30), 1, variants, Path(temp) / "matrix")

        self.assertEqual(scheduled, [("dp", "geqo", "goo"), ("geqo", "goo")])

    def test_matrix_skips_sizes_without_two_eligible_variants(self) -> None:
        variants = (
            Variant("dp", "DP", (("geqo_threshold", 100),), True),
            Variant("geqo", "GEQO", (("geqo_threshold", 2),), True),
        )
        with tempfile.TemporaryDirectory() as temp:
            runnable, skipped = validate_matrix_request(
                (20, 30),
                1,
                variants,
                Path(temp) / "matrix",
                statement_timeout_ms=60_000,
                graph_seed_start=200,
                cardinality_seed_start=1200,
            )
            success = tuple(
                VariantResult(
                    self.case.instance_id,
                    variant.name,
                    "ok",
                    10.0,
                    1.0,
                    10.0,
                    variant.name,
                    "",
                    None,
                )
                for variant in variants
            )
            with (
                patch("synthetic_join_order.matrix.generate_graph", return_value=self.graph),
                patch("synthetic_join_order.matrix.compile_graph", return_value=self.case),
                patch("synthetic_join_order.matrix.install_case"),
                patch("synthetic_join_order.matrix.uninstall_case"),
                patch("synthetic_join_order.matrix.run_variants", return_value=success) as run,
            ):
                output = Path(temp) / "run"
                run_matrix(
                    "db",
                    (20, 30),
                    1,
                    variants,
                    output,
                    benchmark_metadata={"source_revision": "bench123"},
                )
                context = json.loads((output / "run.json").read_text())

        self.assertEqual(runnable, (20,))
        self.assertEqual(skipped, (30,))
        run.assert_called_once()
        self.assertEqual(context["requested_sizes"], [20, 30])
        self.assertEqual(context["sizes"], [20])
        self.assertEqual(context["formula_path_cost_fuzz_factor"], 1.0)
        self.assertEqual(
            context["benchmark"],
            {"source_revision": "bench123"},
        )
        self.assertEqual(context["skipped_sizes"], [{
            "eligible_variants": ["geqo"],
            "n": 30,
            "reason": "fewer than two eligible variants",
        }])

    def test_matrix_rejects_when_every_size_has_only_one_variant(self) -> None:
        variant = Variant("goo", "GOO", (("geqo_threshold", 2),))
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(
                ValueError,
                "every requested matrix size has fewer than two eligible variants",
            ):
                validate_matrix_request(
                    (30,),
                    1,
                    (variant,),
                    Path(temp) / "matrix",
                    statement_timeout_ms=60_000,
                    graph_seed_start=200,
                    cardinality_seed_start=1200,
                )

    def test_matrix_records_timeout_without_aborting(self) -> None:
        variants = (
            Variant("dp", "DP", (("geqo_threshold", 100),), True),
            Variant("geqo", "GEQO", (("geqo_threshold", 2),), True),
        )
        timeout = VariantResult(
            self.case.instance_id,
            "dp",
            "timeout",
            None,
            None,
            None,
            None,
            "statement timeout",
            None,
        )
        success = VariantResult(
            self.case.instance_id,
            "geqo",
            "ok",
            10.0,
            1.0,
            10.0,
            "geqo",
            "",
            None,
        )
        with (
            tempfile.TemporaryDirectory() as temp,
            patch("synthetic_join_order.matrix.generate_graph", return_value=self.graph),
            patch("synthetic_join_order.matrix.compile_graph", return_value=self.case),
            patch("synthetic_join_order.matrix.install_case"),
            patch("synthetic_join_order.matrix.uninstall_case"),
            patch(
                "synthetic_join_order.matrix.run_variants",
                return_value=(timeout, success),
            ),
        ):
            output = Path(temp) / "matrix"
            rows = run_matrix("db", (10,), 1, variants, output)
            context = json.loads((output / "run.json").read_text())
            with (output / "workload_diagnostics.csv").open(newline="") as file:
                diagnostic_fields = tuple(csv.DictReader(file).fieldnames or ())
            with (output / "quality.csv").open(newline="") as file:
                quality_fields = tuple(csv.DictReader(file).fieldnames or ())

        self.assertEqual(rows[0]["status"], "timeout")
        self.assertTrue(context["complete"])
        self.assertEqual(context["status_counts"]["timeout"], 1)
        self.assertNotIn("planning_ms", quality_fields)
        self.assertNotIn("balance_applied", diagnostic_fields)
        self.assertNotIn("root_effective_rows", diagnostic_fields)
        self.assertNotIn("selected_join_count", diagnostic_fields)

    def test_matrix_cleans_up_after_unexpected_error(self) -> None:
        variants = (
            Variant("dp", "DP", (("geqo_threshold", 100),), True),
            Variant("geqo", "GEQO", (("geqo_threshold", 2),), True),
        )
        with (
            tempfile.TemporaryDirectory() as temp,
            patch("synthetic_join_order.matrix.generate_graph", return_value=self.graph),
            patch("synthetic_join_order.matrix.compile_graph", return_value=self.case),
            patch("synthetic_join_order.matrix.install_case"),
            patch("synthetic_join_order.matrix.uninstall_case") as uninstall,
            patch("synthetic_join_order.matrix.run_variants", side_effect=ValueError("bad evidence")),
        ):
            output = Path(temp) / "matrix"
            with self.assertRaisesRegex(ValueError, "bad evidence"):
                run_matrix("db", (10,), 1, variants, output)
            context = json.loads((output / "run.json").read_text())

        uninstall.assert_called_once_with("db", self.case, None)
        self.assertEqual(context["abort_error"], "bad evidence")
        self.assertFalse(context["complete"])

    def test_source_provenance_accepts_git_worktree_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp)
            args = SimpleNamespace(postgres_source=source)

            def command_output(command):
                if command[-1] == "--is-inside-work-tree":
                    return "true"
                if command[-1] == "--untracked-files=all":
                    return ""
                if command[-1] == "HEAD":
                    return "abc123"
                self.fail(f"unexpected metadata command: {command}")

            with patch.object(matrix_cli, "_command_output", side_effect=command_output):
                metadata = matrix_cli._postgres_metadata(args)

        self.assertEqual(metadata, {"source_revision": "abc123"})

    def test_benchmark_provenance_records_revision(self) -> None:
        with patch.object(
            matrix_cli, "_command_output", return_value="bench123"
        ) as command_output:
            metadata = matrix_cli._benchmark_metadata()

        self.assertEqual(metadata, {"source_revision": "bench123"})
        command_output.assert_called_once_with(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"]
        )

    def test_cli_validates_before_postgres_or_database_work(self) -> None:
        variant = Variant("dp", "DP", (("geqo_threshold", 100),), True)
        with tempfile.TemporaryDirectory() as temp:
            args = SimpleNamespace(
                db="db",
                sizes="10,10",
                instances=1,
                variants=None,
                output=Path(temp) / "matrix",
                host=None,
                port=None,
                user=None,
                bootstrap_metadata=True,
                statement_timeout_ms=60_000,
                graph_seed_start=200,
                cardinality_seed_start=1200,
                postgres_source=Path("unused"),
            )
            with (
                patch.object(matrix_cli, "parse_args", return_value=args),
                patch.object(matrix_cli, "load_variants", return_value={"dp": variant}),
                patch.object(matrix_cli, "_benchmark_metadata") as benchmark_metadata,
                patch.object(matrix_cli, "_postgres_metadata") as metadata,
                patch.object(matrix_cli, "postgres_server_version") as server_version,
                patch.object(matrix_cli, "bootstrap_metadata") as bootstrap,
            ):
                with redirect_stderr(StringIO()):
                    self.assertEqual(matrix_cli.main(), 2)

        benchmark_metadata.assert_not_called()
        metadata.assert_not_called()
        server_version.assert_not_called()
        bootstrap.assert_not_called()

    def test_cli_warns_when_requested_sizes_are_skipped(self) -> None:
        variants = {
            "dp": Variant("dp", "DP", (("geqo_threshold", 100),), True),
            "geqo": Variant("geqo", "GEQO", (("geqo_threshold", 2),), True),
        }
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "matrix"
            args = SimpleNamespace(
                db="db",
                sizes="20,30",
                instances=1,
                variants=None,
                output=output,
                host=None,
                port=None,
                user=None,
                bootstrap_metadata=False,
                statement_timeout_ms=60_000,
                graph_seed_start=200,
                cardinality_seed_start=1200,
                postgres_source=Path("source"),
            )

            def fake_run_matrix(*_args, **_kwargs):
                output.mkdir()
                (output / "run.json").write_text(json.dumps({"dp_anomaly_count": 0}))
                return ()

            stderr = StringIO()
            with (
                patch.object(matrix_cli, "parse_args", return_value=args),
                patch.object(matrix_cli, "load_variants", return_value=variants),
                patch.object(matrix_cli, "_benchmark_metadata", return_value={}),
                patch.object(matrix_cli, "_postgres_metadata", return_value={}),
                patch.object(matrix_cli, "postgres_server_version", return_value="19devel"),
                patch.object(matrix_cli, "run_matrix", side_effect=fake_run_matrix),
                redirect_stderr(stderr),
            ):
                self.assertEqual(matrix_cli.main(), 0)

        self.assertIn(
            "skipping requested sizes with fewer than two eligible variants: "
            "30 (geqo); executing sizes: 20",
            stderr.getvalue(),
        )

    def test_main_workload_contract_has_expected_shape(self) -> None:
        specs = matrix_specs(
            MAIN_SIZES,
            MAIN_INSTANCES,
            graph_seed_start=MAIN_GRAPH_SEED_START,
            cardinality_seed_start=MAIN_CARDINALITY_SEED_START,
        )

        self.assertEqual(MAIN_SIZES, tuple(range(12, 21)))
        self.assertEqual(len(specs), len(MAIN_SIZES) * MAIN_INSTANCES)
        self.assertEqual(DP_MAX_SIZE, 20)


if __name__ == "__main__":
    unittest.main()
