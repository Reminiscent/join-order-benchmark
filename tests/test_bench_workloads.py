from __future__ import annotations

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_common import QueryMeta, ResolvedDatasetRun, Scenario
import bench_workloads
import bench
from bench_workloads import (
    load_scenarios,
    load_variants,
    resolve_dataset_runs,
    resolve_prepare_dataset_runs,
    select_queries,
)


class BenchWorkloadsTests(unittest.TestCase):
    def make_scenario(self) -> Scenario:
        return Scenario(
            name="planning",
            description="test scenario",
            default_variants=("dp", "geqo"),
            statement_timeout_ms=1000,
            session_gucs=(),
            datasets=("gpuqo_clique_small", "sqlite_select5"),
        )

    def test_load_variants_uses_default_extra_file_when_present(self) -> None:
        variants = load_variants()

        self.assertIn("dp", variants)
        self.assertIn("geqo", variants)
        self.assertIn("goo_cost", variants)

    def test_load_variants_uses_built_ins_when_default_extra_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_path = Path(tmpdir) / "missing.toml"
            with patch.object(bench_workloads, "DEFAULT_VARIANTS_FILE", missing_path):
                variants = load_variants()

        self.assertEqual(tuple(variants), ("dp", "geqo"))

    def test_load_scenarios_uses_built_in_definitions(self) -> None:
        scenarios = load_scenarios()

        self.assertEqual(tuple(scenarios), ("main", "extended", "planning"))
        self.assertEqual(
            list(scenarios["main"].datasets),
            ["job", "job_complex"],
        )
        self.assertEqual(
            list(scenarios["extended"].datasets),
            ["job", "job_complex", "imdb_ceb_3k"],
        )
        self.assertEqual(
            list(scenarios["planning"].datasets),
            [
                "sqlite_select5",
                "gpuqo_chain_small",
                "gpuqo_star_small",
                "gpuqo_snowflake_small",
                "gpuqo_clique_small",
            ],
        )
        self.assertTrue(
            all(scenario.default_variants == ("dp", "geqo") for scenario in scenarios.values())
        )

    def test_print_scenarios_omits_default_variants(self) -> None:
        out = io.StringIO()

        with redirect_stdout(out):
            bench.print_scenarios({"planning": self.make_scenario()})

        self.assertEqual(
            out.getvalue(),
            "Scenarios\n"
            "name\tdatasets\tdescription\n"
            "planning\tgpuqo_clique_small, sqlite_select5\ttest scenario\n"
            "\n",
        )

    def test_load_variants_accepts_extra_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "variants.toml"
            path.write_text(
                """
[[variant]]
name = "my_algo"
label = "My Algorithm"
session_gucs = { geqo_threshold = 2, enable_my_algo = "on" }
"""
            )

            variants = load_variants(path)

        self.assertEqual(tuple(variants), ("dp", "geqo", "my_algo"))
        self.assertEqual(variants["my_algo"].label, "My Algorithm")

    def test_prepare_dataset_resolution_uses_scenario_datasets(self) -> None:
        resolved = resolve_prepare_dataset_runs(self.make_scenario())

        self.assertEqual(
            [(entry.dataset, entry.db) for entry in resolved],
            [
                ("gpuqo_clique_small", "gpuqo_clique_small_bench"),
                ("sqlite_select5", "sqlite_select5_bench"),
            ],
        )
        self.assertTrue(all(entry.variants == () for entry in resolved))

    def test_dataset_resolution_uses_selected_variants_for_all_datasets(self) -> None:
        resolved = resolve_dataset_runs(
            self.make_scenario(),
            ("dp", "geqo", "my_algo"),
        )

        self.assertEqual(
            [
                (entry.dataset, entry.min_join, entry.variants)
                for entry in resolved
            ],
            [
                ("gpuqo_clique_small", None, ("dp", "geqo", "my_algo")),
                ("sqlite_select5", None, ("dp", "geqo", "my_algo")),
            ],
        )

    def test_dataset_resolution_applies_min_join_override(self) -> None:
        resolved = resolve_dataset_runs(
            self.make_scenario(),
            ("dp", "geqo"),
            min_join=12,
        )

        self.assertEqual(
            [
                (entry.dataset, entry.min_join, entry.variants)
                for entry in resolved
            ],
            [
                ("gpuqo_clique_small", 12, ("dp", "geqo")),
                ("sqlite_select5", 12, ("dp", "geqo")),
            ],
        )

    def test_select_queries_filters_by_min_join(self) -> None:
        queries = [
            QueryMeta("job", "q10", "job/q10.sql", "Q10", 10),
            QueryMeta("job", "q12", "job/q12.sql", "Q12", 12),
            QueryMeta("job", "q14", "job/q14.sql", "Q14", 14),
            QueryMeta("job", "q16", "job/q16.sql", "Q16", 16),
        ]
        spec = ResolvedDatasetRun(
            dataset="job",
            db="bench_job",
            min_join=12,
            variants=("dp",),
        )

        with patch.object(bench_workloads, "parse_manifest", return_value=queries):
            selected = select_queries(spec)

        self.assertEqual([q.query_id for q in selected], ["q12", "q14", "q16"])


if __name__ == "__main__":
    unittest.main()
