from __future__ import annotations

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_common import QueryMeta, ResolvedDatasetRun, Scenario
import bench_config
import bench
from bench_config import (
    load_run_settings,
    load_scenarios,
    load_variants,
    resolve_dataset_runs,
    resolve_prepare_dataset_runs,
    resolve_variant_names,
    select_queries,
)


class BenchConfigTests(unittest.TestCase):
    def make_scenario(self) -> Scenario:
        return Scenario(
            name="planning",
            description="test scenario",
            default_variants=("dp", "geqo"),
            datasets=("gpuqo_clique_small", "sqlite_select5"),
        )

    def test_load_variants_includes_built_ins(self) -> None:
        variants = load_variants()

        self.assertEqual(variants["dp"].label, "dp")
        self.assertEqual(variants["geqo"].label, "GEQO")

    def test_load_variants_uses_built_ins_when_config_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_path = Path(tmpdir) / "missing.toml"

            with patch.object(bench_config, "VARIANTS_FILE", missing_path):
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

    def test_load_variants_reads_extra_variants_from_config_file(self) -> None:
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

            with patch.object(bench_config, "VARIANTS_FILE", path):
                variants = load_variants()

        self.assertEqual(tuple(variants), ("dp", "geqo", "my_algo"))
        self.assertEqual(variants["my_algo"].label, "My Algorithm")

    def test_resolve_variant_names_rejects_unknown_variant(self) -> None:
        variants = {variant.name: variant for variant in bench_config.BUILT_IN_VARIANTS}

        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            resolve_variant_names(
                self.make_scenario(),
                variants,
                "dp,missing_algo",
            )

    def test_load_variants_rejects_non_scalar_guc_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "variants.toml"
            path.write_text(
                """
[[variant]]
name = "bad"
session_gucs = { work_mem = ["1GB"] }
"""
            )

            with (
                patch.object(bench_config, "VARIANTS_FILE", path),
                redirect_stderr(StringIO()),
                self.assertRaises(SystemExit),
            ):
                load_variants()

    def test_load_run_settings_reads_session_gucs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "benchmark_settings.toml"
            path.write_text(
                """
statement_timeout = 1234
join_collapse_limit = 100
work_mem = "1GB"
"""
            )

            with patch.object(bench_config, "BENCHMARK_SETTINGS_FILE", path):
                session_gucs = load_run_settings()

        self.assertEqual(
            session_gucs,
            (
                ("statement_timeout", 1234),
                ("join_collapse_limit", 100),
                ("work_mem", "1GB"),
            ),
        )

    def test_load_run_settings_requires_at_least_one_setting(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "benchmark_settings.toml"
            path.write_text("")

            with (
                patch.object(bench_config, "BENCHMARK_SETTINGS_FILE", path),
                redirect_stderr(StringIO()),
                self.assertRaises(SystemExit),
            ):
                load_run_settings()

    def test_load_run_settings_rejects_table_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "benchmark_settings.toml"
            path.write_text(
                """
[nested]
value = 1000
"""
            )

            with (
                patch.object(bench_config, "BENCHMARK_SETTINGS_FILE", path),
                redirect_stderr(StringIO()),
                self.assertRaises(SystemExit),
            ):
                load_run_settings()

    def test_load_run_settings_rejects_empty_guc_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "benchmark_settings.toml"
            path.write_text(
                """
"" = "bad"
"""
            )

            with (
                patch.object(bench_config, "BENCHMARK_SETTINGS_FILE", path),
                redirect_stderr(StringIO()),
                self.assertRaises(SystemExit),
            ):
                load_run_settings()

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
            QueryMeta("job", "q10", "job/q10.sql", 10),
            QueryMeta("job", "q12", "job/q12.sql", 12),
            QueryMeta("job", "q14", "job/q14.sql", 14),
            QueryMeta("job", "q16", "job/q16.sql", 16),
        ]
        spec = ResolvedDatasetRun(
            dataset="job",
            db="bench_job",
            min_join=12,
            variants=("dp",),
        )

        with patch.object(bench_config, "parse_manifest", return_value=queries):
            selected = select_queries(spec)

        self.assertEqual([q.query_id for q in selected], ["q12", "q14", "q16"])


if __name__ == "__main__":
    unittest.main()
