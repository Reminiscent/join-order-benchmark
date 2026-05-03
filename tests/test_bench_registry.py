from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_common import DatasetSpec, Scenario
from bench_registry import load_scenarios, load_variants, resolve_dataset_runs, resolve_prepare_dataset_runs


class BenchRegistryTests(unittest.TestCase):
    def make_scenario(self) -> Scenario:
        return Scenario(
            name="full",
            description="test scenario",
            default_variants=("dp", "geqo"),
            statement_timeout_ms=1000,
            session_gucs=(),
            datasets=(
                DatasetSpec(dataset="gpuqo_clique_small", exclude_variants=("dp",)),
                DatasetSpec(dataset="gpuqo_clique_small", max_join=12, variants=("dp",)),
                DatasetSpec(dataset="sqlite_select5"),
            ),
        )

    def test_load_variants_uses_built_in_baselines(self) -> None:
        variants = load_variants()

        self.assertEqual(tuple(variants), ("dp", "geqo"))

    def test_load_scenarios_uses_built_in_definitions(self) -> None:
        scenarios = load_scenarios()

        self.assertEqual(tuple(scenarios), ("main", "extended", "full"))
        self.assertEqual(
            [spec.dataset for spec in scenarios["main"].datasets],
            ["job", "job_complex"],
        )
        self.assertEqual(scenarios["full"].datasets[-1].dataset, "imdb_ceb_3k")

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

    def test_prepare_dataset_resolution_ignores_variant_specific_splits(self) -> None:
        resolved = resolve_prepare_dataset_runs(self.make_scenario())

        self.assertEqual(
            [(entry.dataset, entry.db) for entry in resolved],
            [
                ("gpuqo_clique_small", "gpuqo_clique_small_bench"),
                ("sqlite_select5", "sqlite_select5_bench"),
            ],
        )
        self.assertTrue(all(entry.variants == () for entry in resolved))

    def test_dataset_resolution_can_select_non_dp_variants(self) -> None:
        resolved = resolve_dataset_runs(
            self.make_scenario(),
            ("dp", "geqo", "my_algo"),
        )

        self.assertEqual(
            [(entry.dataset, entry.max_join, entry.variants) for entry in resolved],
            [
                ("gpuqo_clique_small", None, ("geqo", "my_algo")),
                ("gpuqo_clique_small", 12, ("dp",)),
                ("sqlite_select5", None, ("dp", "geqo", "my_algo")),
            ],
        )


if __name__ == "__main__":
    unittest.main()
