from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_common import DatasetSpec, Scenario
from bench_config import load_variants, resolve_prepare_dataset_runs


class BenchConfigTests(unittest.TestCase):
    def make_scenario(self) -> Scenario:
        return Scenario(
            name="full",
            description="test scenario",
            default_variants=("dp", "geqo", "hybrid_search"),
            reps=1,
            statement_timeout_ms=1000,
            stabilize="none",
            variant_order_mode="fixed",
            session_gucs=(),
            datasets=(
                DatasetSpec(dataset="gpuqo_clique_small", variants=("geqo", "hybrid_search")),
                DatasetSpec(dataset="gpuqo_clique_small", max_join=12, variants=("dp",)),
                DatasetSpec(dataset="sqlite_select5"),
            ),
        )

    def test_load_variants_uses_default_example_file(self) -> None:
        variants = load_variants()

        self.assertIn("dp", variants)
        self.assertIn("hybrid_search", variants)

    def test_load_variants_accepts_custom_file(self) -> None:
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

        self.assertEqual(tuple(variants), ("my_algo",))
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


if __name__ == "__main__":
    unittest.main()
