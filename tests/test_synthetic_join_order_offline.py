"""Offline contracts for synthetic join-order generation and compilation.

These tests freeze deterministic generation, check subset-cardinality
invariants, and verify the checked-in golden case without using PostgreSQL.
"""

from __future__ import annotations

import copy
import json
import math
import statistics
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from synthetic_join_order.compile import (
    VIRTUAL_PAGE_BYTES,
    CompileError,
    compile_graph,
    relation_page_counts,
    verify_case,
    write_case,
)
from synthetic_join_order.graph import (
    BASE_ROWS_MAX,
    BASE_ROWS_MIN,
    CARDINALITY_BUDGET,
    GenerationSpec,
    SpecError,
    SplitMix64,
    generate_graph,
    validate_graph,
)
from synthetic_join_order.cardinality import (
    MAXIMUM_ROWCOUNT,
    clamp_row_est_from_log,
    connected_subsets,
    recurrence_log_cardinality,
    subset_cardinality,
    subset_log_cardinality,
)
from synthetic_join_order.qualify import QualificationError, qualify_graph


GOLDEN_SPEC = ROOT / "synthetic-join-order" / "specs" / "golden_n5.json"
GOLDEN_CASE = ROOT / "synthetic-join-order" / "cases" / "golden_n5"


class SyntheticJoinOrderOfflineTests(unittest.TestCase):
    def test_generation_is_frozen_deterministic_and_minimal(self) -> None:
        rng = SplitMix64(0)
        self.assertEqual(rng.next_u64(), 0xE220A8397B1DCDAF)

        spec = GenerationSpec(n=8, graph_seed=7, cardinality_seed=1006)
        graph = generate_graph(spec)

        self.assertEqual(
            spec.to_dict(),
            {"n": 8, "graph_seed": 7, "cardinality_seed": 1006},
        )
        with self.assertRaisesRegex(SpecError, "unknown spec fields"):
            GenerationSpec.from_dict({**spec.to_dict(), "extra": 1})
        self.assertEqual(graph, generate_graph(spec))
        self.assertEqual(set(graph), {
            "format", "source_spec", "nodes", "edges", "cardinality_model"
        })
        self.assertEqual(len(graph["nodes"]), 8)
        self.assertEqual(len(graph["edges"]), 7)
        self.assertTrue(
            all("payload_width" in node for node in graph["nodes"])
        )
        self.assertEqual(
            set(graph["edges"][0]),
            {
                "id",
                "left",
                "right",
                "parent",
                "child",
                "growth_role",
                "growth_factor",
                "left_key_ndv",
                "right_key_ndv",
                "key_overlap_fraction",
                "selectivity",
            },
        )

    def test_bounded_growth_uses_a_random_anchor_and_uneven_log_budgets(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=100, graph_seed=0, cardinality_seed=1000)
        )
        model = graph["cardinality_model"]

        self.assertEqual(model["budget"], CARDINALITY_BUDGET)
        self.assertGreater(model["selective_edge_count"], 0)
        self.assertGreater(model["expanding_edge_count"], 0)
        self.assertEqual(model["neutral_edge_count"], 0)
        self.assertEqual(
            model["selective_edge_count"] + model["expanding_edge_count"],
            99,
        )
        self.assertAlmostEqual(
            max(
                model["contraction_log_budget"],
                model["expansion_log_budget"],
            ),
            math.log(CARDINALITY_BUDGET),
        )
        for role in ("selective", "expanding"):
            shares = [
                abs(math.log(edge["growth_factor"]))
                for edge in graph["edges"]
                if edge["growth_role"] == role
            ]
            self.assertGreater(max(shares), 5 * statistics.median(shares))
        self.assertAlmostEqual(
            model["root_log_rows"],
            math.log(graph["nodes"][model["anchor_relation"]]["base_rows"])
            + math.log(model["root_growth_factor"]),
            places=11,
        )
        self.assertGreaterEqual(
            model["minimum_connected_log_rows"],
            math.log(BASE_ROWS_MIN / CARDINALITY_BUDGET) - 1e-11,
        )
        self.assertLessEqual(
            model["maximum_connected_log_rows"],
            math.log(BASE_ROWS_MAX * CARDINALITY_BUDGET) + 1e-11,
        )

    def test_two_relations_apply_the_random_root_growth_directly(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=2, graph_seed=0, cardinality_seed=1000)
        )

        edge = graph["edges"][0]
        model = graph["cardinality_model"]
        self.assertIn(edge["growth_role"], {"selective", "expanding"})
        self.assertEqual(model["neutral_edge_count"], 0)
        self.assertAlmostEqual(edge["growth_factor"], model["root_growth_factor"])

    def test_growth_first_realization_allows_partial_overlap(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=100, graph_seed=0, cardinality_seed=1000)
        )
        roles = {edge["growth_role"] for edge in graph["edges"]}

        self.assertEqual(roles, {"selective", "expanding"})
        self.assertTrue(
            any(
                edge["key_overlap_fraction"] < 0.99
                for edge in graph["edges"]
            )
        )
        for edge in graph["edges"]:
            child_rows = graph["nodes"][edge["child"]]["base_rows"]
            child_ndv = (
                edge["left_key_ndv"]
                if edge["child"] == edge["left"]
                else edge["right_key_ndv"]
            )
            effective_multiplicity = child_rows / child_ndv
            self.assertAlmostEqual(
                edge["selectivity"],
                edge["key_overlap_fraction"]
                / max(edge["left_key_ndv"], edge["right_key_ndv"]),
                places=15,
            )
            self.assertAlmostEqual(
                child_rows * edge["selectivity"],
                edge["growth_factor"],
                places=14,
            )
            self.assertAlmostEqual(
                edge["key_overlap_fraction"] * effective_multiplicity,
                edge["growth_factor"],
                places=12,
            )
            self.assertLess(
                edge["left_key_ndv"],
                graph["nodes"][edge["left"]]["base_rows"],
            )
            self.assertLess(
                edge["right_key_ndv"],
                graph["nodes"][edge["right"]]["base_rows"],
            )

    def test_validation_rejects_derived_field_drift(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=20, graph_seed=4, cardinality_seed=1004)
        )
        broken = copy.deepcopy(graph)
        broken["edges"][0]["selectivity"] *= 0.5

        with self.assertRaisesRegex(SpecError, "selectivity does not match"):
            validate_graph(broken)

    def test_oracle_matches_product_recurrence_and_clamping(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=5, graph_seed=7, cardinality_seed=1003)
        )
        full = tuple(range(5))
        product = math.prod(node["base_rows"] for node in graph["nodes"])
        product *= math.prod(edge["selectivity"] for edge in graph["edges"])

        self.assertAlmostEqual(subset_cardinality(graph, full), float(round(product)), delta=1.0)
        self.assertAlmostEqual(
            recurrence_log_cardinality(graph, (0, 2), (1, 3, 4)),
            subset_log_cardinality(graph, full),
            places=11,
        )
        self.assertEqual(clamp_row_est_from_log(math.log(MAXIMUM_ROWCOUNT) + 1), MAXIMUM_ROWCOUNT)

    def test_recorded_connected_extrema_match_exhaustive_oracle(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=8, graph_seed=17, cardinality_seed=2001)
        )
        values = [
            subset_log_cardinality(graph, subset)
            for subset in connected_subsets(graph)
        ]

        self.assertAlmostEqual(
            graph["cardinality_model"]["minimum_connected_log_rows"],
            min(values),
            places=12,
        )
        self.assertAlmostEqual(
            graph["cardinality_model"]["maximum_connected_log_rows"],
            max(values),
            places=12,
        )

    def test_virtual_pages_follow_generated_projected_width(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=5, graph_seed=7, cardinality_seed=1003)
        )
        pages = relation_page_counts(graph)

        degree = {node["id"]: 0 for node in graph["nodes"]}
        for edge in graph["edges"]:
            degree[edge["left"]] += 1
            degree[edge["right"]] += 1
        self.assertEqual(pages, {
            node["id"]: max(
                1,
                math.ceil(
                    node["base_rows"]
                    * (node["payload_width"] + 8 * degree[node["id"]])
                    / VIRTUAL_PAGE_BYTES
                ),
            )
            for node in graph["nodes"]
        })

    def test_compiler_covers_the_contract(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=5, graph_seed=7, cardinality_seed=1003)
        )
        case = compile_graph(graph)

        self.assertEqual(case.files["query.sql"].count(" = "), 4)
        self.assertEqual(case.files["query.sql"].count('"payload" AS'), 5)
        self.assertEqual(case.files["schema.sql"].count("payload text"), 5)
        self.assertNotIn("CREATE INDEX", case.files["schema.sql"].upper())
        self.assertIn("n_distinct", case.files["base_stats.sql"])
        graph_markdown = case.files["graph.md"]
        self.assertIn("```mermaid", graph_markdown)
        self.assertIn('R0["R0<br/>rows=', graph_markdown)
        self.assertIn("<br/>growth ×", graph_markdown)
        self.assertIn("(anchor)<br/>rows=", graph_markdown)
        self.assertIn("Left `D` | Left `M` | Right `D` | Right `M`", graph_markdown)
        metadata = case.files["cardinality_metadata.sql"]
        self.assertIn("bench.join_order_relation", metadata)
        self.assertIn("bench.join_order_edge", metadata)
        self.assertNotIn("key_overlap_fraction", metadata)

    def test_offline_qualification_covers_compiler_and_oracle(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=5, graph_seed=7, cardinality_seed=1003)
        )

        report = qualify_graph(graph, compile_graph(graph))

        self.assertEqual(report.node_count, 5)
        self.assertGreater(report.connected_subset_count, 0)
        self.assertGreater(report.recurrence_check_count, 0)

    def test_exhaustive_qualification_stays_small(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=11, graph_seed=0, cardinality_seed=1000)
        )

        with self.assertRaises(QualificationError):
            qualify_graph(graph)

    def test_golden_case_recompiles_exactly_and_detects_drift(self) -> None:
        spec = GenerationSpec.from_dict(json.loads(GOLDEN_SPEC.read_text()))
        case = compile_graph(generate_graph(spec))
        verify_case(case, GOLDEN_CASE)

        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            write_case(case, output)
            (output / "query.sql").write_text("SELECT 2;\n")
            with self.assertRaises(CompileError):
                verify_case(case, output)

if __name__ == "__main__":
    unittest.main()
