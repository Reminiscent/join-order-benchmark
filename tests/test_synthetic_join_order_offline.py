"""Offline contracts for synthetic join-order generation and compilation.

These tests freeze deterministic generation, check subset-cardinality
invariants, and verify the checked-in golden case without using PostgreSQL.
"""

from __future__ import annotations

import copy
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from synthetic_join_order.compile import (
    VIRTUAL_ROWS_PER_PAGE,
    CompileError,
    compile_graph,
    relation_page_counts,
    verify_case,
    write_case,
)
from synthetic_join_order.graph import (
    ENDPOINT_MULTIPLICITY_SPREAD_MAX,
    GenerationSpec,
    SpecError,
    SplitMix64,
    generate_graph,
    validate_graph,
)
from synthetic_join_order.cardinality import (
    MAXIMUM_ROWCOUNT,
    clamp_row_est_from_log,
    recurrence_log_cardinality,
    subset_cardinality,
    subset_log_cardinality,
)


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
            "format", "source_spec", "nodes", "edges", "root_balance"
        })
        self.assertEqual(len(graph["nodes"]), 8)
        self.assertEqual(len(graph["edges"]), 7)
        self.assertEqual(
            set(graph["edges"][0]),
            {
                "id",
                "left",
                "right",
                "type",
                "left_key_ndv",
                "right_key_ndv",
                "key_overlap_fraction",
                "selectivity",
            },
        )

    def test_root_balance_uses_one_downward_scale(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=100, graph_seed=0, cardinality_seed=1000)
        )
        scale = graph["root_balance"]["edge_scale"]

        self.assertLessEqual(scale, 1.0)
        for edge in graph["edges"]:
            self.assertAlmostEqual(
                edge["selectivity"],
                edge["key_overlap_fraction"]
                / max(edge["left_key_ndv"], edge["right_key_ndv"]),
                places=15,
            )

    def test_edge_model_separates_key_overlap_from_multiplicity(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=100, graph_seed=0, cardinality_seed=1000)
        )
        seen_types = {edge["type"] for edge in graph["edges"]}

        self.assertEqual(seen_types, {"one_to_many", "many_to_many"})
        for edge in graph["edges"]:
            left_rows = graph["nodes"][edge["left"]]["base_rows"]
            right_rows = graph["nodes"][edge["right"]]["base_rows"]
            smaller_rows = min(left_rows, right_rows)
            larger_rows = max(left_rows, right_rows)
            larger_ndv = max(edge["left_key_ndv"], edge["right_key_ndv"])
            output_factor = (
                edge["key_overlap_fraction"] * smaller_rows / larger_ndv
            )
            join_rows = larger_rows * output_factor

            self.assertAlmostEqual(
                join_rows,
                left_rows
                * right_rows
                * edge["key_overlap_fraction"]
                / larger_ndv,
                places=8,
            )
            self.assertLessEqual(edge["left_key_ndv"], smaller_rows)
            self.assertLessEqual(edge["right_key_ndv"], smaller_rows)
            self.assertGreaterEqual(
                edge["left_key_ndv"],
                math.ceil(smaller_rows / ENDPOINT_MULTIPLICITY_SPREAD_MAX),
            )
            self.assertGreaterEqual(
                edge["right_key_ndv"],
                math.ceil(smaller_rows / ENDPOINT_MULTIPLICITY_SPREAD_MAX),
            )
            left_multiplicity = left_rows / edge["left_key_ndv"]
            right_multiplicity = right_rows / edge["right_key_ndv"]
            if edge["type"] == "one_to_many":
                key_side_ndv = (
                    edge["left_key_ndv"]
                    if left_rows <= right_rows
                    else edge["right_key_ndv"]
                )
                self.assertEqual(key_side_ndv, smaller_rows)
                self.assertIn(1.0, (left_multiplicity, right_multiplicity))
                self.assertGreater(max(left_multiplicity, right_multiplicity), 1.0)
                self.assertLessEqual(output_factor, 1.0)
            else:
                self.assertGreater(left_multiplicity, 1.0)
                self.assertGreater(right_multiplicity, 1.0)

    def test_validation_rejects_derived_field_drift(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=20, graph_seed=4, cardinality_seed=1004)
        )
        broken = copy.deepcopy(graph)
        broken["edges"][0]["selectivity"] *= 0.5

        with self.assertRaisesRegex(SpecError, "key overlap and endpoint NDVs"):
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

    def test_virtual_pages_are_fixed_width_and_topology_independent(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=5, graph_seed=7, cardinality_seed=1003)
        )
        pages = relation_page_counts(graph)

        self.assertEqual(
            pages,
            {
                node["id"]: max(1, math.ceil(node["base_rows"] / VIRTUAL_ROWS_PER_PAGE))
                for node in graph["nodes"]
            },
        )

    def test_compiler_covers_the_contract(self) -> None:
        graph = generate_graph(
            GenerationSpec(n=5, graph_seed=7, cardinality_seed=1003)
        )
        case = compile_graph(graph)

        self.assertEqual(case.files["query.sql"].count(" = "), 4)
        self.assertNotIn("CREATE INDEX", case.files["schema.sql"].upper())
        self.assertIn("n_distinct", case.files["base_stats.sql"])
        graph_markdown = case.files["graph.md"]
        self.assertIn("```mermaid", graph_markdown)
        self.assertIn('R0["R0<br/>rows=', graph_markdown)
        self.assertIn("one_to_many<br/>output ×", graph_markdown)
        self.assertIn("| Left `D` | Left `M` | Right `D` | Right `M` |", graph_markdown)
        metadata = case.files["cardinality_metadata.sql"]
        self.assertIn("bench.join_order_relation", metadata)
        self.assertIn("bench.join_order_edge", metadata)
        self.assertNotIn("key_overlap_fraction", metadata)

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
