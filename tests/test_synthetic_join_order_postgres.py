"""PostgreSQL driver and qualification contracts without a live server.

The tests use structured plan/audit fixtures to check script construction,
output parsing, fail-closed evidence validation, and CLI cleanup behavior.
"""

from __future__ import annotations

import json
import math
import sys
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "bench"))

from synthetic_join_order.compile import compile_graph, relation_page_counts
from synthetic_join_order.graph import (
    GenerationSpec,
    edge_endpoint_key_ndv,
    generate_graph,
)
from synthetic_join_order.cardinality import subset_cardinality, subset_log_cardinality
from synthetic_join_order.plan import (
    AUDIT_BEGIN,
    OUTPUT_END,
    PLAN_BEGIN,
    STATS_BEGIN,
    PlanDriverError,
    PlanEvidence,
    build_plan_script,
    parse_plan_output,
    postgres_server_version,
)
from synthetic_join_order.qualify import QualificationError, qualify_postgres
from tools import qualify_synthetic_join_order_pg as qualify_cli


class SyntheticJoinOrderPostgresTests(unittest.TestCase):
    def setUp(self) -> None:
        self.graph = generate_graph(
            GenerationSpec(n=2, graph_seed=7, cardinality_seed=1003)
        )
        self.case = compile_graph(self.graph)

    def evidence(self, *, total_cost: float = 10.0) -> PlanEvidence:
        edge = self.graph["edges"][0]
        root_rows = subset_cardinality(self.graph, (0, 1))
        scans = [
            {
                "Node Type": "Seq Scan",
                "Relation Name": f"r{node['id']:04d}",
                "Plan Rows": node["base_rows"],
                "Plan Width": node["payload_width"] + 8,
            }
            for node in self.graph["nodes"]
        ]
        plan = {
            "Node Type": "Hash Join",
            "Plan Rows": root_rows,
            "Total Cost": total_cost,
            "Plans": [scans[0], {"Node Type": "Hash", "Plans": [scans[1]]}],
        }
        pages = relation_page_counts(self.graph)
        audit = {
            "active": True,
            "instance_id": self.case.instance_id,
            "base": [
                {
                    "canonical_id": node["id"],
                    "base_rows": node["base_rows"],
                    "base_pages": pages[node["id"]],
                }
                for node in self.graph["nodes"]
            ],
            "join_count": 1,
            "hashjoin_count": 1,
            "final_rows": root_rows,
        }
        key_stats = tuple(
            {
                "relation": f"r{relation_id:04d}",
                "column": f"j_{edge['id']}",
                "null_frac": 0,
                "avg_width": 8,
                "n_distinct": edge_endpoint_key_ndv(edge, relation_id),
                "most_common_freqs": [
                    1.0 / edge_endpoint_key_ndv(edge, relation_id)
                ],
                "histogram_bounds": None,
            }
            for relation_id in (0, 1)
        )
        payload_stats = tuple(
            {
                "relation": f"r{node['id']:04d}",
                "column": "payload",
                "null_frac": 0,
                "avg_width": node["payload_width"],
                "n_distinct": -1,
                "most_common_freqs": None,
                "histogram_bounds": None,
            }
            for node in self.graph["nodes"]
        )
        return PlanEvidence(
            explain={"Plan": plan},
            audit=audit,
            stats=payload_stats + key_stats,
        )

    def test_plan_script_is_plan_only_and_applies_variant_last(self) -> None:
        script = build_plan_script(
            self.case,
            variant_gucs=(("geqo_threshold", 2), ("enable_example_join_search", "on")),
        )

        self.assertIn("EXPLAIN (ANALYZE FALSE", script)
        self.assertNotIn("SUMMARY", script)
        self.assertNotIn("SET enable_hashjoin", script)
        self.assertIn("SET enable_mergejoin = 'off'", script)
        self.assertLess(script.index("RESET ALL"), script.index("SET geqo_threshold = 2"))
        self.assertIn("SET exactcard.audit = summary", script)
        self.assertLess(script.index(PLAN_BEGIN), script.index(AUDIT_BEGIN))
        self.assertLess(script.index(AUDIT_BEGIN), script.index(STATS_BEGIN))
        self.assertLess(script.index(STATS_BEGIN), script.index(OUTPUT_END))

    def test_plan_output_requires_active_structured_evidence(self) -> None:
        output = (
            f"{PLAN_BEGIN}\n[{json.dumps({'Plan': {'Plan Rows': 1}})}]\n"
            f"{AUDIT_BEGIN}\n{json.dumps({'active': True})}\n"
            f"{STATS_BEGIN}\n[]\n{OUTPUT_END}\n"
        )

        self.assertEqual(parse_plan_output(output).explain["Plan"]["Plan Rows"], 1)

    def test_server_version_comes_from_the_connected_server(self) -> None:
        with patch("synthetic_join_order.plan._run_sql", return_value="17.5\n") as run_sql:
            self.assertEqual(postgres_server_version("db"), "17.5")

        run_sql.assert_called_once_with("db", "SHOW server_version;", None)

    def test_summary_qualification_checks_postgres_evidence(self) -> None:
        evidence = self.evidence(total_cost=12.5)
        report = qualify_postgres(self.graph, self.case, evidence)

        self.assertEqual(report.base_relation_count, 2)
        self.assertEqual(report.stats_record_count, 4)
        self.assertEqual(report.root_total_cost, 12.5)

        broken_plan = dict(evidence.explain["Plan"])
        broken_plan["Plan Rows"] += 10
        with self.assertRaises(QualificationError):
            qualify_postgres(
                self.graph,
                self.case,
                PlanEvidence({"Plan": broken_plan}, evidence.audit, evidence.stats),
            )

    def test_trace_qualification_checks_join_and_hash_records(self) -> None:
        evidence = self.evidence(total_cost=12.5)
        edge = self.graph["edges"][0]
        root_rows = subset_cardinality(self.graph, (0, 1))
        inner_key_ndv = edge_endpoint_key_ndv(edge, 1)
        selectivity = 1.0 / inner_key_ndv
        audit = {
            **evidence.audit,
            "joins": [{
                "relations": [0, 1],
                "outer": [0],
                "inner": [1],
                "crossing": [{
                    "left": 0,
                    "right": 1,
                    "left_key_ndv": edge["left_key_ndv"],
                    "right_key_ndv": edge["right_key_ndv"],
                    "selectivity": edge["selectivity"],
                }],
                "formula_rows": root_rows,
                "log_card": subset_log_cardinality(self.graph, (0, 1)),
            }],
            "hashjoins": [{
                "relations": [0, 1],
                "outer": [0],
                "inner": [1],
                "crossing_count": 1,
                "key_ndv": inner_key_ndv,
                "edge_selectivity": edge["selectivity"],
                "inner_endpoint": 1,
                "outer_rows": self.graph["nodes"][0]["base_rows"],
                "inner_rows": self.graph["nodes"][1]["base_rows"],
                "innerbucketsize": max(1.0 / 1024, selectivity),
                "innermcvfreq": selectivity,
                "numbuckets": 1024,
                "numbatches": 1,
                "bucket_stats_computed": True,
                "hashjointuples": root_rows,
            }],
        }

        report = qualify_postgres(
            self.graph,
            self.case,
            PlanEvidence(evidence.explain, audit, evidence.stats),
        )

        self.assertEqual(report.join_record_count, 1)
        self.assertEqual(report.hash_record_count, 1)

    def test_qualification_rejects_invalid_total_cost(self) -> None:
        evidence = self.evidence()

        for total_cost in (math.inf, 0.0):
            with self.subTest(total_cost=total_cost):
                broken_explain = dict(evidence.explain)
                broken_explain["Plan"] = dict(evidence.explain["Plan"])
                broken_explain["Plan"]["Total Cost"] = total_cost
                with self.assertRaises(QualificationError):
                    qualify_postgres(
                        self.graph,
                        self.case,
                        PlanEvidence(broken_explain, evidence.audit, evidence.stats),
                    )

    def test_single_case_qualification_cleans_up_its_install(self) -> None:
        args = SimpleNamespace(
            case=Path("case"),
            db="db",
            host=None,
            port=None,
            user=None,
            bootstrap_metadata=False,
            skip_install=False,
            statement_timeout_ms=60_000,
            audit="trace",
        )
        with (
            patch.object(qualify_cli, "parse_args", return_value=args),
            patch.object(qualify_cli, "load_graph", return_value=self.graph),
            patch.object(qualify_cli, "compile_graph", return_value=self.case),
            patch.object(qualify_cli, "verify_case"),
            patch.object(qualify_cli, "install_case"),
            patch.object(
                qualify_cli, "plan_case", side_effect=PlanDriverError("planner failed")
            ),
            patch.object(qualify_cli, "uninstall_case") as uninstall,
            redirect_stderr(StringIO()),
        ):
            self.assertEqual(qualify_cli.main(), 2)

        uninstall.assert_called_once()


if __name__ == "__main__":
    unittest.main()
