from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any

from bench_catalog import select_queries


def write_raw_csv(raw_path: Path, raw_rows: list[dict[str, str]]) -> None:
    with raw_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run_id",
                "scenario",
                "dataset",
                "db",
                "variant",
                "query_id",
                "query_label",
                "query_path",
                "join_size",
                "variant_position",
                "rep",
                "planning_ms",
                "total_ms",
                "execution_ms",
                "execution_measurement_mode",
                "plan_total_cost",
                "status",
                "error",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(raw_rows)


def write_summary_csv(
    summary_path: Path,
    *,
    run_id: str,
    scenario_name: str,
    resolved_runs: list[Any],
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]],
) -> None:
    with summary_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run_id",
                "scenario",
                "dataset",
                "db",
                "variant",
                "query_id",
                "query_label",
                "query_path",
                "join_size",
                "planning_ms_median",
                "execution_ms_median",
                "total_ms_median",
                "plan_total_cost_median",
                "ok_reps",
                "err_reps",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        for spec in resolved_runs:
            queries = select_queries(spec)
            for q in queries:
                for variant_name in spec.variants:
                    key = (spec.dataset, q.query_id, variant_name)
                    vals = summary_acc.get(key, [])
                    ok = [entry for entry in vals if entry["status"] == "ok"]
                    ok_reps = len(ok)
                    err_reps = len(vals) - ok_reps
                    if ok:
                        planning_vals = [float(entry["planning_ms"]) for entry in ok]
                        execution_vals = [float(entry["execution_ms"]) for entry in ok]
                        total_vals = [float(entry["total_ms"]) for entry in ok]
                        cost_vals = [float(entry["plan_total_cost"]) for entry in ok]
                        row = {
                            "run_id": run_id,
                            "scenario": scenario_name,
                            "dataset": spec.dataset,
                            "db": spec.db,
                            "variant": variant_name,
                            "query_id": q.query_id,
                            "query_label": q.query_label,
                            "query_path": q.query_path,
                            "join_size": str(q.join_size),
                            "planning_ms_median": f"{statistics.median(planning_vals):.3f}",
                            "execution_ms_median": f"{statistics.median(execution_vals):.3f}",
                            "total_ms_median": f"{statistics.median(total_vals):.3f}",
                            "plan_total_cost_median": f"{statistics.median(cost_vals):.3f}",
                            "ok_reps": str(ok_reps),
                            "err_reps": str(err_reps),
                        }
                    else:
                        row = {
                            "run_id": run_id,
                            "scenario": scenario_name,
                            "dataset": spec.dataset,
                            "db": spec.db,
                            "variant": variant_name,
                            "query_id": q.query_id,
                            "query_label": q.query_label,
                            "query_path": q.query_path,
                            "join_size": str(q.join_size),
                            "planning_ms_median": "",
                            "execution_ms_median": "",
                            "total_ms_median": "",
                            "plan_total_cost_median": "",
                            "ok_reps": "0",
                            "err_reps": str(err_reps),
                        }
                    writer.writerow(row)


def build_run_context(
    *,
    run_id: str,
    scenario: Any,
    tag: str,
    reps: int,
    statement_timeout_ms: int,
    stabilize: str,
    warmup_runs: int,
    effective_variant_contexts: list[dict[str, Any]],
    query_counts: list[dict[str, Any]],
) -> dict[str, Any]:
    run_context = {
        "run_id": run_id,
        "scenario": scenario.name,
        "scenario_description": getattr(scenario, "description", ""),
        "protocol": {
            "reps": reps,
            "statement_timeout_ms": statement_timeout_ms,
            "stabilize": stabilize,
            "warmup_runs": warmup_runs,
            "warmup_timeout_policy": "skip_later_measured_repetitions",
            "warmup_scope": "query_group_discarded_pass",
            "measurement_lane": "EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)",
            "session_gucs": [{k: v} for k, v in scenario.session_gucs],
        },
        "variants": effective_variant_contexts,
        "datasets": [
            {
                "dataset": entry["dataset"],
                "max_join": entry["max_join"],
                "variants": entry["variants"],
            }
            for entry in query_counts
        ],
    }
    if tag:
        run_context["tag"] = tag
    return run_context


def write_run_context(path: Path, run_context: dict[str, Any]) -> None:
    path.write_text(json.dumps(run_context, indent=2, sort_keys=True) + "\n")
