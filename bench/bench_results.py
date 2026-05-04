from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any

from bench_workloads import select_queries


def write_raw_csv(raw_path: Path, raw_rows: list[dict[str, str]]) -> None:
    with raw_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "dataset",
                "query_id",
                "variant",
                "rep",
                "planning_ms",
                "execution_ms",
                "total_ms",
                "plan_total_cost",
                "status",
                "error",
            ],
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(raw_rows)


def write_summary_csv(
    summary_path: Path,
    *,
    resolved_runs: list[Any],
    summary_acc: dict[tuple[str, str, str], list[dict[str, object]]],
) -> None:
    with summary_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "dataset",
                "query_id",
                "join_size",
                "variant",
                "planning_ms_median",
                "execution_ms_median",
                "total_ms_median",
                "plan_total_cost_median",
                "ok_reps",
                "timeout_reps",
                "error_reps",
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
                    timeout_reps = sum(1 for entry in vals if entry["status"] == "timeout")
                    error_reps = sum(1 for entry in vals if entry["status"] == "error")
                    if ok:
                        planning_vals = [float(entry["planning_ms"]) for entry in ok]
                        execution_vals = [float(entry["execution_ms"]) for entry in ok]
                        total_vals = [float(entry["total_ms"]) for entry in ok]
                        cost_vals = [float(entry["plan_total_cost"]) for entry in ok]
                        row = {
                            "dataset": spec.dataset,
                            "query_id": q.query_id,
                            "join_size": str(q.join_size),
                            "variant": variant_name,
                            "planning_ms_median": f"{statistics.median(planning_vals):.3f}",
                            "execution_ms_median": f"{statistics.median(execution_vals):.3f}",
                            "total_ms_median": f"{statistics.median(total_vals):.3f}",
                            "plan_total_cost_median": f"{statistics.median(cost_vals):.3f}",
                            "ok_reps": str(ok_reps),
                            "timeout_reps": str(timeout_reps),
                            "error_reps": str(error_reps),
                        }
                    else:
                        row = {
                            "dataset": spec.dataset,
                            "query_id": q.query_id,
                            "join_size": str(q.join_size),
                            "variant": variant_name,
                            "planning_ms_median": "",
                            "execution_ms_median": "",
                            "total_ms_median": "",
                            "plan_total_cost_median": "",
                            "ok_reps": "0",
                            "timeout_reps": str(timeout_reps),
                            "error_reps": str(error_reps),
                        }
                    writer.writerow(row)


def build_run_context(
    *,
    run_id: str,
    scenario: Any,
    tag: str,
    statement_timeout_ms: int,
    measured_reps: int,
    warmup_runs: int,
    effective_variant_contexts: list[dict[str, Any]],
    dataset_contexts: list[dict[str, Any]],
) -> dict[str, Any]:
    run_context = {
        "run_id": run_id,
        "scenario": scenario.name,
        "scenario_description": getattr(scenario, "description", ""),
        "statement_timeout_ms": statement_timeout_ms,
        "protocol": {
            "measured_reps": measured_reps,
            "warmup_runs": warmup_runs,
            "timing": "off",
            "variant_order": "rotate_by_query_and_rep",
            "stats_refresh": "once_per_distinct_database_before_run",
        },
        "variants": effective_variant_contexts,
        "datasets": [
            {
                "dataset": entry["dataset"],
                "max_join": entry["max_join"],
                "variants": entry["variants"],
            }
            for entry in dataset_contexts
        ],
    }
    if tag:
        run_context["tag"] = tag
    return run_context


def write_run_context(path: Path, run_context: dict[str, Any]) -> None:
    path.write_text(json.dumps(run_context, indent=2, sort_keys=True) + "\n")
