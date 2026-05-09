"""Artifact writers for benchmark run output.

This module turns in-memory run rows into ``raw.csv``, ``summary.csv``,
``plans/``, and ``run.json`` so execution code does not own file-format
details.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from bench_common import safe_artifact_name
from bench_config import select_queries


RAW_CSV_FIELDS = [
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
]

SUMMARY_METRIC_FIELDS = (
    ("planning_ms_median", "planning_ms"),
    ("execution_ms_median", "execution_ms"),
    ("total_ms_median", "total_ms"),
    ("plan_total_cost_median", "plan_total_cost"),
)
SUMMARY_CSV_FIELDS = [
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
]


# CSV artifact writers.


def write_raw_csv(raw_path: Path, raw_rows: list[dict[str, str]]) -> None:
    """Write per-repetition execution rows to ``raw.csv``.

    Extra keys are ignored so execution code can carry richer in-memory rows
    without changing the public CSV schema.
    """
    with raw_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=RAW_CSV_FIELDS,
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
    measured_reps: int,
    plans_dir: Path | None = None,
) -> None:
    """Write one representative metric/failure-count row per dataset/query/variant.

    Complete query/variant results use the successful repetition with median
    total time for all metrics.  Incomplete results keep only success and
    failure counts.
    """
    if plans_dir is not None:
        plans_dir.mkdir(parents=True, exist_ok=True)

    with summary_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=SUMMARY_CSV_FIELDS,
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
                    row = {
                        "dataset": spec.dataset,
                        "query_id": q.query_id,
                        "join_size": str(q.join_size),
                        "variant": variant_name,
                        "ok_reps": str(ok_reps),
                        "timeout_reps": str(timeout_reps),
                        "error_reps": str(error_reps),
                    }
                    complete_success = (
                        measured_reps > 0
                        and len(vals) == measured_reps
                        and ok_reps == measured_reps
                    )
                    if complete_success:
                        median_entry = _median_total_repetition(ok)
                        row.update(
                            {
                                summary_field: f"{float(median_entry[source_field]):.3f}"
                                for summary_field, source_field in SUMMARY_METRIC_FIELDS
                            }
                        )
                        if plans_dir is not None:
                            _write_plan_json(
                                plans_dir,
                                dataset=spec.dataset,
                                query_id=q.query_id,
                                variant_name=variant_name,
                                median_entry=median_entry,
                            )
                    else:
                        row.update({summary_field: "" for summary_field, _ in SUMMARY_METRIC_FIELDS})
                    writer.writerow(row)


def _median_total_repetition(ok_entries: list[dict[str, object]]) -> dict[str, object]:
    """Return the successful repetition with median total time."""
    ordered = sorted(
        ok_entries,
        key=lambda entry: (
            float(entry["total_ms"]),
            int(entry.get("rep", 0)),
        ),
    )
    return ordered[len(ordered) // 2]


def _write_plan_json(
    plans_dir: Path,
    *,
    dataset: str,
    query_id: str,
    variant_name: str,
    median_entry: dict[str, object],
) -> None:
    """Write the full EXPLAIN JSON for the summary row's measured repetition."""

    explain_json = median_entry.get("explain_json")
    if not isinstance(explain_json, str) or not explain_json:
        return

    plan_path = (
        plans_dir
        / safe_artifact_name(dataset)
        / safe_artifact_name(query_id)
        / f"{safe_artifact_name(variant_name)}.json"
    )
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(explain_json.rstrip() + "\n")


# run.json artifact helpers.


def build_run_context(
    *,
    run_id: str,
    scenario: Any,
    tag: str,
    run_session_gucs: tuple[tuple[str, Any], ...],
    measured_reps: int,
    warmup_runs: int,
    effective_variant_contexts: list[dict[str, Any]],
    dataset_contexts: list[dict[str, Any]],
    stats_refresh: str,
) -> dict[str, Any]:
    """Build the serializable ``run.json`` metadata for one benchmark run."""
    run_context = {
        "run_id": run_id,
        "scenario": scenario.name,
        "scenario_description": getattr(scenario, "description", ""),
        "session_gucs": [{k: v} for k, v in run_session_gucs],
        "protocol": {
            "measured_reps": measured_reps,
            "warmup_runs": warmup_runs,
            "timing": "off",
            "variant_order": "rotate_by_query_and_rep",
            "stats_refresh": stats_refresh,
        },
        "variants": effective_variant_contexts,
        "datasets": [
            {
                "dataset": entry["dataset"],
                "min_join": entry["min_join"],
                "variants": entry["variants"],
            }
            for entry in dataset_contexts
        ],
    }
    if tag:
        run_context["tag"] = tag
    return run_context


def write_run_context(path: Path, run_context: dict[str, Any]) -> None:
    """Write ``run.json`` with stable indentation and key ordering."""
    path.write_text(json.dumps(run_context, indent=2, sort_keys=True) + "\n")
