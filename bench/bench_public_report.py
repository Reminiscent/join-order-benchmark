from __future__ import annotations

import csv
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PRIMARY_REFERENCE_VARIANT = "dp"
RATIO_FLOOR_MS = 1.0
P99_MIN_COMPARABLE = 200
EQUIVALENCE_LOW = 0.95
EQUIVALENCE_HIGH = 1.05
TAIL_THRESHOLDS = (1.10, 1.25, 1.50, 2.00, 5.00)
TOP_REGRESSION_COUNT = 3

PUBLIC_LABELS = {
    "dp": "DP",
    "geqo": "GEQO",
    "goo_cost": "GOO(cost)",
    "goo_result_size": "GOO(result_size)",
    "goo_selectivity": "GOO(selectivity)",
    "goo_combined": "GOO(combined)",
    "hybrid_search": "Hybrid Search",
}


@dataclass(frozen=True)
class MetricSpec:
    key: str
    column: str
    title: str
    role: str
    description: str
    show_top_regressions: bool


@dataclass(frozen=True)
class SummaryRow:
    dataset: str
    variant: str
    query_id: str
    query_label: str
    query_path: str
    join_size: int
    ok_reps: int
    err_reps: int
    planning_ms_median: float | None
    execution_ms_median: float | None
    total_ms_median: float | None
    plan_total_cost_median: float | None

    def metric_value(self, column: str) -> float | None:
        return getattr(self, column)


METRIC_SPECS = (
    MetricSpec(
        key="execution",
        column="execution_ms_median",
        title="Execution Time",
        role="primary",
        description=(
            "Primary replacement metric. Ratios compare per-query execution time against the "
            "reference variant using `execution_ms_median` from `summary.csv`."
        ),
        show_top_regressions=True,
    ),
    MetricSpec(
        key="planning",
        column="planning_ms_median",
        title="Planning Time",
        role="diagnostic",
        description=(
            "Planner-overhead diagnostic. Ratios compare per-query planning time against the "
            "reference variant using `planning_ms_median` from `summary.csv`."
        ),
        show_top_regressions=False,
    ),
)


def maybe_float(raw: str) -> float | None:
    text = raw.strip()
    if not text:
        return None
    return float(text)


def public_label(variant: str, label_by_name: dict[str, str]) -> str:
    return PUBLIC_LABELS.get(variant, label_by_name.get(variant, variant))


def dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def nearest_rank(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    idx = max(0, math.ceil((percentile / 100.0) * len(ordered)) - 1)
    return ordered[idx]


def ratio_with_floor(algo_ms: float, ref_ms: float, floor_ms: float) -> float:
    return (algo_ms + floor_ms) / (ref_ms + floor_ms)


def format_float(value: float | None, digits: int = 3) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def format_ratio(value: float | None) -> str:
    return format_float(value, digits=3)


def format_ratio_tail(value: float | None) -> str:
    return format_float(value, digits=2)


def format_percent(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value * 100.0:.1f}%"


def render_table(headers: list[str], rows: list[list[str]], right_align: set[int]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt_row(row: list[str]) -> str:
        cells: list[str] = []
        for idx, cell in enumerate(row):
            if idx in right_align:
                cells.append(cell.rjust(widths[idx]))
            else:
                cells.append(cell.ljust(widths[idx]))
        return " ".join(cells)

    sep = " ".join("-" * width for width in widths)
    return "\n".join([fmt_row(headers), sep, *(fmt_row(row) for row in rows)])


def load_summary_rows(summary_path: Path) -> tuple[dict[str, dict[str, dict[str, SummaryRow]]], dict[str, list[str]]]:
    rows: dict[str, dict[str, dict[str, SummaryRow]]] = {}
    query_order: dict[str, list[str]] = {}
    with summary_path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "dataset",
            "variant",
            "query_id",
            "query_label",
            "query_path",
            "join_size",
            "ok_reps",
            "err_reps",
            "planning_ms_median",
            "execution_ms_median",
            "total_ms_median",
            "plan_total_cost_median",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(f"missing required columns in {summary_path}: {', '.join(sorted(missing))}")

        seen_query_ids: dict[str, set[str]] = {}
        for raw in reader:
            row = SummaryRow(
                dataset=raw["dataset"],
                variant=raw["variant"],
                query_id=raw["query_id"],
                query_label=raw["query_label"],
                query_path=raw["query_path"],
                join_size=int(raw["join_size"]),
                ok_reps=int(raw["ok_reps"] or "0"),
                err_reps=int(raw["err_reps"] or "0"),
                planning_ms_median=maybe_float(raw["planning_ms_median"]),
                execution_ms_median=maybe_float(raw["execution_ms_median"]),
                total_ms_median=maybe_float(raw["total_ms_median"]),
                plan_total_cost_median=maybe_float(raw["plan_total_cost_median"]),
            )
            rows.setdefault(row.dataset, {}).setdefault(row.variant, {})[row.query_id] = row
            seen_query_ids.setdefault(row.dataset, set())
            query_order.setdefault(row.dataset, [])
            if row.query_id not in seen_query_ids[row.dataset]:
                seen_query_ids[row.dataset].add(row.query_id)
                query_order[row.dataset].append(row.query_id)
    return rows, query_order


def resolve_reference_variant(variant_order: list[str]) -> str:
    if PRIMARY_REFERENCE_VARIANT in variant_order:
        return PRIMARY_REFERENCE_VARIANT
    if not variant_order:
        raise SystemExit("public report requires at least one variant")
    return variant_order[0]


def metric_available(row: SummaryRow | None, metric: MetricSpec) -> bool:
    return row is not None and row.ok_reps > 0 and row.metric_value(metric.column) is not None


def comparable_query_ids(
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variant: str,
    reference: str,
    metric: MetricSpec,
) -> list[str]:
    comparable: list[str] = []
    for query_id in query_ids:
        row = dataset_rows.get(variant, {}).get(query_id)
        ref_row = dataset_rows.get(reference, {}).get(query_id)
        if metric_available(row, metric) and metric_available(ref_row, metric):
            comparable.append(query_id)
    return comparable


def coverage_rows_for_metric(
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variants: list[str],
    reference: str,
    metric: MetricSpec,
    label_by_name: dict[str, str],
) -> list[dict[str, Any]]:
    total_queries = len(query_ids)
    ref_query_ids = {
        query_id
        for query_id in query_ids
        if metric_available(dataset_rows.get(reference, {}).get(query_id), metric)
    }
    out: list[dict[str, Any]] = []
    for variant in variants:
        ok_query_ids = {
            query_id
            for query_id in query_ids
            if metric_available(dataset_rows.get(variant, {}).get(query_id), metric)
        }
        comparable = len(ok_query_ids & ref_query_ids)
        out.append(
            {
                "variant": variant,
                "label": public_label(variant, label_by_name),
                "ok_queries": len(ok_query_ids),
                "missing_queries": total_queries - len(ok_query_ids),
                "comparable_queries": comparable,
                "full_coverage": len(ok_query_ids) == total_queries,
            }
        )
    return out


def ratio_rows_for_metric(
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variants: list[str],
    reference: str,
    metric: MetricSpec,
    label_by_name: dict[str, str],
    ratio_floor_ms: float,
) -> tuple[list[dict[str, Any]], bool]:
    out: list[dict[str, Any]] = []
    any_p99 = False
    for variant in variants:
        if variant == reference:
            continue
        comparable = comparable_query_ids(dataset_rows, query_ids, variant, reference, metric)
        ratios: list[float] = []
        for query_id in comparable:
            row = dataset_rows[variant][query_id]
            ref_row = dataset_rows[reference][query_id]
            assert row.metric_value(metric.column) is not None
            assert ref_row.metric_value(metric.column) is not None
            ratios.append(
                ratio_with_floor(
                    row.metric_value(metric.column) or 0.0,
                    ref_row.metric_value(metric.column) or 0.0,
                    ratio_floor_ms,
                )
            )
        if not ratios:
            out.append(
                {
                    "variant": variant,
                    "label": public_label(variant, label_by_name),
                    "n": 0,
                    "wins": 0,
                    "within_5pct": 0,
                    "slower_5pct": 0,
                    "gmean_ratio": None,
                    "mean_ratio": None,
                    "p50_ratio": None,
                    "p90_ratio": None,
                    "p95_ratio": None,
                    "p99_ratio": None,
                    "max_ratio": None,
                }
            )
            continue

        p99_ratio: float | None = None
        if len(ratios) >= P99_MIN_COMPARABLE:
            p99_ratio = nearest_rank(ratios, 99)
            any_p99 = True

        out.append(
            {
                "variant": variant,
                "label": public_label(variant, label_by_name),
                "n": len(ratios),
                "wins": sum(1 for ratio in ratios if ratio < EQUIVALENCE_LOW),
                "within_5pct": sum(1 for ratio in ratios if EQUIVALENCE_LOW <= ratio <= EQUIVALENCE_HIGH),
                "slower_5pct": sum(1 for ratio in ratios if ratio > EQUIVALENCE_HIGH),
                "gmean_ratio": statistics.geometric_mean(ratios),
                "mean_ratio": statistics.fmean(ratios),
                "p50_ratio": nearest_rank(ratios, 50),
                "p90_ratio": nearest_rank(ratios, 90),
                "p95_ratio": nearest_rank(ratios, 95),
                "p99_ratio": p99_ratio,
                "max_ratio": max(ratios),
            }
        )
    return out, any_p99


def tail_rows_for_metric(
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variants: list[str],
    reference: str,
    metric: MetricSpec,
    label_by_name: dict[str, str],
    ratio_floor_ms: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for variant in variants:
        if variant == reference:
            continue
        comparable = comparable_query_ids(dataset_rows, query_ids, variant, reference, metric)
        ratios: list[float] = []
        for query_id in comparable:
            row = dataset_rows[variant][query_id]
            ref_row = dataset_rows[reference][query_id]
            assert row.metric_value(metric.column) is not None
            assert ref_row.metric_value(metric.column) is not None
            ratios.append(
                ratio_with_floor(
                    row.metric_value(metric.column) or 0.0,
                    ref_row.metric_value(metric.column) or 0.0,
                    ratio_floor_ms,
                )
            )
        out.append(
            {
                "variant": variant,
                "label": public_label(variant, label_by_name),
                "counts": {f">={threshold:.2f}x": sum(1 for ratio in ratios if ratio >= threshold) for threshold in TAIL_THRESHOLDS},
            }
        )
    return out


def workload_rows_for_metric(
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variants: list[str],
    reference: str,
    metric: MetricSpec,
    label_by_name: dict[str, str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    ref_available = {
        query_id
        for query_id in query_ids
        if metric_available(dataset_rows.get(reference, {}).get(query_id), metric)
    }
    ref_total_all = sum(
        dataset_rows[reference][query_id].metric_value(metric.column) or 0.0
        for query_id in query_ids
        if query_id in ref_available
    )
    out.append(
        {
            "variant": reference,
            "label": public_label(reference, label_by_name),
            "n": len(ref_available),
            "total_ms": ref_total_all,
            "ratio_to_reference": 1.0 if ref_available else None,
            "full_coverage": len(ref_available) == len(query_ids),
        }
    )
    for variant in variants:
        if variant == reference:
            continue
        comparable = comparable_query_ids(dataset_rows, query_ids, variant, reference, metric)
        algo_total = sum(dataset_rows[variant][query_id].metric_value(metric.column) or 0.0 for query_id in comparable)
        ref_total = sum(dataset_rows[reference][query_id].metric_value(metric.column) or 0.0 for query_id in comparable)
        out.append(
            {
                "variant": variant,
                "label": public_label(variant, label_by_name),
                "n": len(comparable),
                "total_ms": algo_total if comparable else None,
                "ratio_to_reference": (algo_total / ref_total) if comparable and ref_total > 0 else None,
                "full_coverage": len(comparable) == len(query_ids),
            }
        )
    return out


def top_regression_rows_for_metric(
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variants: list[str],
    reference: str,
    metric: MetricSpec,
    label_by_name: dict[str, str],
    ratio_floor_ms: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not metric.show_top_regressions:
        return out
    for variant in variants:
        if variant == reference:
            continue
        comparable = comparable_query_ids(dataset_rows, query_ids, variant, reference, metric)
        ranked: list[tuple[float, SummaryRow, SummaryRow]] = []
        for query_id in comparable:
            row = dataset_rows[variant][query_id]
            ref_row = dataset_rows[reference][query_id]
            assert row.metric_value(metric.column) is not None
            assert ref_row.metric_value(metric.column) is not None
            ranked.append(
                (
                    ratio_with_floor(
                        row.metric_value(metric.column) or 0.0,
                        ref_row.metric_value(metric.column) or 0.0,
                        ratio_floor_ms,
                    ),
                    row,
                    ref_row,
                )
            )
        ranked.sort(key=lambda item: (item[0], item[1].metric_value(metric.column) or 0.0), reverse=True)
        for ratio, row, ref_row in ranked[:TOP_REGRESSION_COUNT]:
            out.append(
                {
                    "variant": variant,
                    "label": public_label(variant, label_by_name),
                    "query_id": row.query_id,
                    "query_label": row.query_label,
                    "query_path": row.query_path,
                    "ratio": ratio,
                    "algo_ms": row.metric_value(metric.column),
                    "reference_ms": ref_row.metric_value(metric.column),
                }
            )
    return out


def planning_share_rows(
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variants: list[str],
    label_by_name: dict[str, str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for variant in variants:
        usable = [
            dataset_rows[variant][query_id]
            for query_id in query_ids
            if metric_available(dataset_rows.get(variant, {}).get(query_id), METRIC_SPECS[0])
            and metric_available(dataset_rows.get(variant, {}).get(query_id), METRIC_SPECS[1])
        ]
        planning_total = sum(row.planning_ms_median or 0.0 for row in usable)
        execution_total = sum(row.execution_ms_median or 0.0 for row in usable)
        denom = planning_total + execution_total
        out.append(
            {
                "variant": variant,
                "label": public_label(variant, label_by_name),
                "n": len(usable),
                "planning_ms": planning_total if usable else None,
                "execution_ms": execution_total if usable else None,
                "planning_share": (planning_total / denom) if usable and denom > 0 else None,
            }
        )
    return out


def build_dataset_section(
    dataset: str,
    dataset_rows: dict[str, dict[str, SummaryRow]],
    query_ids: list[str],
    variants: list[str],
    reference: str,
    label_by_name: dict[str, str],
    ratio_floor_ms: float,
) -> dict[str, Any]:
    metrics: list[dict[str, Any]] = []
    for metric in METRIC_SPECS:
        coverage_rows = coverage_rows_for_metric(dataset_rows, query_ids, variants, reference, metric, label_by_name)
        ratio_rows, has_p99 = ratio_rows_for_metric(
            dataset_rows,
            query_ids,
            variants,
            reference,
            metric,
            label_by_name,
            ratio_floor_ms,
        )
        tail_rows = tail_rows_for_metric(dataset_rows, query_ids, variants, reference, metric, label_by_name, ratio_floor_ms)
        workload_rows = workload_rows_for_metric(dataset_rows, query_ids, variants, reference, metric, label_by_name)
        top_regressions = top_regression_rows_for_metric(
            dataset_rows,
            query_ids,
            variants,
            reference,
            metric,
            label_by_name,
            ratio_floor_ms,
        )
        metrics.append(
            {
                "key": metric.key,
                "title": metric.title,
                "column": metric.column,
                "role": metric.role,
                "description": metric.description,
                "show_p99": has_p99,
                "coverage_rows": coverage_rows,
                "ratio_rows": ratio_rows,
                "tail_rows": tail_rows,
                "workload_rows": workload_rows,
                "top_regressions": top_regressions,
            }
        )
    return {
        "dataset": dataset,
        "total_queries": len(query_ids),
        "reference_variant": reference,
        "planning_share_rows": planning_share_rows(dataset_rows, query_ids, variants, label_by_name),
        "metrics": metrics,
    }


def build_public_report_bundle(
    *,
    run_context: dict[str, Any],
    summary_path: Path,
    ratio_floor_ms: float = RATIO_FLOOR_MS,
) -> dict[str, Any]:
    rows_by_dataset, query_order = load_summary_rows(summary_path)
    dataset_order = dedupe_preserve([str(entry["dataset"]) for entry in run_context.get("datasets", []) if entry.get("dataset")])
    if not dataset_order:
        dataset_order = sorted(rows_by_dataset)

    variant_order = dedupe_preserve([str(entry["name"]) for entry in run_context.get("variants", []) if entry.get("name")])
    if not variant_order:
        variant_order = sorted({variant for dataset_rows in rows_by_dataset.values() for variant in dataset_rows})

    reference = resolve_reference_variant(variant_order)
    label_by_name = {
        str(entry.get("name")): str(entry.get("label") or entry.get("name"))
        for entry in run_context.get("variants", [])
        if entry.get("name")
    }

    dataset_sections: list[dict[str, Any]] = []
    for dataset in dataset_order:
        if dataset not in rows_by_dataset:
            continue
        dataset_sections.append(
            build_dataset_section(
                dataset=dataset,
                dataset_rows=rows_by_dataset[dataset],
                query_ids=query_order[dataset],
                variants=[variant for variant in variant_order if variant in rows_by_dataset[dataset]],
                reference=reference,
                label_by_name=label_by_name,
                ratio_floor_ms=ratio_floor_ms,
            )
        )

    measurement_lane = str(run_context.get("protocol", {}).get("measurement_lane", "")).strip()
    if not measurement_lane:
        measurement_lane = "EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)"
    warmup_runs = int(run_context.get("protocol", {}).get("warmup_runs", 0) or 0)
    measured_reps = int(run_context.get("protocol", {}).get("reps", 0) or 0)
    warmup_scope = str(run_context.get("protocol", {}).get("warmup_scope", "")).strip()

    notes = [
        "Execution time is the primary replacement metric; planning time is reported separately as a diagnostic.",
        f"Metrics come from PostgreSQL backend phase times measured via `{measurement_lane}`.",
        "These numbers are not client end-to-end latencies; they exclude parse/rewrite, result formatting, and network transfer.",
        f"Per-query ratios use additive smoothing: (algo_ms + {ratio_floor_ms:.1f}) / (reference_ms + {ratio_floor_ms:.1f}).",
        f"P99 is only shown when there are at least {P99_MIN_COMPARABLE} comparable queries.",
        "Workload-total rows sum per-query aggregated metric values on comparable queries only; coverage is reported separately.",
    ]
    if warmup_runs or measured_reps:
        if warmup_scope == "query_group_discarded_pass":
            warmup_note = (
                f"Each query group used {warmup_runs} discarded warmup pass(es) "
                f"before {measured_reps} measured repetition(s)."
            )
        else:
            warmup_note = (
                f"Each run used {warmup_runs} discarded full-workload warmup pass(es) "
                f"before {measured_reps} measured repetition(s)."
            )
        notes.insert(
            2,
            warmup_note,
        )

    return {
        "report_version": 2,
        "run_id": run_context.get("run_id", ""),
        "scenario": run_context.get("scenario", ""),
        "scenario_description": run_context.get("scenario_description", ""),
        "started_at": run_context.get("started_at", ""),
        "finished_at": run_context.get("finished_at", ""),
        "reference_variant": reference,
        "reference_label": public_label(reference, label_by_name),
        "ratio_floor_ms": ratio_floor_ms,
        "variants": [
            {
                "name": variant,
                "label": public_label(variant, label_by_name),
            }
            for variant in variant_order
        ],
        "datasets": dataset_sections,
        "notes": notes,
    }


def markdown_lines_for_metric(metric: dict[str, Any], reference: str) -> list[str]:
    lines = [
        f"### {metric['title']}",
        "",
        metric["description"],
        "",
    ]
    reference_workload_row = next((row for row in metric["workload_rows"] if row["variant"] == reference), None)
    if reference_workload_row is not None and (reference_workload_row["total_ms"] or 0.0) <= 0.0:
        lines.extend(
            [
                "This metric is at or below the current timing resolution for the reference variant on this dataset; ratio summaries are floor-stabilized and should be treated as non-informative.",
                "",
            ]
        )
    lines.extend(["Coverage", "", "```text"])
    coverage_table = render_table(
        ["algo", "ok", "missing", f"comparable_to_{reference}", "full_coverage"],
        [
            [
                row["label"],
                str(row["ok_queries"]),
                str(row["missing_queries"]),
                str(row["comparable_queries"]),
                "yes" if row["full_coverage"] else "no",
            ]
            for row in metric["coverage_rows"]
        ],
        right_align={1, 2, 3},
    )
    lines.extend([coverage_table, "```", "", "Ratio Summary", "", "```text"])

    ratio_headers = ["algo", "n", "wins", "within_5%", "slower_5%", "gmean", "mean", "p50", "p90", "p95"]
    if metric["show_p99"]:
        ratio_headers.append("p99")
    ratio_headers.append("max")
    ratio_rows: list[list[str]] = []
    for row in metric["ratio_rows"]:
        cells = [
            row["label"],
            str(row["n"]),
            str(row["wins"]),
            str(row["within_5pct"]),
            str(row["slower_5pct"]),
            format_ratio(row["gmean_ratio"]),
            format_ratio(row["mean_ratio"]),
            format_ratio_tail(row["p50_ratio"]),
            format_ratio_tail(row["p90_ratio"]),
            format_ratio_tail(row["p95_ratio"]),
        ]
        if metric["show_p99"]:
            cells.append(format_ratio_tail(row["p99_ratio"]))
        cells.append(format_ratio_tail(row["max_ratio"]))
        ratio_rows.append(cells)
    lines.extend(
        [
            render_table(ratio_headers, ratio_rows, right_align=set(range(1, len(ratio_headers)))),
            "```",
            "",
            "Tail Counts",
            "",
            "```text",
        ]
    )

    tail_headers = ["algo", *[f">={threshold:.2f}x" for threshold in TAIL_THRESHOLDS]]
    tail_rows = [
        [row["label"], *[str(row["counts"][f">={threshold:.2f}x"]) for threshold in TAIL_THRESHOLDS]]
        for row in metric["tail_rows"]
    ]
    lines.extend(
        [
            render_table(tail_headers, tail_rows, right_align=set(range(1, len(tail_headers)))),
            "```",
            "",
            "Workload Totals",
            "",
            "```text",
        ]
    )

    workload_table = render_table(
        ["algo", "n", "total_ms", f"ratio_to_{reference}", "full_coverage"],
        [
            [
                row["label"],
                str(row["n"]),
                format_float(row["total_ms"], digits=2),
                format_ratio(row["ratio_to_reference"]),
                "yes" if row["full_coverage"] else "no",
            ]
            for row in metric["workload_rows"]
        ],
        right_align={1, 2, 3},
    )
    lines.extend([workload_table, "```"])

    if metric["top_regressions"]:
        lines.extend(["", "Worst Queries", "", "```text"])
        regression_table = render_table(
            ["algo", "query_id", "query_label", "ratio", "algo_ms", f"{reference}_ms"],
            [
                [
                    row["label"],
                    row["query_id"],
                    row["query_label"] or "-",
                    format_ratio_tail(row["ratio"]),
                    format_float(row["algo_ms"], digits=2),
                    format_float(row["reference_ms"], digits=2),
                ]
                for row in metric["top_regressions"]
            ],
            right_align={3, 4, 5},
        )
        lines.extend([regression_table, "```"])

    return lines


def render_public_report_markdown(bundle: dict[str, Any]) -> str:
    lines = [
        "# Public Benchmark Report",
        "",
        f"Run `{bundle['run_id']}` for scenario `{bundle['scenario']}`.",
        "",
        f"Reference variant: `{bundle['reference_variant']}` ({bundle['reference_label']}).",
        "",
        "Notes:",
    ]
    for note in bundle["notes"]:
        lines.append(f"- {note}")

    for dataset in bundle["datasets"]:
        lines.extend(
            [
                "",
                f"## Dataset `{dataset['dataset']}`",
                "",
                f"Total queries: {dataset['total_queries']}.",
            ]
        )
        for metric in dataset["metrics"]:
            lines.extend(["", *markdown_lines_for_metric(metric, bundle["reference_variant"])])

        lines.extend(["", "### Planning Share", "", "```text"])
        share_table = render_table(
            ["algo", "n", "planning_ms", "execution_ms", "planning_share"],
            [
                [
                    row["label"],
                    str(row["n"]),
                    format_float(row["planning_ms"], digits=2),
                    format_float(row["execution_ms"], digits=2),
                    format_percent(row["planning_share"]),
                ]
                for row in dataset["planning_share_rows"]
            ],
            right_align={1, 2, 3, 4},
        )
        lines.extend([share_table, "```"])

    return "\n".join(lines) + "\n"


def write_public_reports(
    *,
    run_context: dict[str, Any],
    summary_path: Path,
    markdown_path: Path,
    json_path: Path,
    ratio_floor_ms: float = RATIO_FLOOR_MS,
) -> dict[str, Any]:
    bundle = build_public_report_bundle(
        run_context=run_context,
        summary_path=summary_path,
        ratio_floor_ms=ratio_floor_ms,
    )
    markdown_path.write_text(render_public_report_markdown(bundle))
    json_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n")
    return bundle


def rerender_public_reports_for_run_dir(run_dir: Path, *, ratio_floor_ms: float = RATIO_FLOOR_MS) -> tuple[Path, Path]:
    run_json = run_dir / "run.json"
    summary_csv = run_dir / "summary.csv"
    if not run_json.is_file():
        raise SystemExit(f"missing run context: {run_json}")
    if not summary_csv.is_file():
        raise SystemExit(f"missing summary csv: {summary_csv}")

    run_context = json.loads(run_json.read_text())
    markdown_path = run_dir / "public_report.md"
    json_path = run_dir / "public_report.json"
    write_public_reports(
        run_context=run_context,
        summary_path=summary_csv,
        markdown_path=markdown_path,
        json_path=json_path,
        ratio_floor_ms=ratio_floor_ms,
    )
    return markdown_path, json_path
