#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[3]
RESULTS_DIR = REPO_ROOT / "results"

DEFAULT_MANIFEST = SCRIPT_DIR / "default_run_manifest.csv"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR

ALGO_ORDER = ["dp", "geqo", "hs_rs", "hs_cost", "hs_combined"]
METRIC_PREFIXES = ["planning_ms", "execution_ms", "plan_total_cost"]

SUMMARY_FIELDNAMES = [
    "dataset",
    "algo",
    "source_mode",
    "queries_total",
    "queries_ok",
    "queries_err",
    "queries_partial_err",
    "planning_mean_ms",
    "planning_p50_ms",
    "planning_p90_ms",
    "planning_p95_ms",
    "planning_p99_ms",
    "planning_max_ms",
    "execution_mean_ms",
    "execution_p50_ms",
    "execution_p90_ms",
    "execution_p95_ms",
    "execution_p99_ms",
    "execution_max_ms",
    "total_mean_ms",
    "total_p50_ms",
    "total_p90_ms",
    "total_p95_ms",
    "total_p99_ms",
    "total_max_ms",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate the v6_4 split-hybrid benchmark matrix into summary CSV/Markdown views."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help=f"Manifest CSV to read (default: {DEFAULT_MANIFEST})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory where summary files are written (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=RESULTS_DIR,
        help=f"Benchmark results root containing <run_id>/summary.csv (default: {RESULTS_DIR})",
    )
    return parser.parse_args()


def float_or_none(value: str | None) -> float | None:
    if value in ("", None):
        return None
    return float(value)


def int_or_none(value: str | None) -> int | None:
    if value in ("", None):
        return None
    return int(value)


def format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.3f}"


def mean(values: Iterable[float]) -> float | None:
    vals = list(values)
    if not vals:
        return None
    return sum(vals) / len(vals)


def percentile(values: Iterable[float], p: float) -> float | None:
    vals = sorted(values)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    k = (len(vals) - 1) * p
    floor_idx = math.floor(k)
    ceil_idx = math.ceil(k)
    if floor_idx == ceil_idx:
        return vals[int(k)]
    lower = vals[floor_idx]
    upper = vals[ceil_idx]
    return lower * (ceil_idx - k) + upper * (k - floor_idx)


def natural_sort_key(text: str) -> list[int | str]:
    chunks: list[int | str] = []
    current = ""
    is_digit = False
    for ch in text:
        if ch.isdigit():
            if current and not is_digit:
                chunks.append(current)
                current = ""
            current += ch
            is_digit = True
        else:
            if current and is_digit:
                chunks.append(int(current))
                current = ""
            current += ch
            is_digit = False
    if current:
        chunks.append(int(current) if is_digit else current)
    return chunks


def load_manifest(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise SystemExit(f"missing manifest: {path}")
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    return rows


def load_summary_rows(manifest_rows: list[dict[str, str]], results_dir: Path) -> tuple[list[str], list[dict[str, object]]]:
    dataset_order: list[str] = []
    seen_datasets: set[str] = set()
    seen_query_keys: dict[tuple[str, str, str], str] = {}
    all_rows: list[dict[str, object]] = []

    for manifest_row in manifest_rows:
        dataset = manifest_row["dataset"]
        mode = manifest_row["mode"]
        run_id = manifest_row["run_id"]
        if dataset not in seen_datasets:
            dataset_order.append(dataset)
            seen_datasets.add(dataset)

        summary_path = results_dir / run_id / "summary.csv"
        if not summary_path.is_file():
            raise SystemExit(f"missing summary.csv for run {run_id}: {summary_path}")

        with summary_path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if row["dataset"] != dataset:
                    raise SystemExit(
                        f"manifest dataset {dataset} does not match summary row dataset {row['dataset']} in run {run_id}"
                    )
                key = (dataset, row["algo"], row["query_id"])
                if key in seen_query_keys:
                    prev_run = seen_query_keys[key]
                    raise SystemExit(
                        f"duplicate dataset/algo/query in manifest-backed summaries: {key} appears in {prev_run} and {run_id}"
                    )
                seen_query_keys[key] = run_id
                all_rows.append(
                    {
                        "dataset": dataset,
                        "algo": row["algo"],
                        "query_id": row["query_id"],
                        "query_path": row["query_path"],
                        "query_label": row.get("query_label", ""),
                        "join_size": int_or_none(row["join_size"]),
                        "planning_ms_min": float_or_none(row.get("planning_ms_min")),
                        "execution_ms_min": float_or_none(row.get("execution_ms_min")),
                        "total_ms_min": float_or_none(row.get("total_ms_min")),
                        "plan_total_cost_min": float_or_none(row.get("plan_total_cost_min")),
                        "ok_reps": int(row["ok_reps"]),
                        "err_reps": int(row["err_reps"]),
                        "source_run_id": run_id,
                        "source_mode": mode,
                    }
                )
    return dataset_order, all_rows


def algo_sort_key(algo: str) -> tuple[int, list[int | str]]:
    if algo in ALGO_ORDER:
        return (ALGO_ORDER.index(algo), natural_sort_key(algo))
    return (len(ALGO_ORDER), natural_sort_key(algo))


def aggregate_summary(dataset_order: list[str], rows: list[dict[str, object]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    source_modes: dict[tuple[str, str], set[str]] = defaultdict(set)

    for row in rows:
        key = (str(row["dataset"]), str(row["algo"]))
        grouped[key].append(row)
        source_modes[key].add(str(row["source_mode"]))

    summary_rows: list[dict[str, str]] = []
    for dataset in dataset_order:
        algos = sorted({algo for ds, algo in grouped if ds == dataset}, key=algo_sort_key)
        for algo in algos:
            key = (dataset, algo)
            algo_rows = grouped[key]
            modes = sorted(source_modes[key])
            if len(modes) != 1:
                raise SystemExit(f"expected a single source_mode for {dataset}/{algo}, found {modes}")

            ok_rows = [row for row in algo_rows if int(row["ok_reps"]) > 0]
            planning_vals = [float(row["planning_ms_min"]) for row in ok_rows if row["planning_ms_min"] is not None]
            execution_vals = [float(row["execution_ms_min"]) for row in ok_rows if row["execution_ms_min"] is not None]
            total_vals = [float(row["total_ms_min"]) for row in ok_rows if row["total_ms_min"] is not None]

            summary_rows.append(
                {
                    "dataset": dataset,
                    "algo": algo,
                    "source_mode": modes[0],
                    "queries_total": str(len(algo_rows)),
                    "queries_ok": str(sum(1 for row in algo_rows if int(row["ok_reps"]) > 0)),
                    "queries_err": str(
                        sum(1 for row in algo_rows if int(row["err_reps"]) > 0 and int(row["ok_reps"]) == 0)
                    ),
                    "queries_partial_err": str(
                        sum(1 for row in algo_rows if int(row["err_reps"]) > 0 and int(row["ok_reps"]) > 0)
                    ),
                    "planning_mean_ms": format_float(mean(planning_vals)),
                    "planning_p50_ms": format_float(percentile(planning_vals, 0.50)),
                    "planning_p90_ms": format_float(percentile(planning_vals, 0.90)),
                    "planning_p95_ms": format_float(percentile(planning_vals, 0.95)),
                    "planning_p99_ms": format_float(percentile(planning_vals, 0.99)),
                    "planning_max_ms": format_float(max(planning_vals) if planning_vals else None),
                    "execution_mean_ms": format_float(mean(execution_vals)),
                    "execution_p50_ms": format_float(percentile(execution_vals, 0.50)),
                    "execution_p90_ms": format_float(percentile(execution_vals, 0.90)),
                    "execution_p95_ms": format_float(percentile(execution_vals, 0.95)),
                    "execution_p99_ms": format_float(percentile(execution_vals, 0.99)),
                    "execution_max_ms": format_float(max(execution_vals) if execution_vals else None),
                    "total_mean_ms": format_float(mean(total_vals)),
                    "total_p50_ms": format_float(percentile(total_vals, 0.50)),
                    "total_p90_ms": format_float(percentile(total_vals, 0.90)),
                    "total_p95_ms": format_float(percentile(total_vals, 0.95)),
                    "total_p99_ms": format_float(percentile(total_vals, 0.99)),
                    "total_max_ms": format_float(max(total_vals) if total_vals else None),
                }
            )

    return summary_rows


def build_wide_rows(dataset: str, rows: list[dict[str, object]]) -> list[dict[str, str]]:
    dataset_rows = [row for row in rows if row["dataset"] == dataset]
    by_query: dict[str, dict[str, str]] = {}

    for row in dataset_rows:
        query_id = str(row["query_id"])
        current = by_query.setdefault(
            query_id,
            {
                "join_size": "" if row["join_size"] is None else str(row["join_size"]),
                "query_id": query_id,
                "query_path": str(row["query_path"]),
            },
        )
        join_size = "" if row["join_size"] is None else str(row["join_size"])
        if current["join_size"] != join_size or current["query_path"] != str(row["query_path"]):
            raise SystemExit(f"inconsistent query metadata for {dataset}/{query_id}")

        algo = str(row["algo"])
        current[f"{algo}_planning_ms"] = format_float(row["planning_ms_min"])
        current[f"{algo}_execution_ms"] = format_float(row["execution_ms_min"])
        current[f"{algo}_plan_total_cost"] = format_float(row["plan_total_cost_min"])

    wide_rows = []
    for query_id in sorted(by_query, key=natural_sort_key):
        row = dict(by_query[query_id])
        for algo in ALGO_ORDER:
            for metric in METRIC_PREFIXES:
                row.setdefault(f"{algo}_{metric}", "")
        wide_rows.append(row)
    return wide_rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def rel_to_repo(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def render_markdown_table(rows: list[dict[str, str]], columns: list[str]) -> list[str]:
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, divider]
    for row in rows:
        lines.append("| " + " | ".join(row.get(column, "") for column in columns) + " |")
    return lines


def write_current_summary(
    output_path: Path,
    manifest_path: Path,
    summary_csv_path: Path,
    job_wide_path: Path,
    job_complex_wide_path: Path,
    dataset_order: list[str],
    summary_rows: list[dict[str, str]],
) -> None:
    lines = ["# v6_4 Split-Hybrid Benchmark Summary", ""]
    lines.append(f"- Source manifest: `{rel_to_repo(manifest_path)}`")
    lines.append(f"- Aggregated stats: `{rel_to_repo(summary_csv_path)}`")
    lines.append(f"- JOB query-wide view: `{rel_to_repo(job_wide_path)}`")
    lines.append(f"- JOB-Complex query-wide view: `{rel_to_repo(job_complex_wide_path)}`")
    lines.append("")

    if not summary_rows:
        lines.append("- No manifest-backed summary rows were found yet.")
        lines.append("")
        output_path.write_text("\n".join(lines))
        return

    table_columns = [
        "algo",
        "source_mode",
        "queries_total",
        "queries_ok",
        "queries_err",
        "queries_partial_err",
        "planning_mean_ms",
        "execution_p50_ms",
        "execution_p90_ms",
        "execution_p95_ms",
        "execution_p99_ms",
        "execution_max_ms",
    ]
    rows_by_dataset: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in summary_rows:
        rows_by_dataset[row["dataset"]].append(row)

    for dataset in dataset_order:
        dataset_rows = rows_by_dataset.get(dataset)
        if not dataset_rows:
            continue
        dataset_rows = sorted(dataset_rows, key=lambda row: algo_sort_key(row["algo"]))
        lines.append(f"## {dataset}")
        lines.append("")
        lines.extend(render_markdown_table(dataset_rows, table_columns))
        lines.append("")

    output_path.write_text("\n".join(lines))


def main() -> None:
    args = parse_args()

    manifest_rows = load_manifest(args.manifest)
    dataset_order, rows = load_summary_rows(manifest_rows, args.results_dir)

    summary_rows = aggregate_summary(dataset_order, rows)
    summary_csv_path = args.output_dir / "summary_exec_planning.csv"
    write_csv(summary_csv_path, SUMMARY_FIELDNAMES, summary_rows)

    wide_fieldnames = ["join_size", "query_id", "query_path"]
    for algo in ALGO_ORDER:
        wide_fieldnames.extend(
            [
                f"{algo}_planning_ms",
                f"{algo}_execution_ms",
                f"{algo}_plan_total_cost",
            ]
        )

    job_wide_path = args.output_dir / "job_query_results_wide.csv"
    job_complex_wide_path = args.output_dir / "job_complex_query_results_wide.csv"
    write_csv(job_wide_path, wide_fieldnames, build_wide_rows("job", rows))
    write_csv(job_complex_wide_path, wide_fieldnames, build_wide_rows("job_complex", rows))

    write_current_summary(
        args.output_dir / "current_summary.md",
        args.manifest,
        summary_csv_path,
        job_wide_path,
        job_complex_wide_path,
        dataset_order,
        summary_rows,
    )


if __name__ == "__main__":
    main()
