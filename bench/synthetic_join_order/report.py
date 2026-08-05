"""Aggregate matrix outputs into reader-facing Total Cost quality.

Quality percentiles use only instances where every algorithm scheduled at a
relation count succeeded. Timeouts and errors remain visible in the separate
coverage view. ``render_matrix_report()`` reads durable matrix artifacts and
does not rerun PostgreSQL or modify benchmark results.
"""

from __future__ import annotations

import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any, Mapping, Sequence


def _nearest_rank(values: Sequence[float], percentile: float) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile of no values")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _quality_metrics(values: Sequence[float]) -> dict[str, float | str]:
    if not values:
        return {
            "p50_q": "",
            "p95_q": "",
            "max_q": "",
        }
    return {
        "p50_q": statistics.median(values),
        "p95_q": _nearest_rank(values, 0.95),
        "max_q": max(values),
    }


def build_matrix_reports(
    rows: Sequence[Mapping[str, Any]],
    sizes: Sequence[int],
    variants: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build common-success quality and full-population coverage views."""
    instance_ids = {
        size: {str(row["instance_id"]) for row in rows if int(row["n"]) == size}
        for size in sizes
    }
    quality_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    for size in sizes:
        rows_at_size = [row for row in rows if int(row["n"]) == size]
        scheduled_variants = tuple(
            variant
            for variant in variants
            if any(row["variant"] == variant for row in rows_at_size)
        )
        common_successes = set(instance_ids[size])
        for variant in scheduled_variants:
            common_successes &= {
                str(row["instance_id"])
                for row in rows_at_size
                if row["variant"] == variant and row["status"] == "ok"
            }
        for variant in variants:
            selected = [
                row for row in rows_at_size if row["variant"] == variant
            ]
            counts = {
                status: sum(row["status"] == status for row in selected)
                for status in ("ok", "timeout", "error")
            }
            total = len(instance_ids[size])
            scheduled = len(selected)
            coverage_rows.append(
                {
                    "n": size,
                    "variant": variant,
                    "total_instances": total,
                    "scheduled": scheduled,
                    "not_scheduled": total - scheduled,
                    "planned": counts["ok"],
                    "timeout": counts["timeout"],
                    "error": counts["error"],
                    "planned_rate": counts["ok"] / scheduled if scheduled else "",
                }
            )
            quality_values = [
                float(row["quality_ratio"])
                for row in selected
                if str(row["instance_id"]) in common_successes
                and row["status"] == "ok"
                and row["quality_ratio"] != ""
            ]
            quality_rows.append(
                {
                    "n": size,
                    "variant": variant,
                    "quality_samples": len(quality_values),
                    **_quality_metrics(quality_values),
                }
            )
    return quality_rows, coverage_rows


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ValueError(f"missing report input: {path}")
    with path.open(newline="") as file:
        return list(csv.DictReader(file))


def _q(value: str | float | None) -> str:
    if value in ("", None):
        return "N/A"
    return f"{float(value):.3f}"


def _append_quality_table(
    lines: list[str],
    title: str,
    field: str,
    sizes: Sequence[int],
    variants: Sequence[str],
    quality_by_key: Mapping[tuple[int, str], Mapping[str, Any]],
) -> None:
    lines.extend(
        [
            "",
            f"### {title}",
            "",
            "| n | " + " | ".join(f"`{variant}`" for variant in variants) + " |",
            "| ---: | " + " | ".join("---:" for _ in variants) + " |",
        ]
    )
    for size in sizes:
        values = [
            _q(quality_by_key[(size, variant)][field]) for variant in variants
        ]
        lines.append(f"| {size} | " + " | ".join(values) + " |")


def render_matrix_report(input_dir: Path, output_path: Path) -> None:
    """Render one algorithm-agnostic Markdown report from an existing matrix."""
    run_path = input_dir / "run.json"
    if not run_path.exists():
        raise ValueError(f"missing report input: {run_path}")
    run = json.loads(run_path.read_text())
    rows = _read_csv(input_dir / "quality.csv")
    requested_sizes = tuple(int(size) for size in run["requested_sizes"])
    sizes = tuple(int(size) for size in run["sizes"])
    skipped_sizes = tuple(run["skipped_sizes"])
    variants = tuple(run["variants"])
    quality, coverage = build_matrix_reports(rows, sizes, variants)
    quality_by_key = {
        (int(row["n"]), str(row["variant"])): row for row in quality
    }

    run_complete = run["complete"]
    if not isinstance(run_complete, bool):
        raise ValueError("run.json field 'complete' must be boolean")
    expected_instances = int(run["instances"])
    processed_instances = int(run["processed_instances"])
    lines = [
        "# Synthetic join-order report",
        "",
        "## Run",
        "",
        f"- Requested sizes: {', '.join(str(size) for size in requested_sizes)}",
        f"- Executed sizes: {', '.join(str(size) for size in sizes)}",
        f"- Instances per size: {run['instance_count_per_size']}",
        f"- Algorithms: {', '.join(f'`{variant}`' for variant in variants)}",
        f"- Matrix: {'complete' if run_complete else 'INCOMPLETE'} "
        f"({processed_instances}/{expected_instances} instances processed)",
        (
            "- Status: "
            f"{run['status_counts']['ok']} ok, "
            f"{run['status_counts']['timeout']} timeout, "
            f"{run['status_counts']['error']} error"
        ),
    ]
    if skipped_sizes:
        lines.append(
            "- Skipped sizes: "
            + ", ".join(
                f"{item['n']} ({', '.join(item['eligible_variants']) or 'no eligible variants'})"
                for item in skipped_sizes
            )
            + "; each size requires at least two eligible algorithms."
        )
    anomaly_count = int(run.get("dp_anomaly_count", 0))
    if anomaly_count:
        lines.append(
            f"- DP anomaly warning: {anomaly_count} successful plan(s) had lower "
            "Total Cost than DP and require investigation. Re-run the recorded "
            "n/seeds on the same benchmark server and configuration."
        )
    if not run_complete:
        lines.extend(
            [
                f"- Abort error: `{run.get('abort_error') or 'unknown'}`",
                "",
                "> This report contains partial observations and must not be used "
                "as a completed matrix result.",
            ]
        )

    lines.extend(
        [
            "",
            "## Total Cost quality",
            "",
            "For each instance, `Q = algorithm Total Cost / lowest successful "
            "Total Cost among the algorithms run for that instance`. `Q = 1.000` "
            "is best. Values at sizes where an algorithm was not scheduled are "
            "`N/A`. Headline aggregates use only instances where every algorithm "
            "scheduled at that size succeeded. Timeouts remain in `quality.csv` "
            "and the coverage section.",
            "",
            "### Common-success sample size",
            "",
            "| n | Samples |",
            "| ---: | ---: |",
        ]
    )
    for size in sizes:
        common_samples = max(
            int(quality_by_key[(size, variant)]["quality_samples"])
            for variant in variants
        )
        lines.append(f"| {size} | {common_samples} |")
    _append_quality_table(lines, "p50 Q", "p50_q", sizes, variants, quality_by_key)
    _append_quality_table(lines, "p95 Q", "p95_q", sizes, variants, quality_by_key)
    _append_quality_table(lines, "max Q", "max_q", sizes, variants, quality_by_key)

    failed_coverage = [
        row
        for row in coverage
        if row["scheduled"] and (row["timeout"] or row["error"])
    ]
    if failed_coverage:
        lines.extend(
            [
                "",
                "## Incomplete coverage",
                "",
                "| n | Algorithm | Planned | Timeout | Error |",
                "| ---: | --- | ---: | ---: | ---: |",
            ]
        )
        for row in failed_coverage:
            lines.append(
                f"| {row['n']} | `{row['variant']}` | {row['planned']} | "
                f"{row['timeout']} | {row['error']} |"
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n")
