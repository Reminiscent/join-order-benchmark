from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from bench_common import safe_artifact_name


METRICS = {
    "execution": ("execution_ms_median", "Execution Time"),
    "planning": ("planning_ms_median", "Planning Time"),
}

DEFAULT_METRICS = ("execution", "planning")
DEFAULT_REFERENCE_VARIANT = "dp"
XLSX_MISSING_DEPENDENCY = (
    "missing optional dependency: install XlsxWriter before rendering reviewer XLSX tables "
    "(for example: python3 -m pip install XlsxWriter)"
)
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
class SummaryRow:
    dataset: str
    variant: str
    query_id: str
    join_size: int
    ok_reps: int
    err_reps: int
    planning_ms_median: float | None
    execution_ms_median: float | None
    total_ms_median: float | None
    plan_total_cost_median: float | None

    def metric_value(self, column: str) -> float | None:
        return getattr(self, column)


@dataclass(frozen=True)
class ReviewTableCell:
    text: str
    raw: float | None = None
    css_class: str = ""


@dataclass(frozen=True)
class ReviewTableRow:
    dataset: str
    query_id: str
    join_size: int
    values: dict[str, ReviewTableCell]
    ratios: dict[str, ReviewTableCell]
    family_start: bool = False


@dataclass(frozen=True)
class ReviewTable:
    run_id: str
    scenario: str
    datasets: tuple[str, ...]
    metric: str
    metric_column: str
    metric_title: str
    reference: str
    variants: tuple[str, ...]
    labels: dict[str, str]
    rows: tuple[ReviewTableRow, ...]
    total_values: dict[str, ReviewTableCell]
    total_ratios: dict[str, ReviewTableCell]


def parse_csv_list(raw: Optional[str]) -> list[str]:
    if raw is None:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def maybe_float(raw: str) -> float | None:
    text = raw.strip()
    if not text:
        return None
    return float(text)


def public_label(variant: str, label_by_name: dict[str, str]) -> str:
    return PUBLIC_LABELS.get(variant, label_by_name.get(variant, variant))


def load_summary_rows(summary_path: Path) -> tuple[dict[str, dict[str, dict[str, SummaryRow]]], dict[str, list[str]]]:
    rows: dict[str, dict[str, dict[str, SummaryRow]]] = {}
    query_order: dict[str, list[str]] = {}
    with summary_path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "dataset",
            "variant",
            "query_id",
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


def natural_key(text: str) -> list[object]:
    parts = re.split(r"(\d+)", text)
    out: list[object] = []
    for part in parts:
        if not part:
            continue
        out.append(int(part) if part.isdigit() else part)
    return out


def query_family(query_id: str) -> str:
    match = re.match(r"(\d+)", query_id)
    return match.group(1) if match else query_id


def format_ms(value: float | None) -> str:
    if value is None:
        return ""
    if abs(value) >= 100000:
        return f"{value:.0f}"
    if abs(value) >= 1000:
        return f"{value:.1f}".rstrip("0").rstrip(".")
    return f"{value:.2f}".rstrip("0").rstrip(".")


def format_ratio(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}".rstrip("0").rstrip(".")


def ratio_css_class(value: float | None) -> str:
    if value is None:
        return "missing"
    if value < 0.75:
        return "ratio ratio-fast-strong"
    if value < 0.95:
        return "ratio ratio-fast"
    if value <= 1.05:
        return "ratio ratio-neutral"
    if value <= 1.25:
        return "ratio ratio-slow"
    if value <= 2.0:
        return "ratio ratio-slower"
    return "ratio ratio-worst"


def metric_value(row: SummaryRow | None, metric_column: str) -> float | None:
    if row is None or row.ok_reps <= 0:
        return None
    return row.metric_value(metric_column)


def ratio_to_reference(value: float | None, reference_value: float | None) -> float | None:
    if value is None or reference_value is None:
        return None
    if reference_value <= 0:
        return None
    return value / reference_value


def first_row_for_query(dataset_rows: dict[str, dict[str, SummaryRow]], query_id: str) -> SummaryRow | None:
    for rows_by_query in dataset_rows.values():
        row = rows_by_query.get(query_id)
        if row is not None:
            return row
    return None


def dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def resolve_combined_variant_order(
    *,
    run_context: dict[str, Any],
    rows_by_dataset: dict[str, dict[str, dict[str, SummaryRow]]],
    selected_datasets: list[str],
    variants_csv: Optional[str],
) -> tuple[str, ...]:
    if variants_csv:
        variants = tuple(parse_csv_list(variants_csv))
    else:
        configured = [
            str(entry["name"])
            for entry in run_context.get("variants", [])
            if isinstance(entry, dict) and entry.get("name")
        ]
        if configured:
            variants = tuple(configured)
        else:
            found: list[str] = []
            for dataset in selected_datasets:
                found.extend(rows_by_dataset.get(dataset, {}))
            variants = tuple(sorted(set(found)))

    missing = [
        variant
        for variant in variants
        if not any(variant in rows_by_dataset.get(dataset, {}) for dataset in selected_datasets)
    ]
    if missing:
        raise SystemExit(f"run summary does not contain selected variant(s): {', '.join(missing)}")
    return variants


def label_map(run_context: dict[str, Any], variants: tuple[str, ...]) -> dict[str, str]:
    labels = {
        str(entry.get("name")): str(entry.get("label") or entry.get("name"))
        for entry in run_context.get("variants", [])
        if isinstance(entry, dict) and entry.get("name")
    }
    return {variant: public_label(variant, labels) for variant in variants}


def build_review_table(
    *,
    run_context: dict[str, Any],
    rows_by_dataset: dict[str, dict[str, dict[str, SummaryRow]]],
    query_order: dict[str, list[str]],
    datasets: list[str],
    metric: str,
    variants_csv: Optional[str],
) -> ReviewTable:
    missing_datasets = [dataset for dataset in datasets if dataset not in rows_by_dataset]
    if missing_datasets:
        raise SystemExit(f"run summary does not contain dataset(s): {', '.join(missing_datasets)}")
    if metric not in METRICS:
        raise SystemExit(f"unknown metric '{metric}'")

    metric_column, metric_title = METRICS[metric]
    variants = resolve_combined_variant_order(
        run_context=run_context,
        rows_by_dataset=rows_by_dataset,
        selected_datasets=datasets,
        variants_csv=variants_csv,
    )
    reference = DEFAULT_REFERENCE_VARIANT
    if reference not in variants:
        raise SystemExit(f"reference variant '{reference}' is not in the selected variants")

    labels = label_map(run_context, variants)

    rows: list[ReviewTableRow] = []
    previous_dataset = ""
    previous_family = ""
    for dataset in datasets:
        dataset_rows = rows_by_dataset[dataset]
        query_ids = sorted(query_order[dataset], key=natural_key)
        previous_family = ""
        for query_id in query_ids:
            sample = first_row_for_query(dataset_rows, query_id)
            if sample is None:
                continue
            values: dict[str, ReviewTableCell] = {}
            for variant in variants:
                value = metric_value(dataset_rows.get(variant, {}).get(query_id), metric_column)
                css_class = "numeric" if value is not None else "numeric missing"
                values[variant] = ReviewTableCell(text=format_ms(value), raw=value, css_class=css_class)

            reference_value = values[reference].raw
            ratios: dict[str, ReviewTableCell] = {}
            for variant in variants:
                if variant == reference:
                    continue
                ratio = ratio_to_reference(values[variant].raw, reference_value)
                ratios[variant] = ReviewTableCell(text=format_ratio(ratio), raw=ratio, css_class=ratio_css_class(ratio))

            family = query_family(query_id)
            rows.append(
                ReviewTableRow(
                    dataset=dataset,
                    query_id=query_id,
                    join_size=sample.join_size,
                    values=values,
                    ratios=ratios,
                    family_start=bool(
                        (previous_dataset and dataset != previous_dataset)
                        or (previous_family and family != previous_family)
                    ),
                )
            )
            previous_dataset = dataset
            previous_family = family

    total_values: dict[str, ReviewTableCell] = {}
    for variant in variants:
        total = sum(row.values[variant].raw or 0.0 for row in rows if row.values[variant].raw is not None)
        total_values[variant] = ReviewTableCell(text=format_ms(total), raw=total, css_class="numeric total")

    total_ratios: dict[str, ReviewTableCell] = {}
    for variant in variants:
        if variant == reference:
            continue
        comparable_rows = [
            row
            for row in rows
            if row.values[variant].raw is not None and row.values[reference].raw is not None
        ]
        variant_total = sum(row.values[variant].raw or 0.0 for row in comparable_rows)
        reference_total = sum(row.values[reference].raw or 0.0 for row in comparable_rows)
        ratio = ratio_to_reference(variant_total, reference_total)
        total_ratios[variant] = ReviewTableCell(text=format_ratio(ratio), raw=ratio, css_class=ratio_css_class(ratio))

    return ReviewTable(
        run_id=str(run_context.get("run_id", "")),
        scenario=str(run_context.get("scenario", "")),
        datasets=tuple(datasets),
        metric=metric,
        metric_column=metric_column,
        metric_title=metric_title,
        reference=reference,
        variants=variants,
        labels=labels,
        rows=tuple(rows),
        total_values=total_values,
        total_ratios=total_ratios,
    )


def render_review_table_csv(table: ReviewTable) -> str:
    from io import StringIO

    out = StringIO()
    writer = csv.writer(out, lineterminator="\n")
    header = ["dataset", "query", "join_size"]
    header.extend([f"{variant}_{table.metric_column}" for variant in table.variants])
    header.extend([f"{variant}_to_{table.reference}" for variant in table.variants if variant != table.reference])
    writer.writerow(header)
    for row in table.rows:
        cells: list[str] = [row.dataset, row.query_id, str(row.join_size)]
        cells.extend(cell.text for cell in row.values.values())
        cells.extend(cell.text for cell in row.ratios.values())
        writer.writerow(cells)

    total: list[str] = ["SUM", "", ""]
    total.extend(cell.text for cell in table.total_values.values())
    total.extend(cell.text for cell in table.total_ratios.values())
    writer.writerow(total)
    return out.getvalue()


def sheet_name(raw: str, used: set[str]) -> str:
    base = re.sub(r"[][\\/*?:]", "_", raw).strip() or "sheet"
    base = base[:31]
    name = base
    counter = 2
    while name in used:
        suffix = f" {counter}"
        name = base[: 31 - len(suffix)] + suffix
        counter += 1
    used.add(name)
    return name


def xlsx_format_key(cell: ReviewTableCell, *, total: bool = False) -> str:
    css_class = cell.css_class
    if "missing" in css_class:
        return "missing"
    if "ratio-fast-strong" in css_class:
        return "total_ratio_fast_strong" if total else "ratio_fast_strong"
    if "ratio-fast" in css_class:
        return "total_ratio_fast" if total else "ratio_fast"
    if "ratio-neutral" in css_class:
        return "total_ratio_neutral" if total else "ratio_neutral"
    if "ratio-slow" in css_class:
        return "total_ratio_slow" if total else "ratio_slow"
    if "ratio-slower" in css_class:
        return "total_ratio_slower" if total else "ratio_slower"
    if "ratio-worst" in css_class:
        return "total_ratio_worst" if total else "ratio_worst"
    if "numeric" in css_class:
        return "total_numeric" if total else "numeric"
    return "total_text" if total else "text"


def xlsx_formats(workbook: Any) -> dict[str, Any]:
    border = {
        "border": 1,
        "border_color": "#D1D5DB",
        "font_name": "Aptos",
        "font_size": 11,
        "valign": "vcenter",
    }
    numeric = {**border, "align": "right", "num_format": "0.00"}
    total_base = {
        **border,
        "bold": True,
        "top": 2,
        "top_color": "#111827",
    }
    total_numeric = {**total_base, "align": "right", "num_format": "0.00"}

    return {
        "title": workbook.add_format(
            {"font_name": "Aptos", "font_size": 14, "bold": True, "valign": "vcenter"}
        ),
        "meta": workbook.add_format({"font_name": "Aptos", "font_size": 11, "text_wrap": True}),
        "header": workbook.add_format(
            {**border, "bold": True, "align": "center", "text_wrap": True, "bg_color": "#E5E7EB"}
        ),
        "value_header": workbook.add_format(
            {**border, "bold": True, "align": "center", "text_wrap": True, "bg_color": "#DBEAFE"}
        ),
        "ratio_header": workbook.add_format(
            {**border, "bold": True, "align": "center", "text_wrap": True, "bg_color": "#F3F4F6"}
        ),
        "text": workbook.add_format(border),
        "numeric": workbook.add_format(numeric),
        "missing": workbook.add_format({**border, "bg_color": "#F3F4F6"}),
        "ratio_fast_strong": workbook.add_format({**numeric, "bg_color": "#B7E1CD"}),
        "ratio_fast": workbook.add_format({**numeric, "bg_color": "#D9EAD3"}),
        "ratio_neutral": workbook.add_format({**numeric, "bg_color": "#FFFFFF"}),
        "ratio_slow": workbook.add_format({**numeric, "bg_color": "#FFF2CC"}),
        "ratio_slower": workbook.add_format({**numeric, "bg_color": "#FCE5CD"}),
        "ratio_worst": workbook.add_format({**numeric, "bg_color": "#F4CCCC"}),
        "total_text": workbook.add_format(total_base),
        "total_numeric": workbook.add_format(total_numeric),
        "total_ratio_fast_strong": workbook.add_format({**total_numeric, "bg_color": "#B7E1CD"}),
        "total_ratio_fast": workbook.add_format({**total_numeric, "bg_color": "#D9EAD3"}),
        "total_ratio_neutral": workbook.add_format({**total_numeric, "bg_color": "#FFFFFF"}),
        "total_ratio_slow": workbook.add_format({**total_numeric, "bg_color": "#FFF2CC"}),
        "total_ratio_slower": workbook.add_format({**total_numeric, "bg_color": "#FCE5CD"}),
        "total_ratio_worst": workbook.add_format({**total_numeric, "bg_color": "#F4CCCC"}),
    }


def write_or_merge(
    worksheet: Any,
    first_row: int,
    first_col: int,
    last_row: int,
    last_col: int,
    value: object,
    cell_format: Any,
) -> None:
    if first_row == last_row and first_col == last_col:
        worksheet.write(first_row, first_col, value, cell_format)
    else:
        worksheet.merge_range(first_row, first_col, last_row, last_col, value, cell_format)


def write_cell(worksheet: Any, row: int, col: int, value: object | None, cell_format: Any) -> None:
    if value is None or value == "":
        worksheet.write_blank(row, col, None, cell_format)
    elif isinstance(value, (int, float)):
        worksheet.write_number(row, col, value, cell_format)
    else:
        worksheet.write(row, col, value, cell_format)


def require_xlsxwriter() -> Any:
    try:
        import xlsxwriter
    except ModuleNotFoundError as exc:
        raise SystemExit(XLSX_MISSING_DEPENDENCY) from exc
    return xlsxwriter


def write_review_worksheet(workbook: Any, worksheet: Any, table: ReviewTable) -> None:
    formats = xlsx_formats(workbook)
    query_cols = 3
    value_start = query_cols
    value_end = value_start + len(table.variants) - 1
    ratio_start = value_end + 1
    ratio_end = ratio_start + len(table.variants) - 2
    total_cols = query_cols + len(table.variants) + max(0, len(table.variants) - 1)
    last_col = total_cols - 1

    title = f"{table.scenario or 'benchmark'} {table.metric_title}"
    write_or_merge(worksheet, 0, 0, 0, last_col, title, formats["title"])

    meta = (
        f"run={table.run_id}; scenario={table.scenario}; metric={table.metric_column}; "
        f"reference={table.reference}; ratio=direct variant/{table.reference}"
    )
    write_or_merge(worksheet, 1, 0, 1, last_col, meta, formats["meta"])

    write_or_merge(worksheet, 3, 0, 4, 0, "dataset", formats["header"])
    write_or_merge(worksheet, 3, 1, 4, 1, "query", formats["header"])
    write_or_merge(worksheet, 3, 2, 4, 2, "joins", formats["header"])
    write_or_merge(
        worksheet,
        3,
        value_start,
        3,
        value_end,
        f"median {table.metric_title.lower()} (ms)",
        formats["value_header"],
    )
    if ratio_start <= ratio_end:
        write_or_merge(
            worksheet,
            3,
            ratio_start,
            3,
            ratio_end,
            f"ratio to {table.labels[table.reference]}",
            formats["ratio_header"],
        )

    col = value_start
    for variant in table.variants:
        write_cell(worksheet, 4, col, table.labels[variant], formats["header"])
        col += 1
    for variant in table.variants:
        if variant == table.reference:
            continue
        write_cell(
            worksheet,
            4,
            col,
            f"{table.labels[variant]}/{table.labels[table.reference]}",
            formats["header"],
        )
        col += 1

    row_idx = 5
    for table_row in table.rows:
        write_cell(worksheet, row_idx, 0, table_row.dataset, formats["text"])
        write_cell(worksheet, row_idx, 1, table_row.query_id, formats["text"])
        write_cell(worksheet, row_idx, 2, table_row.join_size, formats["numeric"])
        col = value_start
        for variant in table.variants:
            cell = table_row.values[variant]
            write_cell(worksheet, row_idx, col, cell.raw, formats[xlsx_format_key(cell)])
            col += 1
        for variant in table.variants:
            if variant == table.reference:
                continue
            cell = table_row.ratios[variant]
            write_cell(worksheet, row_idx, col, cell.raw, formats[xlsx_format_key(cell)])
            col += 1
        row_idx += 1

    write_or_merge(worksheet, row_idx, 0, row_idx, query_cols - 1, "SUM", formats["total_text"])
    col = value_start
    for variant in table.variants:
        cell = table.total_values[variant]
        write_cell(worksheet, row_idx, col, cell.raw, formats[xlsx_format_key(cell, total=True)])
        col += 1
    for variant in table.variants:
        if variant == table.reference:
            continue
        cell = table.total_ratios[variant]
        write_cell(worksheet, row_idx, col, cell.raw, formats[xlsx_format_key(cell, total=True)])
        col += 1

    worksheet.freeze_panes(5, 0)
    worksheet.autofilter(4, 0, row_idx, last_col)
    worksheet.set_landscape()
    worksheet.fit_to_pages(1, 0)
    worksheet.set_margins(left=0.3, right=0.3, top=0.5, bottom=0.5)
    worksheet.set_column(0, 0, 20)
    worksheet.set_column(1, 1, 10)
    worksheet.set_column(2, 2, 8)
    worksheet.set_column(value_start, value_end, 15)
    if ratio_start <= ratio_end:
        worksheet.set_column(ratio_start, ratio_end, 13)


def write_review_workbook(path: Path, tables: list[ReviewTable]) -> None:
    if not tables:
        raise SystemExit("cannot write an empty workbook")

    xlsxwriter = require_xlsxwriter()

    used_sheet_names: set[str] = set()
    sheet_names = [sheet_name(table.metric, used_sheet_names) for table in tables]

    workbook = xlsxwriter.Workbook(str(path))
    try:
        workbook.set_properties({"author": "join_order_benchmark"})
        for table, name in zip(tables, sheet_names):
            worksheet = workbook.add_worksheet(name)
            write_review_worksheet(workbook, worksheet, table)
    finally:
        workbook.close()


def default_output_name() -> str:
    return safe_artifact_name("review")


def default_csv_output_name(metric: str) -> str:
    return safe_artifact_name(f"review_{metric}")


def write_review_tables(
    *,
    run_dir: Path,
    datasets: list[str],
    variants_csv: Optional[str] = None,
) -> list[Path]:
    run_json = run_dir / "run.json"
    summary_csv = run_dir / "summary.csv"
    if not run_json.is_file():
        raise SystemExit(f"missing run context: {run_json}")
    if not summary_csv.is_file():
        raise SystemExit(f"missing summary csv: {summary_csv}")

    run_context = json.loads(run_json.read_text())
    rows_by_dataset, query_order = load_summary_rows(summary_csv)
    selected_datasets = datasets or dedupe_preserve(
        [
            str(entry["dataset"])
            for entry in run_context.get("datasets", [])
            if isinstance(entry, dict) and entry.get("dataset") in rows_by_dataset
        ]
    )
    if not selected_datasets:
        selected_datasets = sorted(rows_by_dataset)

    require_xlsxwriter()

    out_dir = run_dir / "review_tables"
    out_dir.mkdir(parents=True, exist_ok=True)

    tables: list[ReviewTable] = []
    written: list[Path] = []
    for metric in DEFAULT_METRICS:
        table = build_review_table(
            run_context=run_context,
            rows_by_dataset=rows_by_dataset,
            query_order=query_order,
            datasets=selected_datasets,
            metric=metric,
            variants_csv=variants_csv,
        )
        tables.append(table)
        csv_path = out_dir / f"{default_csv_output_name(metric)}.csv"
        csv_path.write_text(render_review_table_csv(table))
        written.append(csv_path)
    workbook_path = out_dir / f"{default_output_name()}.xlsx"
    write_review_workbook(workbook_path, tables)
    written.insert(0, workbook_path)
    return written
