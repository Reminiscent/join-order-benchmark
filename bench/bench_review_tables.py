from __future__ import annotations

import csv
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from xml.sax.saxutils import escape as xml_escape

from bench_common import safe_artifact_name


METRICS = {
    "execution": ("execution_ms_median", "Execution Time"),
    "planning": ("planning_ms_median", "Planning Time"),
}

DEFAULT_METRICS = ("execution", "planning")
DEFAULT_REFERENCE_VARIANT = "dp"
XLSX_PAGE_ORIENTATION = "landscape"
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


def col_name(col: int) -> str:
    name = ""
    while col:
        col, rem = divmod(col - 1, 26)
        name = chr(ord("A") + rem) + name
    return name


def xml_attr(value: object) -> str:
    return xml_escape(str(value), {'"': "&quot;"})


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


def xlsx_style_for_cell(cell: ReviewTableCell, *, total: bool = False) -> int:
    css_class = cell.css_class
    if "missing" in css_class:
        return 8
    if "ratio-fast-strong" in css_class:
        return 17 if total else 9
    if "ratio-fast" in css_class:
        return 18 if total else 10
    if "ratio-neutral" in css_class:
        return 19 if total else 11
    if "ratio-slow" in css_class:
        return 20 if total else 12
    if "ratio-slower" in css_class:
        return 21 if total else 13
    if "ratio-worst" in css_class:
        return 22 if total else 14
    if "numeric" in css_class:
        return 16 if total else 7
    return 15 if total else 6


def xlsx_cell(ref: str, value: object | None, style: int, *, numeric: bool = False) -> str:
    if value is None or value == "":
        return f'<c r="{ref}" s="{style}"/>'
    if numeric:
        return f'<c r="{ref}" s="{style}"><v>{xml_escape(str(value))}</v></c>'
    return f'<c r="{ref}" s="{style}" t="inlineStr"><is><t>{xml_escape(str(value))}</t></is></c>'


def xlsx_row(row_idx: int, cells: list[str]) -> str:
    return f'<row r="{row_idx}">{"".join(cells)}</row>'


def xlsx_sheet_xml(table: ReviewTable) -> str:
    query_cols = 3
    value_start = query_cols + 1
    value_end = value_start + len(table.variants) - 1
    ratio_start = value_end + 1
    ratio_end = ratio_start + len(table.variants) - 2
    total_cols = query_cols + len(table.variants) + max(0, len(table.variants) - 1)
    last_col = col_name(total_cols)
    rows: list[str] = []
    merges: list[str] = []

    title = f"{table.scenario or 'benchmark'} {table.metric_title}"
    rows.append(xlsx_row(1, [xlsx_cell("A1", title, 1)]))
    merges.append(f"A1:{last_col}1")

    meta = (
        f"run={table.run_id}; scenario={table.scenario}; metric={table.metric_column}; "
        f"reference={table.reference}; ratio=direct variant/{table.reference}"
    )
    rows.append(xlsx_row(2, [xlsx_cell("A2", meta, 2)]))
    merges.append(f"A2:{last_col}2")

    row_idx = 4
    header_cells: list[str] = [xlsx_cell("A4", "dataset", 3)]
    merges.append("A4:A5")
    header_cells.append(xlsx_cell("B4", "query", 3))
    merges.append("B4:B5")
    header_cells.append(xlsx_cell("C4", "joins", 3))
    merges.append("C4:C5")
    header_cells.append(
        xlsx_cell(
            f"{col_name(value_start)}4",
            f"median {table.metric_title.lower()} (ms)",
            4,
        )
    )
    merges.append(f"{col_name(value_start)}4:{col_name(value_end)}4")
    if ratio_start <= ratio_end:
        header_cells.append(
            xlsx_cell(
                f"{col_name(ratio_start)}4",
                f"ratio to {table.labels[table.reference]}",
                5,
            )
        )
        merges.append(f"{col_name(ratio_start)}4:{col_name(ratio_end)}4")
    rows.append(xlsx_row(row_idx, header_cells))

    row_idx = 5
    subheader_cells: list[str] = []
    for idx in range(1, query_cols + 1):
        subheader_cells.append(xlsx_cell(f"{col_name(idx)}5", "", 3))
    col = value_start
    for variant in table.variants:
        subheader_cells.append(xlsx_cell(f"{col_name(col)}5", table.labels[variant], 3))
        col += 1
    for variant in table.variants:
        if variant == table.reference:
            continue
        subheader_cells.append(
            xlsx_cell(
                f"{col_name(col)}5",
                f"{table.labels[variant]}/{table.labels[table.reference]}",
                3,
            )
        )
        col += 1
    rows.append(xlsx_row(row_idx, subheader_cells))

    row_idx = 6
    for table_row in table.rows:
        cells: list[str] = [xlsx_cell(f"A{row_idx}", table_row.dataset, 6)]
        cells.append(xlsx_cell(f"B{row_idx}", table_row.query_id, 6))
        cells.append(xlsx_cell(f"C{row_idx}", table_row.join_size, 7, numeric=True))
        col = 4
        for variant in table.variants:
            cell = table_row.values[variant]
            cells.append(
                xlsx_cell(
                    f"{col_name(col)}{row_idx}",
                    cell.raw,
                    xlsx_style_for_cell(cell),
                    numeric=cell.raw is not None,
                )
            )
            col += 1
        for variant in table.variants:
            if variant == table.reference:
                continue
            cell = table_row.ratios[variant]
            cells.append(
                xlsx_cell(
                    f"{col_name(col)}{row_idx}",
                    cell.raw,
                    xlsx_style_for_cell(cell),
                    numeric=cell.raw is not None,
                )
            )
            col += 1
        rows.append(xlsx_row(row_idx, cells))
        row_idx += 1

    total_cells = [xlsx_cell(f"A{row_idx}", "SUM", 15)]
    if query_cols > 1:
        merges.append(f"A{row_idx}:{col_name(query_cols)}{row_idx}")
        for col in range(2, query_cols + 1):
            total_cells.append(xlsx_cell(f"{col_name(col)}{row_idx}", "", 15))
    col = value_start
    for variant in table.variants:
        cell = table.total_values[variant]
        total_cells.append(
            xlsx_cell(
                f"{col_name(col)}{row_idx}",
                cell.raw,
                xlsx_style_for_cell(cell, total=True),
                numeric=cell.raw is not None,
            )
        )
        col += 1
    for variant in table.variants:
        if variant == table.reference:
            continue
        cell = table.total_ratios[variant]
        total_cells.append(
            xlsx_cell(
                f"{col_name(col)}{row_idx}",
                cell.raw,
                xlsx_style_for_cell(cell, total=True),
                numeric=cell.raw is not None,
            )
        )
        col += 1
    rows.append(xlsx_row(row_idx, total_cells))

    width_entries = ['<col min="1" max="1" width="20" customWidth="1"/>']
    width_entries.append('<col min="2" max="2" width="10" customWidth="1"/>')
    width_entries.append('<col min="3" max="3" width="8" customWidth="1"/>')
    if value_start <= value_end:
        width_entries.append(f'<col min="{value_start}" max="{value_end}" width="15" customWidth="1"/>')
    if ratio_start <= ratio_end:
        width_entries.append(f'<col min="{ratio_start}" max="{ratio_end}" width="13" customWidth="1"/>')

    merge_xml = ""
    if merges:
        merge_xml = f'<mergeCells count="{len(merges)}">' + "".join(f'<mergeCell ref="{ref}"/>' for ref in merges) + "</mergeCells>"

    return "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
            '<sheetViews><sheetView workbookViewId="0"><pane ySplit="5" topLeftCell="A6" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>',
            f"<cols>{''.join(width_entries)}</cols>",
            f'<sheetData>{"".join(rows)}</sheetData>',
            f'<autoFilter ref="A5:{last_col}{row_idx}"/>',
            merge_xml,
            '<pageMargins left="0.3" right="0.3" top="0.5" bottom="0.5" header="0.2" footer="0.2"/>',
            f'<pageSetup orientation="{xml_attr(XLSX_PAGE_ORIENTATION)}" fitToWidth="1" fitToHeight="0"/>',
            "</worksheet>",
        ]
    )


def xlsx_styles_xml() -> str:
    fills = [
        "FFFFFF",
        "F3F4F6",
        "E5E7EB",
        "DBEAFE",
        "F3F4F6",
        "B7E1CD",
        "D9EAD3",
        "FFFFFF",
        "FFF2CC",
        "FCE5CD",
        "F4CCCC",
    ]

    def fill(color: str) -> str:
        return f'<fill><patternFill patternType="solid"><fgColor rgb="FF{color}"/><bgColor indexed="64"/></patternFill></fill>'

    def xf(font_id: int, fill_id: int, border_id: int, align: str = "", num_fmt: int = 0) -> str:
        attrs = f'numFmtId="{num_fmt}" fontId="{font_id}" fillId="{fill_id}" borderId="{border_id}" xfId="0"'
        if num_fmt:
            attrs += ' applyNumberFormat="1"'
        if align:
            return f'<xf {attrs} applyAlignment="1"><alignment {align}/></xf>'
        return f"<xf {attrs}/>"

    xfs = [
        xf(0, 0, 0),  # 0 default
        xf(1, 0, 0, 'horizontal="left" vertical="center"'),  # 1 title
        xf(0, 0, 0, 'horizontal="left" vertical="center" wrapText="1"'),  # 2 meta
        xf(2, 2, 1, 'horizontal="center" vertical="center" wrapText="1"'),  # 3 header
        xf(2, 3, 1, 'horizontal="center" vertical="center" wrapText="1"'),  # 4 value group
        xf(2, 4, 1, 'horizontal="center" vertical="center" wrapText="1"'),  # 5 ratio group
        xf(0, 0, 1),  # 6 text
        xf(0, 0, 1, 'horizontal="right" vertical="center"', 164),  # 7 numeric
        xf(0, 5, 1),  # 8 missing
        xf(0, 6, 1, 'horizontal="right" vertical="center"', 165),  # 9 ratio fast strong
        xf(0, 7, 1, 'horizontal="right" vertical="center"', 165),  # 10 ratio fast
        xf(0, 8, 1, 'horizontal="right" vertical="center"', 165),  # 11 ratio neutral
        xf(0, 9, 1, 'horizontal="right" vertical="center"', 165),  # 12 ratio slow
        xf(0, 10, 1, 'horizontal="right" vertical="center"', 165),  # 13 ratio slower
        xf(0, 11, 1, 'horizontal="right" vertical="center"', 165),  # 14 ratio worst
        xf(2, 0, 2),  # 15 total text
        xf(2, 0, 2, 'horizontal="right" vertical="center"', 164),  # 16 total numeric
        xf(2, 6, 2, 'horizontal="right" vertical="center"', 165),  # 17 total ratio fast strong
        xf(2, 7, 2, 'horizontal="right" vertical="center"', 165),  # 18 total ratio fast
        xf(2, 8, 2, 'horizontal="right" vertical="center"', 165),  # 19 total ratio neutral
        xf(2, 9, 2, 'horizontal="right" vertical="center"', 165),  # 20 total ratio slow
        xf(2, 10, 2, 'horizontal="right" vertical="center"', 165),  # 21 total ratio slower
        xf(2, 11, 2, 'horizontal="right" vertical="center"', 165),  # 22 total ratio worst
    ]
    return "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">',
            '<numFmts count="2"><numFmt numFmtId="164" formatCode="0.00"/><numFmt numFmtId="165" formatCode="0.00"/></numFmts>',
            '<fonts count="3"><font><sz val="11"/><name val="Aptos"/></font><font><b/><sz val="14"/><name val="Aptos"/></font><font><b/><sz val="11"/><name val="Aptos"/></font></fonts>',
            f'<fills count="{2 + len(fills)}"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill>{"".join(fill(c) for c in fills)}</fills>',
            '<borders count="3"><border/><border><left style="thin"><color rgb="FFD1D5DB"/></left><right style="thin"><color rgb="FFD1D5DB"/></right><top style="thin"><color rgb="FFD1D5DB"/></top><bottom style="thin"><color rgb="FFD1D5DB"/></bottom></border><border><left style="thin"><color rgb="FF111827"/></left><right style="thin"><color rgb="FF111827"/></right><top style="medium"><color rgb="FF111827"/></top><bottom style="thin"><color rgb="FF111827"/></bottom></border></borders>',
            '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>',
            f'<cellXfs count="{len(xfs)}">{"".join(xfs)}</cellXfs>',
            '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>',
            "</styleSheet>",
        ]
    )


def write_review_workbook(path: Path, tables: list[ReviewTable]) -> None:
    if not tables:
        raise SystemExit("cannot write an empty workbook")

    used_sheet_names: set[str] = set()
    sheet_names = [sheet_name(table.metric, used_sheet_names) for table in tables]

    workbook_sheets = "".join(
        f'<sheet name="{xml_attr(name)}" sheetId="{idx}" r:id="rId{idx}"/>'
        for idx, name in enumerate(sheet_names, start=1)
    )
    workbook_rels = "".join(
        f'<Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>'
        for idx in range(1, len(tables) + 1)
    )
    workbook_rels += '<Relationship Id="rIdStyles" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
    sheet_overrides = "".join(
        f'<Override PartName="/xl/worksheets/sheet{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for idx in range(1, len(tables) + 1)
    )

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "[Content_Types].xml",
            "\n".join(
                [
                    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
                    '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
                    '<Default Extension="xml" ContentType="application/xml"/>',
                    '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
                    '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
                    sheet_overrides,
                    '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
                    '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
                    "</Types>",
                ]
            ),
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>',
        )
        zf.writestr(
            "xl/workbook.xml",
            f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>{workbook_sheets}</sheets></workbook>',
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{workbook_rels}</Relationships>',
        )
        zf.writestr("xl/styles.xml", xlsx_styles_xml())
        zf.writestr(
            "docProps/core.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:creator>join_order_benchmark</dc:creator></cp:coreProperties>',
        )
        zf.writestr(
            "docProps/app.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>join_order_benchmark</Application></Properties>',
        )
        for idx, table in enumerate(tables, start=1):
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", xlsx_sheet_xml(table))


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
