"""Compile one expanded graph into deterministic PostgreSQL artifacts.

``compile_graph`` emits six files from one semantic source:

* ``graph.json`` preserves the canonical graph.
* ``graph.md`` renders the graph for quick human inspection.
* ``schema.sql`` creates empty tables and edge-specific join columns.
* ``cardinality_metadata.sql`` maps those tables and edges back to the graph.
* ``base_stats.sql`` restores generated relation and column statistics.
* ``query.sql`` expresses every graph edge as one equality predicate.

The schema deliberately has no rows, indexes, constraints, filters, ordering,
or explicit join parentheses, so it does not add access paths, inferred edges,
or a predetermined join order.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .graph import (
    canonical_json,
    edge_endpoint_key_ndv,
    graph_instance_id,
    validate_graph,
)


CASE_FILENAMES = (
    "graph.json",
    "graph.md",
    "schema.sql",
    "cardinality_metadata.sql",
    "base_stats.sql",
    "query.sql",
)
CASE_DOCUMENTATION_FILENAMES = ("README.md",)

# Synthetic pages make scan I/O grow with base rows while keeping it
# independent of graph degree and the number of generated join columns.
VIRTUAL_ROWS_PER_PAGE = 200


class CompileError(ValueError):
    """Raised when an IR cannot be compiled or artifacts fail verification."""


@dataclass(frozen=True)
class CompiledCase:
    """Stable identity and generated files for one PostgreSQL-ready case."""

    instance_id: str
    schema_name: str
    files: Mapping[str, str]


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _float_literal(value: float) -> str:
    return format(value, ".17g")


def _relation_name(canonical_id: int) -> str:
    return f"r{canonical_id:04d}"


def _column_name(edge: Mapping[str, Any]) -> str:
    return f"j_{edge['id']}"


def _qualified(schema_name: str, relation_name: str) -> str:
    return f"{_quote_identifier(schema_name)}.{_quote_identifier(relation_name)}"


def _compact_number(value: float) -> str:
    return format(value, ".6g")


def _compile_graph_markdown(graph: Mapping[str, Any]) -> str:
    """Render a compact reading view derived entirely from ``graph.json``."""
    node_rows = {node["id"]: node["base_rows"] for node in graph["nodes"]}
    lines = [
        "# Generated Join graph",
        "",
        "This file is generated for quick inspection. `graph.json` remains the",
        "authoritative machine-readable graph.",
        "",
        "```mermaid",
        "graph LR",
    ]
    for node in graph["nodes"]:
        lines.append(
            f'    R{node["id"]}["R{node["id"]}<br/>rows={node["base_rows"]:,}"]'
        )
    for edge in graph["edges"]:
        smaller_rows = min(node_rows[edge["left"]], node_rows[edge["right"]])
        output_factor = (
            edge["key_overlap_fraction"]
            * smaller_rows
            / max(edge["left_key_ndv"], edge["right_key_ndv"])
        )
        lines.append(
            f'    R{edge["left"]} ---|"{edge["type"]}<br/>'
            f'output ×{_compact_number(output_factor)}"| R{edge["right"]}'
        )
    lines.extend(
        [
            "```",
            "",
            "## Edge details",
            "",
            "For each endpoint, `D` is its key NDV and `M = rows / D` is its",
            "average key multiplicity. `q` is the effective fraction of the",
            "smaller key set that matches the other endpoint. The logical",
            "selectivity is `q / max(D_left, D_right)`.",
            "",
            "| Edge | Type | Left `D` | Left `M` | Right `D` | Right `M` | Overlap `q` | Selectivity | Output factor |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for edge in graph["edges"]:
        left_rows = node_rows[edge["left"]]
        right_rows = node_rows[edge["right"]]
        smaller_rows = min(node_rows[edge["left"]], node_rows[edge["right"]])
        output_factor = (
            edge["key_overlap_fraction"]
            * smaller_rows
            / max(edge["left_key_ndv"], edge["right_key_ndv"])
        )
        lines.append(
            f'| R{edge["left"]} -- R{edge["right"]} '
            f'| `{edge["type"]}` '
            f'| {edge["left_key_ndv"]:,} '
            f'| {_compact_number(left_rows / edge["left_key_ndv"])} '
            f'| {edge["right_key_ndv"]:,} '
            f'| {_compact_number(right_rows / edge["right_key_ndv"])} '
            f'| {_compact_number(edge["key_overlap_fraction"])} '
            f'| {_compact_number(edge["selectivity"])} '
            f'| {_compact_number(output_factor)} |'
        )
    lines.append("")
    return "\n".join(lines)


def relation_page_counts(graph: Mapping[str, Any]) -> dict[int, int]:
    """Return ``ceil(base_rows / 200)`` virtual pages for every relation.

    This is a controlled costing input, not an estimate of PostgreSQL heap
    tuple layout.
    """
    return {
        node["id"]: max(
            1,
            (node["base_rows"] + VIRTUAL_ROWS_PER_PAGE - 1)
            // VIRTUAL_ROWS_PER_PAGE,
        )
        for node in graph["nodes"]
    }


def _compile_schema(graph: Mapping[str, Any], schema_name: str) -> str:
    """Create one empty table per node and one distinct column per edge."""
    incident: dict[int, list[Mapping[str, Any]]] = {node["id"]: [] for node in graph["nodes"]}
    for edge in graph["edges"]:
        incident[edge["left"]].append(edge)
        incident[edge["right"]].append(edge)

    lines = [f"CREATE SCHEMA {_quote_identifier(schema_name)};", ""]
    for node in graph["nodes"]:
        relation_name = _relation_name(node["id"])
        lines.append(f"CREATE TABLE {_qualified(schema_name, relation_name)} (")
        columns = sorted(_column_name(edge) for edge in incident[node["id"]])
        for index, column in enumerate(columns):
            comma = "," if index + 1 < len(columns) else ""
            lines.append(f"    {_quote_identifier(column)} bigint{comma}")
        lines.extend(
            [
                ") WITH (autovacuum_enabled = false);",
                "",
            ]
        )
    return "\n".join(lines)


def _compile_cardinality_metadata(
    graph: Mapping[str, Any], instance_id: str, schema_name: str
) -> str:
    """Emit the minimal metadata needed to recover subset cardinalities.

    Relation rows map canonical IDs to PostgreSQL tables and their virtual
    size. Edge rows carry endpoints, both endpoint key NDVs, and final
    selectivity. Generator-only fields are intentionally omitted.
    """
    base_pages = relation_page_counts(graph)

    relation_values = []
    for node in graph["nodes"]:
        relation_name = _relation_name(node["id"])
        regclass = f"{schema_name}.{relation_name}"
        relation_values.append(
            f"    ({_quote_literal(instance_id)}, {node['id']}, "
            f"{_quote_literal(regclass)}::pg_catalog.regclass, "
            f"{node['base_rows']}::double precision, {base_pages[node['id']]}::bigint)"
        )
    edge_values = [
        f"    ({_quote_literal(instance_id)}, {edge['left']}, {edge['right']}, "
        f"{edge['left_key_ndv']}::bigint, "
        f"{edge['right_key_ndv']}::bigint, "
        f"{_float_literal(edge['selectivity'])}::double precision)"
        for edge in graph["edges"]
    ]
    return (
        "INSERT INTO bench.join_order_relation\n"
        "    (instance_id, canonical_id, relation, base_rows, base_pages)\n"
        "VALUES\n"
        + ",\n".join(relation_values)
        + ";\n\n"
        "INSERT INTO bench.join_order_edge\n"
        "    (instance_id, left_id, right_id, left_key_ndv, right_key_ndv, selectivity)\n"
        "VALUES\n"
        + ",\n".join(edge_values)
        + ";\n"
    )


def _compile_base_stats(graph: Mapping[str, Any], schema_name: str) -> str:
    """Restore base rows/pages and uniform statistics for every edge column."""
    base_pages = relation_page_counts(graph)
    incident: dict[int, list[Mapping[str, Any]]] = {node["id"]: [] for node in graph["nodes"]}
    for edge in graph["edges"]:
        incident[edge["left"]].append(edge)
        incident[edge["right"]].append(edge)

    lines = ["DO $synthetic_join_order$", "BEGIN"]
    for node in graph["nodes"]:
        relation_name = _relation_name(node["id"])
        relpages = base_pages[node["id"]]
        lines.extend(
            [
                "    IF NOT pg_catalog.pg_restore_relation_stats(",
                f"        'schemaname', {_quote_literal(schema_name)},",
                f"        'relname', {_quote_literal(relation_name)},",
                f"        'relpages', {relpages}::integer,",
                f"        'reltuples', {node['base_rows']}::real) THEN",
                f"        RAISE EXCEPTION 'failed to restore relation stats for {relation_name}';",
                "    END IF;",
            ]
        )
        for edge in sorted(incident[node["id"]], key=lambda value: value["id"]):
            column_name = _column_name(edge)
            key_ndv = edge_endpoint_key_ndv(edge, node["id"])
            lines.extend(
                [
                    "    IF NOT pg_catalog.pg_restore_attribute_stats(",
                    f"        'schemaname', {_quote_literal(schema_name)},",
                    f"        'relname', {_quote_literal(relation_name)},",
                    f"        'attname', {_quote_literal(column_name)},",
                    "        'inherited', false::boolean,",
                    "        'null_frac', 0::real,",
                    "        'avg_width', 8::integer,",
                    f"        'n_distinct', {key_ndv}::real,",
                    "        'most_common_vals', '{0}'::text,",
                    f"        'most_common_freqs', ARRAY[(1.0 / {key_ndv})::real]::real[]) THEN",
                    "        RAISE EXCEPTION "
                    f"'failed to restore attribute stats for {relation_name}.{column_name}';",
                    "    END IF;",
                ]
            )
    lines.extend(["END", "$synthetic_join_order$;", ""])
    return "\n".join(lines)


def _compile_query(graph: Mapping[str, Any], schema_name: str) -> str:
    """Emit one flat inner-join query that mirrors the graph exactly.

    Every node appears once in ``FROM`` and every edge appears once in
    ``WHERE``. No explicit join parentheses constrain planner search.
    """
    from_items = [
        f"    {_qualified(schema_name, _relation_name(node['id']))} AS "
        f"{_quote_identifier(_relation_name(node['id']))}"
        for node in graph["nodes"]
    ]
    predicates = []
    for edge in graph["edges"]:
        left = _quote_identifier(_relation_name(edge["left"]))
        right = _quote_identifier(_relation_name(edge["right"]))
        column = _quote_identifier(_column_name(edge))
        predicates.append(f"{left}.{column} = {right}.{column}")
    where_lines = [f"    {predicates[0]}"] + [f"  AND {predicate}" for predicate in predicates[1:]]
    return "SELECT 1\nFROM\n" + ",\n".join(from_items) + "\nWHERE\n" + "\n".join(where_lines) + ";\n"


def compile_graph(graph: Mapping[str, Any]) -> CompiledCase:
    """Compile a validated graph and derive its stable instance/schema names."""
    validate_graph(graph)
    instance_id = graph_instance_id(graph)
    schema_name = f"sjo_{instance_id[:12]}"
    files = {
        # Keep the semantic graph reviewable while its identity continues to
        # use the compact canonical serialization in graph_instance_id().
        "graph.json": json.dumps(graph, indent=2, sort_keys=True) + "\n",
        "graph.md": _compile_graph_markdown(graph),
        "schema.sql": _compile_schema(graph, schema_name),
        "cardinality_metadata.sql": _compile_cardinality_metadata(
            graph, instance_id, schema_name
        ),
        "base_stats.sql": _compile_base_stats(graph, schema_name),
        "query.sql": _compile_query(graph, schema_name),
    }
    return CompiledCase(instance_id=instance_id, schema_name=schema_name, files=files)


def write_case(case: CompiledCase, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    unexpected = [
        path.name
        for path in output_dir.iterdir()
        if path.is_file()
        and path.name not in CASE_FILENAMES + CASE_DOCUMENTATION_FILENAMES
    ]
    if unexpected:
        raise CompileError(f"output directory contains unexpected files: {', '.join(sorted(unexpected))}")
    for filename in CASE_FILENAMES:
        path = output_dir / filename
        if path.exists() and path.read_text(encoding="utf-8") != case.files[filename]:
            raise CompileError(f"refusing to overwrite differing artifact: {path}")
    for filename in CASE_FILENAMES:
        (output_dir / filename).write_text(case.files[filename], encoding="utf-8")


def verify_case(case: CompiledCase, output_dir: Path) -> None:
    actual_files = (
        {path.name for path in output_dir.iterdir() if path.is_file()}
        if output_dir.exists()
        else set()
    )
    expected_files = set(CASE_FILENAMES)
    generated_files = actual_files - set(CASE_DOCUMENTATION_FILENAMES)
    if generated_files != expected_files:
        missing = sorted(expected_files - generated_files)
        extra = sorted(generated_files - expected_files)
        raise CompileError(f"artifact set mismatch: missing={missing}, extra={extra}")
    for filename in CASE_FILENAMES:
        actual = (output_dir / filename).read_text(encoding="utf-8")
        if actual != case.files[filename]:
            raise CompileError(f"artifact differs from deterministic recompilation: {filename}")


def load_graph(path: Path) -> dict[str, Any]:
    try:
        graph = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CompileError(f"cannot read graph JSON {path}: {exc}") from exc
    validate_graph(graph)
    return graph
