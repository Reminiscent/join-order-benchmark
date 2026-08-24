"""Fail-closed qualification for generated cases and PostgreSQL evidence.

There are two entry points:

* ``qualify_graph()`` exhaustively checks a small offline graph, its compiler
  output, and construction-independent subset formula.
* ``qualify_postgres()`` checks one selected plan, exactcard audit, and restored
  statistics collected by ``plan.py``.

Private helpers group the checks by evidence source. Qualification verifies
the benchmark contract; it does not score competing algorithms.
"""

from __future__ import annotations

import math
import re
import struct
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Mapping

from .compile import CompiledCase, compile_graph, relation_page_counts
from .graph import (
    GenerationSpec,
    edge_endpoint_key_ndv,
    generate_graph,
    graph_instance_id,
    validate_graph,
)
from .cardinality import (
    MAXIMUM_ROWCOUNT,
    clamp_row_est_from_log,
    connected_subsets,
    crossing_edges,
    is_connected_subset,
    recurrence_log_cardinality,
    subset_log_cardinality,
    subset_cardinality,
)
from .plan import PlanEvidence


class QualificationError(AssertionError):
    """Raised when an offline proof obligation fails."""


@dataclass(frozen=True)
class QualificationReport:
    """Counts produced by exhaustive offline qualification of a small graph."""

    instance_id: str
    node_count: int
    edge_count: int
    connected_subset_count: int
    recurrence_check_count: int


@dataclass(frozen=True)
class PostgresQualificationReport:
    """Validated evidence counts and root Total Cost for one PostgreSQL plan."""

    instance_id: str
    base_relation_count: int
    join_record_count: int
    hash_record_count: int
    stats_record_count: int
    root_total_cost: float


FROM_ITEM_RE = re.compile(
    r'^\s*"(?P<schema>[^"]+)"\."r(?P<table>\d{4})"\s+AS\s+"r(?P<alias>\d{4})"\s*,?$',
    re.MULTILINE,
)
PREDICATE_RE = re.compile(
    r'"r(?P<left>\d{4})"\."j_e_(?P<edge_left>\d{4})_(?P<edge_right>\d{4})"\s*=\s*'
    r'"r(?P<right>\d{4})"\."j_e_(?P=edge_left)_(?P=edge_right)"'
)
COLUMN_RE = re.compile(r'^\s*"j_(?P<edge>e_\d{4}_\d{4})"\s+bigint,?$', re.MULTILINE)
PAYLOAD_COLUMN_RE = re.compile(r"^\s*payload\s+text,?$", re.MULTILINE)
PAYLOAD_TARGET_RE = re.compile(
    r'^\s*"r(?P<table>\d{4})"\."payload"\s+AS\s+"payload_(?P<alias>\d{4})"\s*,?$',
    re.MULTILINE,
)
TABLE_RE = re.compile(
    r'CREATE TABLE "(?P<schema>[^"]+)"\."r(?P<table>\d{4})" \(\n'
    r'(?P<body>.*?)\n\) WITH \(',
    re.DOTALL,
)


def _qualify_query(graph: Mapping[str, Any], case: CompiledCase) -> None:
    query = case.files["query.sql"]
    payload_targets = PAYLOAD_TARGET_RE.findall(query)
    expected_ids = [f"{node['id']:04d}" for node in graph["nodes"]]
    if [table for table, _ in payload_targets] != expected_ids or any(
        alias != table for table, alias in payload_targets
    ):
        raise QualificationError("query payload targets do not match canonical nodes")
    from_items = FROM_ITEM_RE.findall(query)
    if [table for _, table, _ in from_items] != expected_ids:
        raise QualificationError("query FROM relations do not match canonical nodes")
    if any(alias != table or schema != case.schema_name for schema, table, alias in from_items):
        raise QualificationError("query aliases or schema are not canonical")

    actual_edges = []
    for match in PREDICATE_RE.finditer(query):
        left = int(match.group("left"))
        right = int(match.group("right"))
        edge_left = int(match.group("edge_left"))
        edge_right = int(match.group("edge_right"))
        if (left, right) != (edge_left, edge_right):
            raise QualificationError("predicate endpoints do not match its edge-specific column")
        actual_edges.append((left, right))
    expected_edges = [(edge["left"], edge["right"]) for edge in graph["edges"]]
    if actual_edges != expected_edges:
        raise QualificationError("query predicates do not exactly match graph edges")


def _qualify_schema(graph: Mapping[str, Any], case: CompiledCase) -> None:
    schema_sql = case.files["schema.sql"]
    forbidden = ("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CREATE INDEX", "INSERT INTO")
    if any(token in schema_sql.upper() for token in forbidden):
        raise QualificationError("schema contains a forbidden constraint, index, or data load")
    expected_columns: dict[int, set[str]] = {node["id"]: set() for node in graph["nodes"]}
    for edge in graph["edges"]:
        expected_columns[edge["left"]].add(edge["id"])
        expected_columns[edge["right"]].add(edge["id"])
    matches = list(TABLE_RE.finditer(schema_sql))
    if len(matches) != len(graph["nodes"]):
        raise QualificationError("schema table count does not match graph nodes")
    for match in matches:
        relation_id = int(match.group("table"))
        actual_columns = {column.group("edge") for column in COLUMN_RE.finditer(match.group("body"))}
        if actual_columns != expected_columns[relation_id]:
            raise QualificationError(f"schema columns for R{relation_id} do not match incident edges")
        if len(PAYLOAD_COLUMN_RE.findall(match.group("body"))) != 1:
            raise QualificationError(f"schema payload for R{relation_id} is missing or duplicated")
        if match.group("schema") != case.schema_name:
            raise QualificationError("schema SQL uses a non-canonical schema name")


def _qualify_oracle(graph: Mapping[str, Any]) -> tuple[int, int]:
    subsets = connected_subsets(graph)
    recurrence_checks = 0
    for subset in subsets:
        direct = subset_log_cardinality(graph, subset)
        wanted = set(subset)
        exact = Fraction(math.prod(graph["nodes"][relid]["base_rows"] for relid in subset), 1)
        for edge in graph["edges"]:
            if edge["left"] in wanted and edge["right"] in wanted:
                exact *= Fraction.from_float(edge["selectivity"])
        exact_log = math.log(exact.numerator) - math.log(exact.denominator)
        if not math.isclose(direct, exact_log, rel_tol=0.0, abs_tol=1e-11):
            raise QualificationError(f"oracle differs from exact rational formula for {subset}")
        if exact <= 1:
            expected_rows = 1.0
        elif exact >= Fraction(10**100, 1):
            expected_rows = MAXIMUM_ROWCOUNT
        else:
            expected_rows = float(round(exact))
        actual_rows = clamp_row_est_from_log(direct)
        if not math.isclose(actual_rows, expected_rows, rel_tol=1e-13, abs_tol=1.0):
            raise QualificationError(f"clamped rows differ from exact formula for {subset}")
        if len(subset) == 1:
            expected = math.log(graph["nodes"][subset[0]]["base_rows"])
            if not math.isclose(direct, expected, rel_tol=0.0, abs_tol=1e-12):
                raise QualificationError("base subset oracle does not equal base rows")
            continue
        anchor = subset[0]
        remaining = subset[1:]
        for mask in range(1 << len(remaining)):
            left = (anchor,) + tuple(
                remaining[index] for index in range(len(remaining)) if mask & (1 << index)
            )
            right = tuple(relid for relid in subset if relid not in left)
            if not right:
                continue
            if not is_connected_subset(graph, left) or not is_connected_subset(graph, right):
                continue
            recurrence = recurrence_log_cardinality(graph, left, right)
            if not math.isclose(recurrence, direct, rel_tol=0.0, abs_tol=1e-11):
                raise QualificationError(f"oracle recurrence differs for subset {subset}")
            recurrence_checks += 1
    return len(subsets), recurrence_checks


def qualify_compiled_graph(
    graph: Mapping[str, Any], case: CompiledCase | None = None
) -> CompiledCase:
    """Qualify deterministic graph/compiler structure without subset enumeration."""
    validate_graph(graph)
    regenerated = generate_graph(GenerationSpec.from_dict(graph["source_spec"]))
    if dict(regenerated) != dict(graph):
        raise QualificationError("graph does not match deterministic regeneration from source_spec")
    compiled = case or compile_graph(graph)
    if compiled.instance_id != graph_instance_id(graph):
        raise QualificationError("compiled instance id does not match canonical graph digest")
    if dict(compiled.files) != dict(compile_graph(graph).files):
        raise QualificationError("compiler output is not deterministic")
    _qualify_schema(graph, compiled)
    _qualify_query(graph, compiled)
    return compiled


def qualify_graph(graph: Mapping[str, Any], case: CompiledCase | None = None) -> QualificationReport:
    """Run exhaustive offline qualification for a graph with at most ten nodes."""

    compiled = qualify_compiled_graph(graph, case)
    if len(graph["nodes"]) > 10:
        raise QualificationError("offline exhaustive qualification is limited to n <= 10")
    subset_count, recurrence_count = _qualify_oracle(graph)
    return QualificationReport(
        instance_id=compiled.instance_id,
        node_count=len(graph["nodes"]),
        edge_count=len(graph["edges"]),
        connected_subset_count=subset_count,
        recurrence_check_count=recurrence_count,
    )


def _close(actual: float, expected: float, *, abs_tol: float = 1.0) -> bool:
    return math.isclose(actual, expected, rel_tol=1e-12, abs_tol=abs_tol)


def _float4(value: float) -> float:
    """Return the value PostgreSQL stores after a real/float4 round trip."""
    return struct.unpack("!f", struct.pack("!f", value))[0]


def _plan_nodes(plan: Mapping[str, Any]):
    yield plan
    for child in plan.get("Plans", ()) or ():
        if isinstance(child, Mapping):
            yield from _plan_nodes(child)


_JOIN_NODE_TYPES = {"Hash Join", "Nested Loop"}


def selected_plan_join_diagnostics(
    graph: Mapping[str, Any], plan: Mapping[str, Any]
) -> tuple[float, ...]:
    """Validate the selected join tree and return non-root cardinality logs."""
    records: list[tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]] = []

    def visit(node: Mapping[str, Any]) -> set[int]:
        node_type = node.get("Node Type")
        relation_name = node.get("Relation Name")
        if node_type == "Seq Scan" and isinstance(relation_name, str):
            match = re.fullmatch(r"r(\d{4})", relation_name)
            if match is None:
                raise QualificationError(
                    f"unexpected Seq Scan relation name: {relation_name}"
                )
            return {int(match.group(1))}

        children = [
            visit(child)
            for child in node.get("Plans", ()) or ()
            if isinstance(child, Mapping)
        ]
        combined: set[int] = set()
        for child in children:
            if combined & child:
                raise QualificationError("EXPLAIN plan scans a relation more than once")
            combined.update(child)
        if node_type in _JOIN_NODE_TYPES:
            relational = [child for child in children if child]
            if len(relational) != 2:
                raise QualificationError(f"{node_type} does not have two relational inputs")
            records.append(
                (
                    tuple(sorted(combined)),
                    tuple(sorted(relational[0])),
                    tuple(sorted(relational[1])),
                )
            )
        return combined

    full_subset = tuple(range(len(graph["nodes"])))
    if visit(plan) != set(full_subset) or len(records) != len(full_subset) - 1:
        raise QualificationError("EXPLAIN plan is not a full binary join tree")

    intermediate_logs = []
    for parent, _left, _right in records:
        parent_log = subset_log_cardinality(graph, parent)
        if parent != full_subset:
            intermediate_logs.append(parent_log)
    return tuple(intermediate_logs)


def _qualify_base_evidence(
    graph: Mapping[str, Any], evidence: PlanEvidence
) -> tuple[int, dict[int, Mapping[str, Any]]]:
    expected_rows = {node["id"]: float(node["base_rows"]) for node in graph["nodes"]}
    expected_pages = relation_page_counts(graph)
    records = {int(item["canonical_id"]): item for item in evidence.audit.get("base", ())}
    if set(records) != set(expected_rows):
        raise QualificationError("exactcard base audit does not cover every canonical relation")
    for relation_id, record in records.items():
        if not _close(float(record["base_rows"]), expected_rows[relation_id]):
            raise QualificationError(f"injected base rows differ for R{relation_id}")
        if int(record["base_pages"]) != expected_pages[relation_id]:
            raise QualificationError(f"injected base pages differ for R{relation_id}")

    scans = {
        int(str(node["Relation Name"])[1:]): node
        for node in _plan_nodes(evidence.explain["Plan"])
        if node.get("Node Type") == "Seq Scan"
        and isinstance(node.get("Relation Name"), str)
        and str(node["Relation Name"]).startswith("r")
    }
    if set(scans) != set(expected_rows):
        raise QualificationError("EXPLAIN does not contain one Seq Scan per graph relation")
    for relation_id, scan in scans.items():
        if not _close(float(scan["Plan Rows"]), expected_rows[relation_id]):
            raise QualificationError(f"Seq Scan Plan Rows differ for R{relation_id}")
        degree = sum(
            relation_id in (edge["left"], edge["right"]) for edge in graph["edges"]
        )
        expected_width = graph["nodes"][relation_id]["payload_width"] + 8 * degree
        if int(scan["Plan Width"]) != expected_width:
            raise QualificationError(f"Seq Scan Plan Width differs for R{relation_id}")
    return len(records), records


def _qualify_join_evidence(graph: Mapping[str, Any], evidence: PlanEvidence) -> tuple[int, int]:
    joins = tuple(evidence.audit.get("joins", ()))
    hashes = tuple(evidence.audit.get("hashjoins", ()))
    if int(evidence.audit.get("join_count", -1)) != len(joins):
        raise QualificationError("join_count does not match trace records")
    if int(evidence.audit.get("hashjoin_count", -1)) != len(hashes):
        raise QualificationError("hashjoin_count does not match trace records")
    if not joins or not hashes:
        raise QualificationError("PostgreSQL qualification produced no join or Hash records")

    for record in joins:
        subset = tuple(int(value) for value in record["relations"])
        expected_log = subset_log_cardinality(graph, subset)
        expected_rows = subset_cardinality(graph, subset)
        if not math.isclose(float(record["log_card"]), expected_log, rel_tol=1e-12, abs_tol=1e-11):
            raise QualificationError(f"join log cardinality differs for subset {subset}")
        if not _close(float(record["formula_rows"]), expected_rows):
            raise QualificationError(f"join rows differ for subset {subset}")
        crossing = crossing_edges(graph, record["outer"], record["inner"])
        if len(record["crossing"]) != len(crossing):
            raise QualificationError(f"join crossing edge count differs for subset {subset}")
        crossing_records = {
            (int(item["left"]), int(item["right"])): item
            for item in record["crossing"]
        }
        expected_crossing = {
            (int(edge["left"]), int(edge["right"])): edge for edge in crossing
        }
        if set(crossing_records) != set(expected_crossing):
            raise QualificationError(f"join crossing edges differ for subset {subset}")
        for key, edge in expected_crossing.items():
            crossing_record = crossing_records[key]
            if (
                int(crossing_record["left_key_ndv"]) != edge["left_key_ndv"]
                or int(crossing_record["right_key_ndv"])
                != edge["right_key_ndv"]
            ):
                raise QualificationError(
                    f"join crossing endpoint NDVs differ for subset {subset}"
                )
            if not math.isclose(
                float(crossing_record["selectivity"]),
                edge["selectivity"],
                rel_tol=1e-12,
                abs_tol=1e-15,
            ):
                raise QualificationError(
                    f"join crossing selectivity differs for subset {subset}"
                )

    bucket_cache: dict[tuple[str, int], float] = {}
    for record in hashes:
        subset = tuple(int(value) for value in record["relations"])
        outer = tuple(int(value) for value in record["outer"])
        inner = tuple(int(value) for value in record["inner"])
        if set(outer) | set(inner) != set(subset) or set(outer) & set(inner):
            raise QualificationError(f"Hash input subsets do not partition {subset}")
        crossing = crossing_edges(graph, outer, inner)
        if int(record["crossing_count"]) != len(crossing):
            raise QualificationError(f"Hash crossing count differs for subset {subset}")
        if not crossing:
            raise QualificationError(f"Hash subset {subset} has no crossing edge")
        if not _close(float(record["outer_rows"]), subset_cardinality(graph, outer)):
            raise QualificationError(f"Hash outer rows differ for subset {subset}")
        if not _close(float(record["inner_rows"]), subset_cardinality(graph, inner)):
            raise QualificationError(f"Hash inner rows differ for subset {subset}")
        if len(crossing) == 1:
            edge = crossing[0]
            inner_endpoint = edge["left"] if edge["left"] in inner else edge["right"]
            inner_key_ndv = edge_endpoint_key_ndv(edge, inner_endpoint)
            if int(record["key_ndv"]) != inner_key_ndv:
                raise QualificationError(f"Hash crossing key NDV differs for subset {subset}")
            if not math.isclose(
                float(record["edge_selectivity"]),
                edge["selectivity"],
                rel_tol=1e-12,
                abs_tol=1e-15,
            ):
                raise QualificationError(f"Hash edge selectivity differs for subset {subset}")
            if int(record["inner_endpoint"]) != inner_endpoint:
                raise QualificationError(f"Hash inner endpoint differs for subset {subset}")
            selectivity = 1.0 / inner_key_ndv
            if not math.isclose(
                float(record["innermcvfreq"]),
                selectivity,
                rel_tol=1e-6,
                abs_tol=1e-12,
            ):
                raise QualificationError(f"Hash MCV frequency differs for subset {subset}")
            virtual_buckets = int(record["numbuckets"]) * int(record["numbatches"])
            expected_bucket = max(1.0 / virtual_buckets, selectivity)
            cache_key = (edge["id"], inner_endpoint)
            actual_bucket = float(record["innerbucketsize"])
            if record["bucket_stats_computed"]:
                if not math.isclose(
                    actual_bucket, expected_bucket, rel_tol=1e-6, abs_tol=1e-12
                ):
                    raise QualificationError(
                        f"new Hash bucket size differs for subset {subset}"
                    )
                bucket_cache[cache_key] = actual_bucket
            else:
                cached = bucket_cache.get(cache_key)
                if cached is None or not math.isclose(
                    actual_bucket, cached, rel_tol=1e-12, abs_tol=1e-15
                ):
                    raise QualificationError(
                        f"cached Hash bucket size differs for subset {subset}"
                    )
        expected_rows = subset_cardinality(graph, subset)
        if not _close(float(record["hashjointuples"]), expected_rows):
            raise QualificationError(f"Hash tuple count differs for subset {subset}")

    full_subset = tuple(range(len(graph["nodes"])))
    root_rows = subset_cardinality(graph, full_subset)
    if not _close(float(evidence.audit["final_rows"]), root_rows):
        raise QualificationError("exactcard final rows differ from the full-subset oracle")
    if not _close(float(evidence.explain["Plan"]["Plan Rows"]), root_rows):
        raise QualificationError("root Plan Rows differ from the full-subset oracle")
    return len(joins), len(hashes)


def _qualify_summary_evidence(
    graph: Mapping[str, Any], evidence: PlanEvidence
) -> tuple[int, int]:
    """Check the bounded audit evidence used by the main matrix."""
    join_count = int(evidence.audit.get("join_count", -1))
    hash_count = int(evidence.audit.get("hashjoin_count", -1))
    if join_count <= 0 or hash_count < 0:
        raise QualificationError("summary audit produced invalid activity counts")

    full_subset = tuple(range(len(graph["nodes"])))
    root_rows = subset_cardinality(graph, full_subset)
    if not _close(float(evidence.audit["final_rows"]), root_rows):
        raise QualificationError("exactcard final rows differ from the full-subset oracle")
    if not _close(float(evidence.explain["Plan"]["Plan Rows"]), root_rows):
        raise QualificationError("root Plan Rows differ from the full-subset oracle")
    selected_plan_join_diagnostics(graph, evidence.explain["Plan"])
    return join_count, hash_count


def _qualify_stats(graph: Mapping[str, Any], evidence: PlanEvidence) -> int:
    records = {
        (str(item["relation"]), str(item["column"])): item for item in evidence.stats
    }
    expected: dict[tuple[str, str], tuple[str, int]] = {}
    for node in graph["nodes"]:
        expected[(f"r{node['id']:04d}", "payload")] = (
            "payload",
            node["payload_width"],
        )
    for edge in graph["edges"]:
        column = f"j_{edge['id']}"
        expected[(f"r{edge['left']:04d}", column)] = ("key", edge["left_key_ndv"])
        expected[(f"r{edge['right']:04d}", column)] = ("key", edge["right_key_ndv"])
    if set(records) != set(expected):
        raise QualificationError("pg_stats columns do not exactly match graph columns")
    for key, (kind, value) in expected.items():
        record = records[key]
        expected_width = 8 if kind == "key" else value
        if (
            float(record["null_frac"]) != 0.0
            or int(record["avg_width"]) != expected_width
        ):
            raise QualificationError(f"stats width/null contract differs for {key}")
        if kind == "payload":
            if float(record["n_distinct"]) != -1.0:
                raise QualificationError(f"payload n_distinct differs for {key}")
            if record.get("most_common_freqs") is not None:
                raise QualificationError(f"unexpected payload MCV for {key}")
            if record.get("histogram_bounds") is not None:
                raise QualificationError(f"unexpected payload histogram for {key}")
            continue
        key_ndv = value
        if float(record["n_distinct"]) != _float4(float(key_ndv)):
            raise QualificationError(f"n_distinct differs for {key}")
        frequencies = record.get("most_common_freqs")
        if not isinstance(frequencies, list) or len(frequencies) != 1:
            raise QualificationError(f"representative MCV is missing for {key}")
        if not math.isclose(
            float(frequencies[0]), 1.0 / key_ndv, rel_tol=1e-6, abs_tol=1e-12
        ):
            raise QualificationError(f"representative MCV frequency differs for {key}")
        if record.get("histogram_bounds") is not None:
            raise QualificationError(f"unexpected histogram for {key}")
    return len(records)


def qualify_postgres(
    graph: Mapping[str, Any], case: CompiledCase, evidence: PlanEvidence
) -> PostgresQualificationReport:
    """Check the PostgreSQL evidence required by the selected audit tier."""
    qualify_compiled_graph(graph, case)
    try:
        if evidence.audit.get("instance_id") != case.instance_id:
            raise QualificationError("exactcard audit instance id differs from the compiled case")
        base_count, _ = _qualify_base_evidence(graph, evidence)
        if "joins" in evidence.audit or "hashjoins" in evidence.audit:
            if "joins" not in evidence.audit or "hashjoins" not in evidence.audit:
                raise QualificationError("trace audit is missing join or Hash records")
            join_count, hash_count = _qualify_join_evidence(graph, evidence)
        else:
            join_count, hash_count = _qualify_summary_evidence(graph, evidence)
        stats_count = _qualify_stats(graph, evidence)
        root_total_cost = float(evidence.explain["Plan"]["Total Cost"])
        if not math.isfinite(root_total_cost) or root_total_cost <= 0.0:
            raise QualificationError("root Total Cost must be finite and positive")
        return PostgresQualificationReport(
            instance_id=case.instance_id,
            base_relation_count=base_count,
            join_record_count=join_count,
            hash_record_count=hash_count,
            stats_record_count=stats_count,
            root_total_cost=root_total_cost,
        )
    except KeyError as exc:
        raise QualificationError(
            f"PostgreSQL qualification evidence is missing field {exc.args[0]!r}"
        ) from exc
    except (TypeError, ValueError, OverflowError) as exc:
        raise QualificationError(
            f"PostgreSQL qualification evidence contains an invalid numeric value: {exc}"
        ) from exc
