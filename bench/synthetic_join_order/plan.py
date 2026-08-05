"""Install one compiled case and collect plan-only PostgreSQL evidence.

The public flow is ``install_case() -> plan_case() -> uninstall_case()``.
``plan_case()`` runs one psql session that applies the benchmark settings,
issues ``EXPLAIN`` without execution, and captures three JSON values:
PostgreSQL's selected plan, exactcard's audit, and the restored ``pg_stats``
rows. This module collects evidence; ``qualify.py`` decides whether it is
valid.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from bench_common import ConnOpts, psql_sql_raw, sql_literal
from bench_exec import first_error_line
from .compile import CompiledCase


PLAN_BEGIN = "__SYNTHETIC_JOIN_ORDER_PLAN_BEGIN__"
AUDIT_BEGIN = "__SYNTHETIC_JOIN_ORDER_AUDIT_BEGIN__"
STATS_BEGIN = "__SYNTHETIC_JOIN_ORDER_STATS_BEGIN__"
OUTPUT_END = "__SYNTHETIC_JOIN_ORDER_OUTPUT_END__"

BENCHMARK_GUCS = (
    ("join_collapse_limit", 10000),
    ("from_collapse_limit", 10000),
    ("max_parallel_workers_per_gather", 0),
    ("jit", "off"),
    ("enable_mergejoin", "off"),
)

METADATA_DDL = """\
CREATE SCHEMA bench;
CREATE TABLE bench.join_order_relation (
    instance_id text NOT NULL,
    canonical_id integer NOT NULL,
    relation regclass NOT NULL,
    base_rows double precision NOT NULL,
    base_pages bigint NOT NULL,
    PRIMARY KEY (instance_id, canonical_id),
    UNIQUE (instance_id, relation),
    CHECK (base_rows > 0),
    CHECK (base_pages > 0)
);
CREATE TABLE bench.join_order_edge (
    instance_id text NOT NULL,
    left_id integer NOT NULL,
    right_id integer NOT NULL,
    left_key_ndv bigint NOT NULL,
    right_key_ndv bigint NOT NULL,
    selectivity double precision NOT NULL,
    PRIMARY KEY (instance_id, left_id, right_id),
    CHECK (left_id < right_id),
    CHECK (left_key_ndv > 0),
    CHECK (right_key_ndv > 0),
    CHECK (selectivity > 0 AND selectivity <= 1)
);
"""


class PlanDriverError(RuntimeError):
    """Raised when PostgreSQL setup, planning, or output parsing fails."""


class PlanTimeoutError(PlanDriverError):
    """Raised when PostgreSQL cancels plan construction at statement timeout."""


@dataclass(frozen=True)
class PlanEvidence:
    """The plan, provider audit, and column statistics from one psql session."""

    explain: Mapping[str, Any]
    audit: Mapping[str, Any]
    stats: tuple[Mapping[str, Any], ...]


def _run_sql(db: str, sql: str, conn: ConnOpts | None) -> str:
    result = psql_sql_raw(db, sql, conn=conn, extra_args=["-A", "-t"], check=False)
    if result.returncode != 0:
        output = (result.stdout or "") + (result.stderr or "")
        message = first_error_line(output) or "psql command failed"
        if "statement timeout" in message.lower():
            raise PlanTimeoutError(message)
        raise PlanDriverError(message)
    return result.stdout or ""


def bootstrap_metadata(db: str, conn: ConnOpts | None = None) -> None:
    """Install the shared metadata schema into a clean qualification DB."""
    _run_sql(db, METADATA_DDL, conn)


def postgres_server_version(db: str, conn: ConnOpts | None = None) -> str:
    """Return the version reported by the PostgreSQL server being benchmarked."""
    version = _run_sql(db, "SHOW server_version;", conn).strip()
    if not version:
        raise PlanDriverError("PostgreSQL server returned an empty version")
    return version


def install_case(db: str, case: CompiledCase, conn: ConnOpts | None = None) -> None:
    """Install one generated case atomically; existing objects fail closed."""
    script = "\n".join(
        (
            "BEGIN;",
            case.files["schema.sql"],
            case.files["cardinality_metadata.sql"],
            case.files["base_stats.sql"],
            "COMMIT;",
            "",
        )
    )
    _run_sql(db, script, conn)


def uninstall_case(db: str, case: CompiledCase, conn: ConnOpts | None = None) -> None:
    """Remove one successfully processed case and its control metadata."""
    instance = sql_literal(case.instance_id)
    schema = '"' + case.schema_name.replace('"', '""') + '"'
    script = "\n".join(
        (
            "BEGIN;",
            f"DELETE FROM bench.join_order_edge WHERE instance_id = {instance};",
            f"DELETE FROM bench.join_order_relation WHERE instance_id = {instance};",
            f"DROP SCHEMA {schema} CASCADE;",
            "COMMIT;",
            "",
        )
    )
    _run_sql(db, script, conn)


def build_plan_script(
    case: CompiledCase,
    *,
    statement_timeout_ms: int = 60_000,
    variant_gucs: tuple[tuple[str, Any], ...] = (),
    audit_mode: str = "summary",
) -> str:
    """Build the single-session SQL script that produces ``PlanEvidence``."""
    if statement_timeout_ms <= 0:
        raise ValueError("statement_timeout_ms must be positive")
    if audit_mode not in ("summary", "trace"):
        raise ValueError(f"unsupported exactcard audit mode: {audit_mode}")
    query = case.files["query.sql"].strip()
    schema = sql_literal(case.schema_name)
    instance = sql_literal(case.instance_id)
    lines = [
        "RESET ALL;",
        f"SET statement_timeout = {statement_timeout_ms};",
    ]
    lines.extend(f"SET {name} = {sql_literal(value)};" for name, value in BENCHMARK_GUCS)
    # Variant definitions are trusted benchmark configuration.  They are
    # intentionally applied last so an experiment can override a common GUC;
    # run.json records the exact per-variant settings for review.
    lines.extend(f"SET {name} = {sql_literal(value)};" for name, value in variant_gucs)
    lines.extend(
        [
            "SET exactcard.mode = formula;",
            f"SET exactcard.instance_id = {instance};",
            f"SET exactcard.audit = {audit_mode};",
            f"\\echo {PLAN_BEGIN}",
            "EXPLAIN (ANALYZE FALSE, FORMAT JSON, SETTINGS ON) " + query,
            f"\\echo {AUDIT_BEGIN}",
            "SELECT pg_catalog.exactcard_last_audit();",
            "SET exactcard.mode = off;",
            f"\\echo {STATS_BEGIN}",
            "SELECT COALESCE(jsonb_agg(jsonb_build_object("
            "'relation', tablename, 'column', attname, 'null_frac', null_frac, "
            "'avg_width', avg_width, 'n_distinct', n_distinct, "
            "'most_common_freqs', most_common_freqs, "
            "'histogram_bounds', histogram_bounds::text) "
            "ORDER BY tablename, attname), '[]'::jsonb) "
            "FROM pg_catalog.pg_stats WHERE schemaname = "
            + schema
            + ";",
            f"\\echo {OUTPUT_END}",
            "",
        ]
    )
    return "\n".join(lines)


def _section(output: str, begin: str, end: str) -> str:
    try:
        value = output.split(begin, 1)[1].split(end, 1)[0].strip()
    except IndexError as exc:
        raise PlanDriverError(f"missing psql output marker {begin}") from exc
    if not value:
        raise PlanDriverError(f"empty psql output section {begin}")
    return value


def parse_plan_output(output: str) -> PlanEvidence:
    """Parse the three marker-delimited JSON values emitted by psql."""
    try:
        explain_raw = json.loads(_section(output, PLAN_BEGIN, AUDIT_BEGIN))
        audit = json.loads(_section(output, AUDIT_BEGIN, STATS_BEGIN))
        stats = json.loads(_section(output, STATS_BEGIN, OUTPUT_END))
    except json.JSONDecodeError as exc:
        raise PlanDriverError(f"invalid PostgreSQL qualification JSON: {exc}") from exc
    if not isinstance(explain_raw, list) or len(explain_raw) != 1:
        raise PlanDriverError("EXPLAIN JSON must contain exactly one root")
    explain = explain_raw[0]
    if not isinstance(explain, dict) or not isinstance(explain.get("Plan"), dict):
        raise PlanDriverError("EXPLAIN JSON is missing its Plan root")
    if not isinstance(audit, dict) or audit.get("active") is not True:
        raise PlanDriverError("exactcard audit is not active")
    if not isinstance(stats, list) or not all(isinstance(item, dict) for item in stats):
        raise PlanDriverError("pg_stats evidence must be a JSON array of objects")
    return PlanEvidence(explain=explain, audit=audit, stats=tuple(stats))


def plan_case(
    db: str,
    case: CompiledCase,
    conn: ConnOpts | None = None,
    *,
    statement_timeout_ms: int = 60_000,
    variant_gucs: tuple[tuple[str, Any], ...] = (),
    audit_mode: str = "summary",
) -> PlanEvidence:
    """Plan one case without execution and capture audit in the same session."""
    output = _run_sql(
        db,
        build_plan_script(
            case,
            statement_timeout_ms=statement_timeout_ms,
            variant_gucs=variant_gucs,
            audit_mode=audit_mode,
        ),
        conn,
    )
    return parse_plan_output(output)
