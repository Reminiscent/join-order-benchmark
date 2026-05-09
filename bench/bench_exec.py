"""PostgreSQL execution helpers for benchmark runs.

This module owns per-statement session setup, ``EXPLAIN ANALYZE`` execution,
GUC validation, statistics refresh, and parsing of ``psql`` output.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from bench_common import (
    ConnOpts,
    Variant,
    die,
    psql_cmd,
    psql_sql,
    psql_sql_raw,
    run_cmd,
    sql_literal,
)


# Data returned to bench_run.py.


@dataclass(frozen=True)
class RunMetrics:
    """Timing, optimizer-cost, and raw EXPLAIN JSON output for one run."""

    planning_ms: float
    execution_ms: float
    total_ms: float
    plan_total_cost: float
    explain_json: str = ""


class StatementTimeoutError(RuntimeError):
    """Raised when PostgreSQL cancels a statement due to statement_timeout."""


# Main statement execution.


def run_one_statement(
    db: str,
    run_session_gucs: tuple[tuple[str, Any], ...],
    variant: Variant,
    stmt: str,
    *,
    conn: Optional[ConnOpts] = None,
) -> RunMetrics:
    """Run one benchmark statement in a clean PostgreSQL session.

    The generated script resets the session, applies the shared run GUCs,
    applies the selected variant GUCs, and finally runs EXPLAIN JSON.  A PostgreSQL
    statement_timeout is reported as ``StatementTimeoutError`` so the run
    driver can classify it separately from other execution errors.
    """

    script_lines = [*build_session_prelude(run_session_gucs, variant)]
    script_lines.extend([explain_sql(stmt), ""])

    script = "\n".join(script_lines)
    p = psql_sql_raw(db, script, conn=conn, extra_args=["-A", "-t"], check=False)
    stdout = p.stdout or ""
    stderr = p.stderr or ""
    out = stdout + stderr
    if p.returncode != 0:
        message = first_error_line(out) or "query failed"
        if is_statement_timeout_error(message):
            raise StatementTimeoutError(message)
        raise RuntimeError(message)

    payload = stdout.strip()
    if not payload:
        raise RuntimeError("missing EXPLAIN JSON output")
    return parse_explain_json(payload)


# Per-statement session script construction.


def build_session_prelude(
    run_session_gucs: tuple[tuple[str, Any], ...],
    variant: Variant,
) -> list[str]:
    """Build the psql script prefix used before each EXPLAIN statement."""

    lines = ["RESET ALL;"]
    lines.extend(set_guc_lines(run_session_gucs))
    lines.extend(set_guc_lines(variant.session_gucs))
    return lines


def set_guc_lines(gucs: tuple[tuple[str, Any], ...]) -> list[str]:
    """Render session GUC assignments as psql script lines."""

    return [f"SET {k} = {sql_literal(v)};" for k, v in gucs]


def explain_sql(stmt: str) -> str:
    """Wrap a benchmark query in the EXPLAIN mode used by public artifacts."""

    return f"EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON) {stmt}"


# EXPLAIN JSON parsing.


def parse_explain_json(payload: str) -> RunMetrics:
    """Extract timing and cost metrics from PostgreSQL EXPLAIN JSON output."""

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"invalid EXPLAIN JSON output: {e}") from e

    if isinstance(parsed, list):
        if not parsed:
            raise RuntimeError("empty EXPLAIN JSON output")
        root = parsed[0]
    elif isinstance(parsed, dict):
        root = parsed
    else:
        raise RuntimeError("unexpected EXPLAIN JSON payload type")

    if not isinstance(root, dict):
        raise RuntimeError("unexpected EXPLAIN JSON root structure")

    plan = root.get("Plan")
    if not isinstance(plan, dict):
        raise RuntimeError("missing Plan in EXPLAIN JSON output")

    planning_raw = root.get("Planning Time")
    if planning_raw is None:
        raise RuntimeError("missing Planning Time in EXPLAIN JSON output")
    planning_ms = float(planning_raw)

    cost_raw = plan.get("Total Cost")
    if cost_raw is None:
        raise RuntimeError("missing Total Cost in EXPLAIN JSON output")
    plan_total_cost = float(cost_raw)

    execution_raw = root.get("Execution Time")
    if execution_raw is None:
        raise RuntimeError("missing Execution Time in EXPLAIN JSON output")
    execution_ms = float(execution_raw)
    total_ms = planning_ms + execution_ms

    return RunMetrics(
        planning_ms=planning_ms,
        execution_ms=execution_ms,
        total_ms=total_ms,
        plan_total_cost=plan_total_cost,
        explain_json=payload,
    )


# Run setup and validation.


def stabilize_db(
    db: str,
    conn: Optional[ConnOpts] = None,
) -> None:
    """Create the statistics snapshot used by a fresh benchmark run.

    ``VACUUM FREEZE ANALYZE`` refreshes table statistics and freezes tuples so
    measured queries are not mixed with autovacuum-style maintenance effects.
    ``CHECKPOINT`` is best-effort because some environments do not allow it, and
    failure there should not hide an otherwise usable benchmark database.
    """

    psql_sql(db, "VACUUM FREEZE ANALYZE;", conn=conn, check=True)
    psql_sql(db, "CHECKPOINT;", conn=conn, check=False)


def ensure_databases_reachable(dbs: list[str], conn: Optional[ConnOpts] = None) -> None:
    """Fail early when a benchmark database cannot be reached."""

    for db in dbs:
        p = run_cmd(psql_cmd(db, conn) + ["-At"], input_text="SELECT 1;\n", check=False)
        if p.returncode == 0:
            continue
        out = (p.stdout or "") + (p.stderr or "")
        die(
            f"cannot connect to benchmark database '{db}': "
            f"{first_error_line(out) or 'connection failed'}. "
            "Run prepare first, reuse an existing database, or fix the PostgreSQL connection flags."
        )


def validate_session_gucs(
    db: str,
    conn: Optional[ConnOpts],
    run_session_gucs: tuple[tuple[str, Any], ...],
    variants_registry: dict[str, Variant],
    variant_names: tuple[str, ...],
) -> None:
    """Fail before measurement if configured shared or variant GUCs are invalid."""

    validate_guc_assignments(db, conn, "benchmark settings", run_session_gucs)
    for variant_name in variant_names:
        validate_guc_assignments(
            db,
            conn,
            f"variant '{variant_name}'",
            variants_registry[variant_name].session_gucs,
        )


def validate_guc_assignments(
    db: str,
    conn: Optional[ConnOpts],
    source: str,
    gucs: tuple[tuple[str, Any], ...],
) -> None:
    """Fail before measurement if PostgreSQL rejects a configured SET value."""

    if not gucs:
        return

    script = "\n".join(["RESET ALL;", *set_guc_lines(gucs), ""])
    p = psql_sql_raw(db, script, conn=conn, check=False)
    if p.returncode == 0:
        return

    out = (p.stdout or "") + (p.stderr or "")
    die(
        f"invalid PostgreSQL setting assignment(s) in {source}: "
        f"{first_error_line(out) or 'SET failed'}"
    )


# psql error classification.


def first_error_line(output: str) -> str:
    """Pick the most useful one-line error from psql stdout/stderr."""

    for line in output.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("ERROR:") or s.startswith("FATAL:") or s.startswith("psql:"):
            return s
    for line in output.splitlines():
        s = line.strip()
        if s:
            return s
    return ""


def is_statement_timeout_error(message: str) -> bool:
    """Return whether an error message represents PostgreSQL statement_timeout."""

    return "statement timeout" in message.lower()
