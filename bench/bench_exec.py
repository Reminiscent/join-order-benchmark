"""PostgreSQL execution helpers for benchmark runs.

``run_one()`` is the primary entry point: it creates a clean session prelude,
runs one measured ``EXPLAIN ANALYZE`` statement, and returns the timing/cost
fields consumed by ``bench_run.py``.  The remaining helpers validate the
PostgreSQL execution environment, stabilize database statistics, and parse
``psql`` output.
"""

from __future__ import annotations

import functools
import json
from dataclasses import dataclass
from typing import Any, Optional

from bench_common import (
    ConnOpts,
    Scenario,
    Variant,
    die,
    psql_cmd,
    psql_sql,
    psql_sql_raw,
    run_cmd,
    sql_literal,
)


@dataclass(frozen=True)
class RunMetrics:
    """Timing and optimizer-cost fields extracted from EXPLAIN JSON output."""

    planning_ms: float
    execution_ms: float
    total_ms: float
    plan_total_cost: float


class StatementTimeoutError(RuntimeError):
    """Raised when PostgreSQL cancels a statement due to statement_timeout."""


def run_one(
    db: str,
    scenario_session_gucs: tuple[tuple[str, Any], ...],
    variant: Variant,
    stmt: str,
    *,
    conn: Optional[ConnOpts] = None,
    statement_timeout_ms: int,
) -> RunMetrics:
    """Run one benchmark statement in a clean PostgreSQL session.

    The generated script resets the session, applies the scenario GUCs, applies
    the selected variant GUCs, applies optional variant GUCs only when the
    server supports them, and finally runs EXPLAIN JSON.  A PostgreSQL
    statement_timeout is reported as ``StatementTimeoutError`` so the run
    driver can classify it separately from other execution errors.
    """

    script_lines = [
        *build_session_prelude(db, conn, scenario_session_gucs, variant, statement_timeout_ms)
    ]
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
    """Fail early when a prepared benchmark database cannot be reached."""

    for db in dbs:
        p = run_cmd(psql_cmd(db, conn) + ["-At"], input_text="SELECT 1;\n", check=False)
        if p.returncode == 0:
            continue
        out = (p.stdout or "") + (p.stderr or "")
        die(
            f"cannot connect to benchmark database '{db}': "
            f"{first_error_line(out) or 'connection failed'}. "
            "Run prepare first or fix the PostgreSQL connection flags."
        )


def validate_required_gucs(
    db: str,
    conn: Optional[ConnOpts],
    scenario: Scenario,
    variants_registry: dict[str, Variant],
    variant_names: tuple[str, ...],
) -> None:
    """Verify that mandatory scenario and variant GUCs exist on the server."""

    missing_scenario = [
        name for name, _ in scenario.session_gucs if current_setting(db, name, conn) is None
    ]
    if missing_scenario:
        die(
            f"scenario '{scenario.name}' requires unsupported PostgreSQL parameter(s): "
            f"{', '.join(sorted(missing_scenario))}"
        )

    missing_by_variant: list[str] = []
    for variant_name in variant_names:
        variant = variants_registry[variant_name]
        missing = [
            name
            for name, _ in variant.session_gucs
            if current_setting(db, name, conn) is None
        ]
        if missing:
            missing_by_variant.append(f"{variant_name}: {', '.join(sorted(missing))}")

    if missing_by_variant:
        die(
            "selected variant(s) require unsupported PostgreSQL parameter(s): "
            + " | ".join(missing_by_variant)
        )


def resolved_variant_session_gucs(
    db: str,
    conn: Optional[ConnOpts],
    variant: Variant,
) -> tuple[tuple[str, object], ...]:
    """Return the variant GUCs that will actually be applied on this server."""

    resolved = list(variant.session_gucs)
    for name, value in variant.optional_session_gucs:
        if guc_exists(db, conn, name):
            resolved.append((name, value))
    return tuple(resolved)


def build_session_prelude(
    db: str,
    conn: Optional[ConnOpts],
    scenario_session_gucs: tuple[tuple[str, Any], ...],
    variant: Variant,
    statement_timeout_ms: int,
) -> list[str]:
    """Build the psql script prefix used before each EXPLAIN statement."""

    lines = ["RESET ALL;"]
    lines.append(f"SET statement_timeout = {statement_timeout_ms};")
    lines.extend(f"SET {k} = {sql_literal(v)};" for k, v in scenario_session_gucs)
    lines.extend(f"SET {k} = {sql_literal(v)};" for k, v in variant.session_gucs)
    for k, v in variant.optional_session_gucs:
        if guc_exists(db, conn, k):
            lines.append(f"SET {k} = {sql_literal(v)};")
    return lines


def explain_sql(stmt: str) -> str:
    """Wrap a benchmark query in the EXPLAIN mode used by public artifacts."""

    return f"EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON) {stmt}"


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
    )


def current_setting(db: str, name: str, conn: Optional[ConnOpts] = None) -> Optional[str]:
    """Return a PostgreSQL setting value, or None when the setting is absent."""

    sql = f"SELECT current_setting({sql_literal(name)}, true);\n"
    p = run_cmd(psql_cmd(db, conn) + ["-At"], input_text=sql, check=False)
    if p.returncode != 0:
        return None
    value = (p.stdout or "").strip()
    return value or None


@functools.lru_cache(maxsize=None)
def guc_exists(db: str, conn: Optional[ConnOpts], name: str) -> bool:
    """Return whether the server recognizes a GUC name."""

    return current_setting(db, name, conn) is not None


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
