from __future__ import annotations

import functools
import json
from dataclasses import dataclass
from typing import Any, Optional

from bench_common import ConnOpts, Variant, die, psql_cmd, psql_sql, psql_sql_raw, run_cmd, sql_literal


@dataclass(frozen=True)
class RunMetrics:
    planning_ms: float
    execution_ms: float
    total_ms: float
    plan_total_cost: float


class StatementTimeoutError(RuntimeError):
    """Raised when PostgreSQL cancels a statement due to statement_timeout."""


def first_error_line(output: str) -> str:
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
    return "statement timeout" in message.lower()


def current_setting(db: str, name: str, conn: Optional[ConnOpts] = None) -> Optional[str]:
    sql = f"SELECT current_setting({sql_literal(name)}, true);\n"
    p = run_cmd(psql_cmd(db, conn) + ["-At"], input_text=sql, check=False)
    if p.returncode != 0:
        return None
    value = (p.stdout or "").strip()
    return value or None


@functools.lru_cache(maxsize=None)
def guc_exists(db: str, conn: Optional[ConnOpts], name: str) -> bool:
    return current_setting(db, name, conn) is not None


def build_session_prelude(
    db: str,
    conn: Optional[ConnOpts],
    scenario_session_gucs: tuple[tuple[str, Any], ...],
    variant: Variant,
    statement_timeout_ms: Optional[int],
) -> list[str]:
    lines = ["RESET ALL;"]
    if statement_timeout_ms is not None and statement_timeout_ms > 0:
        lines.append(f"SET statement_timeout = {statement_timeout_ms};")
    lines.extend(f"SET {k} = {sql_literal(v)};" for k, v in scenario_session_gucs)
    lines.extend(f"SET {k} = {sql_literal(v)};" for k, v in variant.session_gucs)
    for k, v in variant.optional_session_gucs:
        if guc_exists(db, conn, k):
            lines.append(f"SET {k} = {sql_literal(v)};")
    return lines


def explain_sql(stmt: str) -> str:
    return f"EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON) {stmt}"


def parse_explain_json(payload: str) -> RunMetrics:
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


def run_one(
    db: str,
    scenario_session_gucs: tuple[tuple[str, Any], ...],
    variant: Variant,
    stmt: str,
    conn: Optional[ConnOpts] = None,
    statement_timeout_ms: Optional[int] = None,
) -> RunMetrics:
    script_lines = [*build_session_prelude(db, conn, scenario_session_gucs, variant, statement_timeout_ms)]
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
    mode: str,
    conn: Optional[ConnOpts] = None,
) -> None:
    if mode == "vacuum_freeze_analyze":
        psql_sql(db, "VACUUM FREEZE ANALYZE;", conn=conn, check=True)
        psql_sql(db, "CHECKPOINT;", conn=conn, check=False)
        return
    if mode == "none":
        return
    die(f"unknown stabilize mode: {mode}")


def rotate_variants(variants: list[Variant], offset: int) -> list[Variant]:
    if not variants:
        return []
    normalized = offset % len(variants)
    if normalized == 0:
        return list(variants)
    return list(variants[normalized:]) + list(variants[:normalized])
