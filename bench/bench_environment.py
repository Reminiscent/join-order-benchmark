from __future__ import annotations

import functools
from typing import Optional

from bench_common import ConnOpts, Scenario, Variant, psql_cmd, run_cmd, sql_literal, die


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


def ensure_databases_reachable(dbs: list[str], conn: Optional[ConnOpts] = None) -> None:
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


def resolved_variant_session_gucs(
    db: str,
    conn: Optional[ConnOpts],
    variant: Variant,
) -> tuple[tuple[str, object], ...]:
    resolved = list(variant.session_gucs)
    for name, value in variant.optional_session_gucs:
        if guc_exists(db, conn, name):
            resolved.append((name, value))
    return tuple(resolved)


def validate_required_gucs(
    db: str,
    conn: Optional[ConnOpts],
    scenario: Scenario,
    variants_registry: dict[str, Variant],
    variant_names: tuple[str, ...],
) -> None:
    missing_scenario = [name for name, _ in scenario.session_gucs if current_setting(db, name, conn) is None]
    if missing_scenario:
        die(
            f"scenario '{scenario.name}' requires unsupported PostgreSQL parameter(s): "
            f"{', '.join(sorted(missing_scenario))}"
        )

    missing_by_variant: list[str] = []
    for variant_name in variant_names:
        variant = variants_registry[variant_name]
        missing = [name for name, _ in variant.session_gucs if current_setting(db, name, conn) is None]
        if missing:
            missing_by_variant.append(f"{variant_name}: {', '.join(sorted(missing))}")

    if missing_by_variant:
        die(
            "selected variant(s) require unsupported PostgreSQL parameter(s): "
            + " | ".join(missing_by_variant)
        )
