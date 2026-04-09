from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "meta" / "query_manifest.csv"
VARIANTS_CONFIG_PATH = REPO_ROOT / "config" / "variants.toml"
SCENARIOS_CONFIG_PATH = REPO_ROOT / "config" / "scenarios.toml"
OUTPUTS_DIR = REPO_ROOT / "outputs"

SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class QueryMeta:
    dataset: str
    query_id: str
    query_path: str
    query_label: str
    join_size: int


@dataclass(frozen=True)
class Variant:
    name: str
    label: str
    session_gucs: tuple[tuple[str, Any], ...]
    optional_session_gucs: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class DatasetSpec:
    dataset: str
    min_join: Optional[int] = None
    max_join: Optional[int] = None
    max_queries: Optional[int] = None
    variants: Optional[tuple[str, ...]] = None


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    default_variants: tuple[str, ...]
    reps: int
    statement_timeout_ms: int
    stabilize: str
    variant_order_mode: str
    session_gucs: tuple[tuple[str, Any], ...]
    datasets: tuple[DatasetSpec, ...]


@dataclass(frozen=True)
class ResolvedDatasetRun:
    dataset: str
    db: str
    min_join: Optional[int]
    max_join: Optional[int]
    max_queries: Optional[int]
    variants: tuple[str, ...]


@dataclass(frozen=True)
class ConnOpts:
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None

    def to_args(self) -> list[str]:
        args: list[str] = []
        if self.host:
            args.extend(["-h", self.host])
        if self.port is not None:
            args.extend(["-p", str(self.port)])
        if self.user:
            args.extend(["-U", self.user])
        return args


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(2)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_cmd(
    cmd: list[str],
    *,
    input_text: Optional[str] = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    p = subprocess.run(cmd, input=input_text, text=True, capture_output=True)
    if check and p.returncode != 0:
        out = (p.stdout or "") + (p.stderr or "")
        die(f"command failed ({p.returncode}): {' '.join(cmd)}\n{out.strip()}")
    return p


def psql_cmd(db: str, conn: Optional[ConnOpts] = None) -> list[str]:
    c = conn or ConnOpts()
    return ["psql", "-X", "-q", "-P", "pager=off", "-v", "ON_ERROR_STOP=1", *c.to_args(), "-d", db]


def psql_sql(
    db: str,
    sql: str,
    *,
    conn: Optional[ConnOpts] = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return run_cmd(psql_cmd(db, conn), input_text=sql, check=check)


def psql_sql_raw(
    db: str,
    sql: str,
    *,
    conn: Optional[ConnOpts] = None,
    extra_args: Optional[list[str]] = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    cmd = psql_cmd(db, conn)
    if extra_args:
        cmd.extend(extra_args)
    return run_cmd(cmd, input_text=sql, check=check)


def psql_file(
    db: str,
    path: Path,
    *,
    conn: Optional[ConnOpts] = None,
    vars: Optional[dict[str, str]] = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    cmd = psql_cmd(db, conn) + ["-f", str(path)]
    if vars:
        for k, v in vars.items():
            cmd.extend(["-v", f"{k}={v}"])
    return run_cmd(cmd, check=check)


def sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "on" if value else "off"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def sql_identifier(name: str) -> str:
    text = str(name).replace('"', '""')
    return f'"{text}"'


def safe_artifact_name(text: str) -> str:
    return SAFE_NAME_RE.sub("_", text)


def parse_csv_list(raw: Optional[str]) -> list[str]:
    if raw is None:
        return []
    parts = [item.strip() for item in raw.split(",")]
    return [item for item in parts if item]
