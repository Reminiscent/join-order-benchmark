"""Shared dataclasses, paths, and subprocess helpers for the benchmark harness.

The other ``bench_*`` modules import these primitives instead of duplicating
connection handling, SQL quoting, and artifact-name logic.
"""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "tools" / "query_manifest.csv"
OUTPUTS_DIR = REPO_ROOT / "outputs"

SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


# Shared benchmark records passed between CLI, workload, run, and output modules.


@dataclass(frozen=True)
class QueryMeta:
    """Manifest metadata for one benchmark SQL query."""

    dataset: str
    query_id: str
    query_path: str
    join_size: int


@dataclass(frozen=True)
class Variant:
    """One algorithm/configuration variant with its session GUCs."""

    name: str
    label: str
    session_gucs: tuple[tuple[str, Any], ...]
    baseline: bool = False


@dataclass(frozen=True)
class Scenario:
    """A public benchmark scenario and its workload selection."""

    name: str
    description: str
    datasets: tuple[str, ...]


@dataclass(frozen=True)
class ResolvedDatasetRun:
    """Concrete dataset/database/variant work item produced from a scenario."""

    dataset: str
    db: str
    variants: tuple[str, ...]
    min_join: Optional[int] = None


@dataclass(frozen=True)
class ConnOpts:
    """Optional PostgreSQL connection flags shared by prepare/run commands."""

    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None

    def to_args(self) -> list[str]:
        """Render the configured connection options as ``psql`` CLI arguments."""
        args: list[str] = []
        if self.host:
            args.extend(["-h", self.host])
        if self.port is not None:
            args.extend(["-p", str(self.port)])
        if self.user:
            args.extend(["-U", self.user])
        return args


# CLI, time, and subprocess helpers.


def die(msg: str) -> None:
    """Print a CLI-style error and stop with the harness error exit code."""
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(2)


def utc_now() -> datetime:
    """Return the current UTC timestamp for run identifiers and metadata."""
    return datetime.now(timezone.utc)


def run_cmd(
    cmd: list[str],
    *,
    input_text: Optional[str] = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a command with captured text output.

    When ``check`` is true, non-zero exits are reported through the harness
    error path so callers get a consistent message and exit code.
    """
    result = subprocess.run(cmd, input=input_text, text=True, capture_output=True)
    if check and result.returncode != 0:
        out = (result.stdout or "") + (result.stderr or "")
        die(f"command failed ({result.returncode}): {' '.join(cmd)}\n{out.strip()}")
    return result


# PostgreSQL command helpers.


def psql_cmd(db: str, conn: Optional[ConnOpts] = None) -> list[str]:
    """Build the standard non-interactive ``psql`` command for one database."""
    c = conn or ConnOpts()
    return ["psql", "-X", "-q", "-P", "pager=off", "-v", "ON_ERROR_STOP=1", *c.to_args(), "-d", db]


def psql_sql(
    db: str,
    sql: str,
    *,
    conn: Optional[ConnOpts] = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Execute an inline SQL script through the standard ``psql`` path."""
    return psql_sql_raw(db, sql, conn=conn, check=check)


def psql_sql_raw(
    db: str,
    sql: str,
    *,
    conn: Optional[ConnOpts] = None,
    extra_args: Optional[list[str]] = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Execute inline SQL with optional extra ``psql`` arguments."""
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
    """Execute a SQL file through ``psql``, optionally passing ``-v`` variables."""
    cmd = psql_cmd(db, conn) + ["-f", str(path)]
    if vars:
        for k, v in vars.items():
            cmd.extend(["-v", f"{k}={v}"])
    return run_cmd(cmd, check=check)


# SQL and artifact-name formatting helpers.


def sql_literal(value: Any) -> str:
    """Format a Python value as a SQL literal for generated harness SQL.

    Booleans use PostgreSQL's unquoted ``on``/``off`` tokens because most uses
    are GUC assignments.
    """
    if isinstance(value, bool):
        return "on" if value else "off"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def sql_identifier(name: str) -> str:
    """Quote a SQL identifier for generated utility commands."""
    text = str(name).replace('"', '""')
    return f'"{text}"'


def safe_artifact_name(text: str) -> str:
    """Replace unsafe path characters for run directory names."""
    return SAFE_NAME_RE.sub("_", text)


def parse_csv_list(raw: Optional[str]) -> list[str]:
    """Parse a comma-separated CLI option into non-empty trimmed items."""
    if raw is None:
        return []
    parts = [item.strip() for item in raw.split(",")]
    return [item for item in parts if item]
