#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "meta" / "query_manifest.csv"


WRAP_COUNT_DATASETS = {
    "sqlite_select5",
    "gpuqo_chain_small",
    "gpuqo_clique_small",
    "gpuqo_star_small",
    "gpuqo_snowflake_small",
}


@dataclass(frozen=True)
class QueryMeta:
    dataset: str
    query_id: str
    query_path: str
    query_label: str
    join_size: int


@dataclass(frozen=True)
class Algo:
    name: str
    gucs: list[tuple[str, str]]  # (key, value) emitted verbatim into SET


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


SELECT5_HEADER_RE = re.compile(r"^--\s*query\s+(\d+)\s+\((.*?)\)\s*$", flags=re.IGNORECASE)
PLANNING_RE = re.compile(r"Planning Time:\s*([0-9.]+)\s*ms", flags=re.IGNORECASE)
TIME_RE = re.compile(r"Time:\s*([0-9.]+)\s*ms", flags=re.IGNORECASE)
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(2)


def run_cmd(cmd: list[str], *, input_text: Optional[str] = None, check: bool = False) -> subprocess.CompletedProcess[str]:
    p = subprocess.run(cmd, input=input_text, text=True, capture_output=True)
    if check and p.returncode != 0:
        out = (p.stdout or "") + (p.stderr or "")
        die(f"command failed ({p.returncode}): {' '.join(cmd)}\n{out.strip()}")
    return p


def psql_cmd(db: str, conn: Optional[ConnOpts] = None) -> list[str]:
    # -X: no ~/.psqlrc, -q: quiet, pager off for stable parsing/logging.
    c = conn or ConnOpts()
    return ["psql", "-X", "-q", "-P", "pager=off", "-v", "ON_ERROR_STOP=1", *c.to_args(), "-d", db]


def psql_sql(db: str, sql: str, *, conn: Optional[ConnOpts] = None, check: bool = False) -> subprocess.CompletedProcess[str]:
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


def drop_and_create_db(db: str, conn: Optional[ConnOpts] = None) -> None:
    c = conn or ConnOpts()
    conn_args = c.to_args()
    run_cmd(["dropdb", "--if-exists", *conn_args, db], check=False)
    run_cmd(["createdb", *conn_args, db], check=True)


def parse_manifest(dataset: str) -> list[QueryMeta]:
    if not MANIFEST_PATH.is_file():
        die(f"missing manifest: {MANIFEST_PATH} (run tools/build_query_manifest.py)")

    out: list[QueryMeta] = []
    with MANIFEST_PATH.open(newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get("dataset") != dataset:
                continue
            try:
                join_size = int(row["join_size"])
            except Exception as e:
                raise SystemExit(f"bad join_size in manifest row: {row!r} ({e})")
            out.append(
                QueryMeta(
                    dataset=row["dataset"],
                    query_id=row["query_id"],
                    query_path=row["query_path"],
                    query_label=row.get("query_label", "") or "",
                    join_size=join_size,
                )
            )
    if not out:
        die(f"unknown dataset '{dataset}' or no rows in manifest")

    out.sort(key=lambda q: (q.query_id, q.query_path))
    return out


def parse_select5_queries(sql_path: Path) -> dict[str, str]:
    # File format:
    #   -- query 0001 (join-4-1)
    #   SELECT ...;
    queries: dict[str, str] = {}
    cur_id: Optional[str] = None
    cur_lines: list[str] = []

    def flush() -> None:
        nonlocal cur_id, cur_lines
        if cur_id is None:
            return
        sql = "\n".join(cur_lines).strip()
        if not sql:
            die(f"empty SQL in {sql_path} for query {cur_id}")
        queries[cur_id] = sql
        cur_id = None
        cur_lines = []

    for raw in sql_path.read_text(errors="ignore").splitlines():
        m = SELECT5_HEADER_RE.match(raw.strip())
        if m:
            flush()
            cur_id = m.group(1).zfill(4)
            continue
        if cur_id is None:
            continue
        if raw.lstrip().startswith("--"):
            continue
        cur_lines.append(raw)
        if "\n".join(cur_lines).strip().endswith(";"):
            flush()

    flush()
    return queries


def strip_trailing_semicolon_and_comment(sql: str) -> str:
    s = sql.strip()
    # Remove a trailing ';' and optional trailing '-- ...' comment.
    s = re.sub(r";\s*(--.*)?\s*\Z", "", s, flags=re.DOTALL)
    return s.strip()


def ensure_semicolon(sql: str) -> str:
    s = sql.strip()
    if not s.endswith(";"):
        return s + ";"
    return s


def parse_algo(spec: str) -> Algo:
    if ":" not in spec:
        die(f"bad --algo '{spec}' (expected name:key=value,...)")
    name, rest = spec.split(":", 1)
    name = name.strip()
    if not name:
        die(f"bad --algo '{spec}' (empty name)")
    gucs: list[tuple[str, str]] = []
    rest = rest.strip()
    if rest:
        for item in rest.split(","):
            item = item.strip()
            if not item:
                continue
            if "=" not in item:
                die(f"bad --algo '{spec}' (expected k=v, got '{item}')")
            k, v = item.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                die(f"bad --algo '{spec}' (empty GUC key)")
            if not v:
                die(f"bad --algo '{spec}' (empty GUC value for {k})")
            gucs.append((k, v))
    return Algo(name=name, gucs=gucs)


def dataset_prepare_scripts(dataset: str) -> tuple[Path, Path, Optional[Path], bool]:
    # Returns: (schema_sql, load_sql, index_sql, needs_csv_dir)
    if dataset == "sqlite_select5":
        return (REPO_ROOT / "sqlite" / "schema.sql", REPO_ROOT / "sqlite" / "load.sql", None, False)

    if dataset == "job":
        return (
            REPO_ROOT / "join-order-benchmark" / "schema.sql",
            REPO_ROOT / "join-order-benchmark" / "load.sql",
            REPO_ROOT / "join-order-benchmark" / "fkindexes.sql",
            True,
        )

    if dataset == "job_complex":
        return (
            REPO_ROOT / "JOB-Complex" / "schema.sql",
            REPO_ROOT / "JOB-Complex" / "load.sql",
            REPO_ROOT / "join-order-benchmark" / "fkindexes.sql",
            True,
        )

    if dataset == "imdb_ceb_3k":
        return (
            REPO_ROOT / "imdb_pg_dataset" / "schema.sql",
            REPO_ROOT / "imdb_pg_dataset" / "load.sql",
            REPO_ROOT / "join-order-benchmark" / "fkindexes.sql",
            True,
        )

    if dataset.startswith("gpuqo_"):
        name = dataset.removeprefix("gpuqo_").replace("_", "-")
        base = REPO_ROOT / "postgres-gpuqo" / "scripts" / "databases" / name
        return (base / "schema.sql", base / "load.sql", None, False)

    die(f"dataset '{dataset}' is not supported by prepare")


def prepare(dataset: str, db: str, csv_dir: Optional[str], conn: Optional[ConnOpts] = None) -> None:
    schema_sql, load_sql, index_sql, needs_csv_dir = dataset_prepare_scripts(dataset)
    if needs_csv_dir and not csv_dir:
        die(f"dataset '{dataset}' requires --csv-dir /absolute/path/to/imdb_csv")

    drop_and_create_db(db, conn)

    print(f"[prepare] dataset={dataset} db={db}")
    psql_file(db, schema_sql, conn=conn, check=True)
    psql_file(db, load_sql, conn=conn, vars=({"csv_dir": csv_dir} if csv_dir else None), check=True)

    if index_sql is not None:
        # Recommended for IMDB schema workloads; applied after load for faster ingest.
        psql_file(db, index_sql, conn=conn, check=True)


def get_postgres_version(db: str, conn: Optional[ConnOpts] = None) -> str:
    p = run_cmd(psql_cmd(db, conn) + ["-At"], input_text="SELECT version();\n", check=True)
    return (p.stdout or "").strip()


def first_error_line(output: str) -> str:
    for line in output.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("ERROR:") or s.startswith("FATAL:") or s.startswith("psql:"):
            return s
    # Fallback: first non-empty line.
    for line in output.splitlines():
        s = line.strip()
        if s:
            return s
    return ""


def build_statement(dataset: str, sql: str) -> str:
    if dataset in WRAP_COUNT_DATASETS:
        inner = strip_trailing_semicolon_and_comment(sql)
        return f"SELECT count(*) FROM ({inner}) q;"
    return ensure_semicolon(sql)


def build_session_prelude(algo: Algo, statement_timeout_ms: Optional[int]) -> list[str]:
    lines = ["RESET ALL;"]
    if statement_timeout_ms is not None and statement_timeout_ms > 0:
        lines.append(f"SET statement_timeout = {statement_timeout_ms};")
    lines.extend(f"SET {k} = {v};" for k, v in algo.gucs)
    return lines


def safe_artifact_name(text: str) -> str:
    return SAFE_NAME_RE.sub("_", text)


def run_one(
    db: str,
    algo: Algo,
    stmt: str,
    conn: Optional[ConnOpts] = None,
    statement_timeout_ms: Optional[int] = None,
) -> tuple[float, float]:
    script = "\n".join(
        [
            *build_session_prelude(algo, statement_timeout_ms),
            f"EXPLAIN (SUMMARY) {stmt}",
            r"\timing on",
            r"\o /dev/null",
            stmt,
            r"\o",
            "",
        ]
    )
    p = psql_sql(db, script, conn=conn, check=False)
    out = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0:
        raise RuntimeError(first_error_line(out) or "query failed")

    m = PLANNING_RE.search(out)
    if not m:
        raise RuntimeError("missing Planning Time")
    planning_ms = float(m.group(1))

    times = TIME_RE.findall(out)
    if not times:
        raise RuntimeError("missing Time")
    total_ms = float(times[-1])
    return planning_ms, total_ms


def capture_explain_analyze(
    db: str,
    algo: Algo,
    query_id: str,
    stmt: str,
    artifact_dir: Path,
    *,
    conn: Optional[ConnOpts] = None,
    statement_timeout_ms: Optional[int] = None,
) -> tuple[str, str]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    base = f"{safe_artifact_name(query_id)}__{safe_artifact_name(algo.name)}"
    ok_path = artifact_dir / f"{base}.json"
    err_path = artifact_dir / f"{base}.error.txt"
    script = "\n".join(
        [
            *build_session_prelude(algo, statement_timeout_ms),
            f"EXPLAIN (ANALYZE, FORMAT JSON, VERBOSE, BUFFERS, SETTINGS, SUMMARY) {stmt}",
            "",
        ]
    )
    p = psql_sql_raw(db, script, conn=conn, extra_args=["-A", "-t"], check=False)
    stdout = p.stdout or ""
    stderr = p.stderr or ""
    out = stdout + stderr
    if p.returncode != 0:
        err_path.write_text(out)
        if ok_path.exists():
            ok_path.unlink()
        return ("error", str(err_path.relative_to(REPO_ROOT)))
    payload = stdout.strip()
    if not payload:
        err_path.write_text("missing EXPLAIN ANALYZE output\n" + out)
        if ok_path.exists():
            ok_path.unlink()
        return ("error", str(err_path.relative_to(REPO_ROOT)))
    ok_path.write_text(payload + "\n")
    if err_path.exists():
        err_path.unlink()
    return ("ok", str(ok_path.relative_to(REPO_ROOT)))


def run_bench(
    dataset: str,
    db: str,
    algos: list[Algo],
    min_join: Optional[int],
    max_join: Optional[int],
    query_offset: int,
    max_queries: Optional[int],
    *,
    reps: int = 3,
    stabilize: str = "vacuum_freeze_analyze",
    conn: Optional[ConnOpts] = None,
    statement_timeout_ms: Optional[int] = None,
    capture_explain_analyze_json: bool = False,
) -> None:
    if reps <= 0:
        die(f"--reps must be >= 1 (got {reps})")
    if max_queries is not None and max_queries <= 0:
        die(f"--max-queries must be >= 1 (got {max_queries})")
    if statement_timeout_ms is not None and statement_timeout_ms < 0:
        die(f"--statement-timeout-ms must be >= 0 (got {statement_timeout_ms})")
    if min_join is not None and min_join <= 0:
        die(f"--min-join must be >= 1 (got {min_join})")
    if max_join is not None and max_join <= 0:
        die(f"--max-join must be >= 1 (got {max_join})")
    if min_join is not None and max_join is not None and min_join > max_join:
        die(f"--min-join must be <= --max-join (got {min_join} > {max_join})")
    if query_offset < 0:
        die(f"--query-offset must be >= 0 (got {query_offset})")

    queries = parse_manifest(dataset)

    mj = min_join
    xj = max_join
    if mj is not None:
        queries = [q for q in queries if q.join_size >= mj]
    if xj is not None:
        queries = [q for q in queries if q.join_size <= xj]
    if query_offset:
        queries = queries[query_offset:]
    if max_queries is not None:
        queries = queries[:max_queries]
    if not queries:
        mj_desc = str(mj) if mj is not None else "all"
        xj_desc = str(xj) if xj is not None else "all"
        die(
            f"no queries selected (dataset={dataset}, min_join={mj_desc}, "
            f"max_join={xj_desc}, max_queries={max_queries})"
        )

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = REPO_ROOT / "results" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    version = get_postgres_version(db, conn)
    run_cfg = {
        "run_id": run_id,
        "dataset": dataset,
        "db": db,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "postgres_version": version,
        "min_join": mj,
        "max_join": xj,
        "query_offset": query_offset,
        "max_queries": max_queries,
        "algos": [{"name": a.name, "gucs": [{k: v} for k, v in a.gucs]} for a in algos],
        "repetitions": reps,
        "stabilize": stabilize,
        "statement_timeout_ms": statement_timeout_ms,
        "capture_explain_analyze_json": capture_explain_analyze_json,
        "explain_analyze_options": "ANALYZE, FORMAT JSON, VERBOSE, BUFFERS, SETTINGS, SUMMARY"
        if capture_explain_analyze_json
        else "",
    }
    (out_dir / "run.json").write_text(json.dumps(run_cfg, indent=2, sort_keys=True) + "\n")

    raw_path = out_dir / "raw.csv"
    summary_path = out_dir / "summary.csv"
    explain_dir = out_dir / "explain_analyze"

    mj_desc = str(mj) if mj is not None else "all"
    xj_desc = str(xj) if xj is not None else "all"
    print(
        f"[run] dataset={dataset} db={db} queries={len(queries)} "
        f"algos={len(algos)} reps={reps} min_join={mj_desc} max_join={xj_desc} "
        f"query_offset={query_offset} "
        f"stabilize={stabilize}"
    )
    print(f"[run] writing results to: {out_dir}")

    # Stability prelude (once per run).
    if stabilize == "vacuum_freeze_analyze":
        psql_sql(db, "VACUUM FREEZE ANALYZE;", conn=conn, check=True)
        psql_sql(db, "CHECKPOINT;", conn=conn, check=False)
    elif stabilize == "analyze":
        psql_sql(db, "ANALYZE;", conn=conn, check=True)
        psql_sql(db, "CHECKPOINT;", conn=conn, check=False)
    elif stabilize == "none":
        pass
    else:
        die(f"unknown --stabilize mode: {stabilize}")

    if dataset == "sqlite_select5":
        select5_path = REPO_ROOT / "sqlite" / "queries" / "select5.sql"
        select5_map = parse_select5_queries(select5_path)
    else:
        select5_map = {}

    raw_rows: list[dict[str, str]] = []
    # (query_id, algo) -> list of (planning_ms, total_ms, exec_ms, status)
    summary_acc: dict[tuple[str, str], list[tuple[float, float, float, str]]] = {}
    explain_acc: dict[tuple[str, str], tuple[str, str]] = {}

    for q in queries:
        if dataset == "sqlite_select5":
            sql = select5_map.get(q.query_id)
            if sql is None:
                die(f"missing sqlite_select5 query_id={q.query_id} in sqlite/queries/select5.sql")
        else:
            p = REPO_ROOT / q.query_path
            if not p.is_file():
                die(f"missing query file: {p}")
            sql = p.read_text(errors="ignore")

        stmt = build_statement(dataset, sql)

        for algo in algos:
            key = (q.query_id, algo.name)
            for rep in range(1, reps + 1):
                status = "ok"
                err = ""
                planning_ms = -1.0
                total_ms = -1.0
                exec_ms = -1.0

                try:
                    planning_ms, total_ms = run_one(
                        db,
                        algo,
                        stmt,
                        conn,
                        statement_timeout_ms=statement_timeout_ms,
                    )
                    exec_ms = max(total_ms - planning_ms, 0.0)
                except Exception as e:
                    status = "error"
                    err = str(e)

                raw_rows.append(
                    {
                        "run_id": run_id,
                        "dataset": dataset,
                        "db": db,
                        "algo": algo.name,
                        "query_id": q.query_id,
                        "query_label": q.query_label,
                        "query_path": q.query_path,
                        "join_size": str(q.join_size),
                        "rep": str(rep),
                        "planning_ms": f"{planning_ms:.3f}" if planning_ms >= 0 else "",
                        "total_ms": f"{total_ms:.3f}" if total_ms >= 0 else "",
                        "execution_ms": f"{exec_ms:.3f}" if exec_ms >= 0 else "",
                        "status": status,
                        "error": err,
                    }
                )
                summary_acc.setdefault(key, []).append((planning_ms, total_ms, exec_ms, status))

            if capture_explain_analyze_json:
                explain_acc[key] = capture_explain_analyze(
                    db,
                    algo,
                    q.query_id,
                    stmt,
                    explain_dir,
                    conn=conn,
                    statement_timeout_ms=statement_timeout_ms,
                )

    with raw_path.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_id",
                "dataset",
                "db",
                "algo",
                "query_id",
                "query_label",
                "query_path",
                "join_size",
                "rep",
                "planning_ms",
                "total_ms",
                "execution_ms",
                "status",
                "error",
            ],
            lineterminator="\n",
        )
        w.writeheader()
        w.writerows(raw_rows)

    with summary_path.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_id",
                "dataset",
                "db",
                "algo",
                "query_id",
                "query_label",
                "query_path",
                "join_size",
                "planning_ms_min",
                "total_ms_min",
                "execution_ms_min",
                "ok_reps",
                "err_reps",
                "explain_analyze_status",
                "explain_analyze_artifact",
            ],
            lineterminator="\n",
        )
        w.writeheader()
        for q in queries:
            for algo in algos:
                vals = summary_acc.get((q.query_id, algo.name), [])
                explain_status, explain_artifact = explain_acc.get((q.query_id, algo.name), ("", ""))
                ok = [(p, t, e) for (p, t, e, s) in vals if s == "ok"]
                ok_reps = len(ok)
                err_reps = len(vals) - ok_reps
                if ok:
                    planning_min = min(p for (p, _, _) in ok)
                    total_min = min(t for (_, t, _) in ok)
                    exec_min = min(e for (_, _, e) in ok)
                    row = {
                        "run_id": run_id,
                        "dataset": dataset,
                        "db": db,
                        "algo": algo.name,
                        "query_id": q.query_id,
                        "query_label": q.query_label,
                        "query_path": q.query_path,
                        "join_size": str(q.join_size),
                        "planning_ms_min": f"{planning_min:.3f}",
                        "total_ms_min": f"{total_min:.3f}",
                        "execution_ms_min": f"{exec_min:.3f}",
                        "ok_reps": str(ok_reps),
                        "err_reps": str(err_reps),
                        "explain_analyze_status": explain_status,
                        "explain_analyze_artifact": explain_artifact,
                    }
                else:
                    row = {
                        "run_id": run_id,
                        "dataset": dataset,
                        "db": db,
                        "algo": algo.name,
                        "query_id": q.query_id,
                        "query_label": q.query_label,
                        "query_path": q.query_path,
                        "join_size": str(q.join_size),
                        "planning_ms_min": "",
                        "total_ms_min": "",
                        "execution_ms_min": "",
                        "ok_reps": "0",
                        "err_reps": str(err_reps),
                        "explain_analyze_status": explain_status,
                        "explain_analyze_artifact": explain_artifact,
                    }
                w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser(description="Minimal GUC-driven join-order benchmark harness (PostgreSQL).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_conn_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=None, help="PostgreSQL host (e.g. localhost)")
        p.add_argument("--port", type=int, default=None, help="PostgreSQL port (e.g. 54321)")
        p.add_argument("--user", default=None, help="PostgreSQL user (optional)")

    ap_prep = sub.add_parser("prepare", help="Drop/create DB and load a dataset.")
    ap_prep.add_argument("dataset", help="dataset id (must exist in meta/query_manifest.csv)")
    ap_prep.add_argument("db", help="database name")
    ap_prep.add_argument("--csv-dir", default=None, help="IMDB CSV directory for JOB/CEB datasets")
    add_conn_args(ap_prep)

    ap_run = sub.add_parser("run", help="Run queries under one or more GUC sets and write CSV results.")
    ap_run.add_argument("dataset", help="dataset id (must exist in meta/query_manifest.csv)")
    ap_run.add_argument("db", help="database name")
    ap_run.add_argument("--algo", action="append", required=True, help="name:key=value,key=value (repeatable)")
    ap_run.add_argument("--min-join", type=int, default=None, help="min join_size filter (default: no filter)")
    ap_run.add_argument("--max-join", type=int, default=None, help="max join_size filter (default: no filter)")
    ap_run.add_argument("--query-offset", type=int, default=0, help="skip the first N selected queries")
    ap_run.add_argument("--max-queries", type=int, default=None, help="limit number of selected queries")
    ap_run.add_argument("--reps", type=int, default=3, help="repetitions per (query, algo) (default: 3)")
    ap_run.add_argument(
        "--capture-explain-analyze-json",
        action="store_true",
        help="run one extra EXPLAIN ANALYZE (FORMAT JSON, VERBOSE, BUFFERS, SETTINGS, SUMMARY) per (query, algo)",
    )
    ap_run.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=0,
        help="PostgreSQL statement_timeout in milliseconds; 0 disables timeout (default: 0)",
    )
    ap_run.add_argument(
        "--stabilize",
        choices=["vacuum_freeze_analyze", "analyze", "none"],
        default="vacuum_freeze_analyze",
        help="stability prelude before running queries (default: vacuum_freeze_analyze)",
    )
    add_conn_args(ap_run)

    ap_smoke = sub.add_parser(
        "smoke",
        help="Quick smoke run: light defaults for connectivity and query execution checks.",
    )
    ap_smoke.add_argument("dataset", help="dataset id (must exist in meta/query_manifest.csv)")
    ap_smoke.add_argument("db", help="database name")
    ap_smoke.add_argument("--algo", action="append", required=True, help="name:key=value,key=value (repeatable)")
    ap_smoke.add_argument("--min-join", type=int, default=None, help="min join_size filter (default: no filter)")
    ap_smoke.add_argument("--max-join", type=int, default=None, help="max join_size filter (default: no filter)")
    ap_smoke.add_argument("--query-offset", type=int, default=0, help="skip the first N selected queries")
    ap_smoke.add_argument(
        "--queries",
        type=int,
        default=1,
        help="number of selected queries to run for smoke check (default: 1)",
    )
    ap_smoke.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=0,
        help="PostgreSQL statement_timeout in milliseconds; 0 disables timeout (default: 0)",
    )
    ap_smoke.add_argument(
        "--capture-explain-analyze-json",
        action="store_true",
        help="run one extra EXPLAIN ANALYZE (FORMAT JSON, VERBOSE, BUFFERS, SETTINGS, SUMMARY) per (query, algo)",
    )
    add_conn_args(ap_smoke)

    args = ap.parse_args()
    conn = ConnOpts(host=args.host, port=args.port, user=args.user)

    if args.cmd == "prepare":
        prepare(args.dataset, args.db, args.csv_dir, conn)
        return

    if args.cmd == "run":
        algos = [parse_algo(s) for s in args.algo]
        run_bench(
            args.dataset,
            args.db,
            algos,
            args.min_join,
            args.max_join,
            args.query_offset,
            args.max_queries,
            reps=args.reps,
            stabilize=args.stabilize,
            conn=conn,
            statement_timeout_ms=args.statement_timeout_ms,
            capture_explain_analyze_json=args.capture_explain_analyze_json,
        )
        return

    if args.cmd == "smoke":
        if args.queries <= 0:
            die(f"--queries must be >= 1 (got {args.queries})")
        algos = [parse_algo(s) for s in args.algo]
        run_bench(
            args.dataset,
            args.db,
            algos,
            args.min_join,
            args.max_join,
            args.query_offset,
            args.queries,
            reps=1,
            stabilize="none",
            conn=conn,
            statement_timeout_ms=args.statement_timeout_ms,
            capture_explain_analyze_json=args.capture_explain_analyze_json,
        )
        return

    die(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
