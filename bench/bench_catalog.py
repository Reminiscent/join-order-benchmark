from __future__ import annotations

import csv
import functools
import re
from pathlib import Path
from typing import Optional

from bench_common import MANIFEST_PATH, REPO_ROOT, QueryMeta, ResolvedDatasetRun, die


WRAP_COUNT_DATASETS = {
    "sqlite_select5",
    "gpuqo_chain_small",
    "gpuqo_clique_small",
    "gpuqo_star_small",
    "gpuqo_snowflake_small",
}

IMDB_DATASETS = {"job", "job_complex", "imdb_ceb_3k"}

DEFAULT_DB_BY_DATASET = {
    "job": "imdb_bench",
    "job_complex": "imdb_bench",
    "imdb_ceb_3k": "imdb_bench",
    "sqlite_select5": "sqlite_select5_bench",
    "gpuqo_chain_small": "gpuqo_chain_small_bench",
    "gpuqo_clique_small": "gpuqo_clique_small_bench",
    "gpuqo_star_small": "gpuqo_star_small_bench",
    "gpuqo_snowflake_small": "gpuqo_snowflake_small_bench",
}

PREPARE_MARKERS = {
    "job": ("title", "aka_name"),
    "job_complex": ("title", "aka_name"),
    "imdb_ceb_3k": ("title", "aka_name"),
    "sqlite_select5": ("t1", "t64"),
    "gpuqo_chain_small": ("t1", "t40"),
    "gpuqo_clique_small": ("t1", "t40"),
    "gpuqo_star_small": ("t0", "t39"),
    "gpuqo_snowflake_small": ("t_1", "t_1_16"),
}

SELECT5_HEADER_RE = re.compile(r"^--\s*query\s+(\d+)\s+\((.*?)\)\s*$", flags=re.IGNORECASE)


@functools.lru_cache(maxsize=1)
def load_manifest_by_dataset() -> dict[str, tuple[QueryMeta, ...]]:
    if not MANIFEST_PATH.is_file():
        die(f"missing manifest: {MANIFEST_PATH} (run tools/build_query_manifest.py --verify --summary)")

    out: dict[str, list[QueryMeta]] = {}
    with MANIFEST_PATH.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                join_size = int(row["join_size"])
            except Exception as e:  # pragma: no cover
                die(f"bad join_size in manifest row: {row!r} ({e})")
            q = QueryMeta(
                dataset=row["dataset"],
                query_id=row["query_id"],
                query_path=row["query_path"],
                query_label=row.get("query_label", "") or "",
                join_size=join_size,
            )
            out.setdefault(q.dataset, []).append(q)

    finalized: dict[str, tuple[QueryMeta, ...]] = {}
    for dataset, queries in out.items():
        queries.sort(key=lambda q: (q.query_id, q.query_path))
        finalized[dataset] = tuple(queries)
    return finalized


def available_datasets() -> tuple[str, ...]:
    return tuple(sorted(load_manifest_by_dataset().keys()))


def parse_manifest(dataset: str) -> list[QueryMeta]:
    manifest = load_manifest_by_dataset()
    if dataset not in manifest:
        die(f"unknown dataset '{dataset}'")
    return list(manifest[dataset])


@functools.lru_cache(maxsize=1)
def parse_select5_queries() -> dict[str, str]:
    sql_path = REPO_ROOT / "sqlite" / "queries" / "select5.sql"
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
    s = re.sub(r";\s*(--.*)?\s*\Z", "", s, flags=re.DOTALL)
    return s.strip()


def ensure_semicolon(sql: str) -> str:
    s = sql.strip()
    if not s.endswith(";"):
        return s + ";"
    return s


def build_statement(dataset: str, sql: str) -> str:
    if dataset in WRAP_COUNT_DATASETS:
        inner = strip_trailing_semicolon_and_comment(sql)
        return f"SELECT count(*) FROM ({inner}) q;"
    return ensure_semicolon(sql)


def dataset_db_name(dataset: str) -> str:
    if dataset not in DEFAULT_DB_BY_DATASET:
        die(f"no default benchmark database configured for dataset '{dataset}'")
    return DEFAULT_DB_BY_DATASET[dataset]


def dataset_prepare_scripts(dataset: str) -> tuple[Path, Path, Optional[Path], bool]:
    if dataset in IMDB_DATASETS:
        return (
            REPO_ROOT / "join-order-benchmark" / "schema.sql",
            REPO_ROOT / "join-order-benchmark" / "load.sql",
            REPO_ROOT / "join-order-benchmark" / "fkindexes.sql",
            True,
        )

    if dataset == "sqlite_select5":
        return (
            REPO_ROOT / "sqlite" / "schema.sql",
            REPO_ROOT / "sqlite" / "load.sql",
            None,
            False,
        )

    if dataset.startswith("gpuqo_"):
        name = dataset.removeprefix("gpuqo_").replace("_", "-")
        base = REPO_ROOT / "postgres-gpuqo" / "scripts" / "databases" / name
        return (base / "schema.sql", base / "load.sql", None, False)

    die(f"dataset '{dataset}' is not supported by prepare")


def select_queries(spec: ResolvedDatasetRun) -> list[QueryMeta]:
    queries = parse_manifest(spec.dataset)
    if spec.min_join is not None:
        queries = [q for q in queries if q.join_size >= spec.min_join]
    if spec.max_join is not None:
        queries = [q for q in queries if q.join_size <= spec.max_join]
    if spec.max_queries is not None:
        queries = queries[: spec.max_queries]
    if not queries:
        die(
            "no queries selected "
            f"(dataset={spec.dataset}, min_join={spec.min_join}, max_join={spec.max_join}, "
            f"max_queries={spec.max_queries})"
        )
    return queries


def load_sql_for_query(query: QueryMeta) -> str:
    if query.dataset == "sqlite_select5":
        sql = parse_select5_queries().get(query.query_id)
        if sql is None:
            die(f"missing sqlite_select5 query_id={query.query_id} in sqlite/queries/select5.sql")
        return sql

    path = REPO_ROOT / query.query_path
    if not path.is_file():
        die(f"missing query file: {path}")
    return path.read_text(errors="ignore")
