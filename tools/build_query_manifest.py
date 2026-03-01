#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class QueryEntry:
    dataset: str
    query_id: str
    query_path: str
    query_label: str
    join_size: int
    sql_sha1: str


FROM_RE = re.compile(r"\bFROM\b(.*?)(\bWHERE\b|;|\Z)", flags=re.IGNORECASE | re.DOTALL)
SELECT5_HEADER_RE = re.compile(r"^--\s*query\s+(\d+)\s+\((.*?)\)\s*$", flags=re.IGNORECASE)


def strip_line_comments(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        if line.lstrip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def canonical_sql(sql: str) -> str:
    # Canonicalization for stable hashing across formatting-only diffs:
    # - strip line comments
    # - lowercase
    # - remove all whitespace
    s = strip_line_comments(sql).lower()
    return "".join(s.split())


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()


def join_size_from_sql(sql: str) -> Optional[int]:
    s = strip_line_comments(sql)
    m = FROM_RE.search(s)
    if not m:
        return None
    from_part = m.group(1)
    rels = [x.strip() for x in from_part.split(",") if x.strip()]
    return len(rels) if rels else None


def iter_sql_files(paths: Iterable[Path]) -> Iterable[Path]:
    for p in paths:
        if p.is_file():
            yield p


def build_file_dataset(dataset: str, files: list[Path]) -> list[QueryEntry]:
    out: list[QueryEntry] = []
    for p in sorted(files, key=lambda x: x.as_posix()):
        sql = p.read_text(errors="ignore")
        join_size = join_size_from_sql(sql)
        if join_size is None:
            raise SystemExit(f"[{dataset}] failed to parse join_size: {p}")
        rel = p.relative_to(REPO_ROOT).as_posix()
        out.append(
            QueryEntry(
                dataset=dataset,
                query_id=p.stem,
                query_path=rel,
                query_label="",
                join_size=join_size,
                sql_sha1=sha1_hex(canonical_sql(sql)),
            )
        )
    return out


def parse_select5(sql_path: Path) -> list[QueryEntry]:
    # Parse sqlite/queries/select5.sql which has the form:
    #   -- query 0001 (join-4-1)
    #   SELECT ...;
    #   <blank line>
    entries: list[QueryEntry] = []

    cur_id: Optional[str] = None
    cur_label: str = ""
    cur_lines: list[str] = []

    def flush():
        nonlocal cur_id, cur_label, cur_lines
        if cur_id is None:
            return
        sql = "\n".join(cur_lines).strip()
        if not sql:
            raise SystemExit(f"[sqlite_select5] empty SQL for query {cur_id}")
        join_size = join_size_from_sql(sql)
        if join_size is None:
            raise SystemExit(f"[sqlite_select5] failed to parse join_size for query {cur_id}")
        entries.append(
            QueryEntry(
                dataset="sqlite_select5",
                query_id=cur_id,
                query_path=sql_path.relative_to(REPO_ROOT).as_posix(),
                query_label=cur_label,
                join_size=join_size,
                sql_sha1=sha1_hex(canonical_sql(sql)),
            )
        )
        cur_id = None
        cur_label = ""
        cur_lines = []

    for raw in sql_path.read_text(errors="ignore").splitlines():
        m = SELECT5_HEADER_RE.match(raw.strip())
        if m:
            flush()
            cur_id = m.group(1).zfill(4)
            cur_label = m.group(2).strip()
            continue
        if cur_id is None:
            continue
        # Keep original SQL formatting (hash canonicalization normalizes it anyway).
        if raw.lstrip().startswith("--"):
            continue
        cur_lines.append(raw)
        if "\n".join(cur_lines).strip().endswith(";"):
            flush()

    flush()

    # Sort by query_id numeric.
    entries.sort(key=lambda e: int(e.query_id))
    return entries


def print_summary(entries: list[QueryEntry]) -> None:
    by_ds: dict[str, list[QueryEntry]] = {}
    for e in entries:
        by_ds.setdefault(e.dataset, []).append(e)

    print("== query manifest summary ==")
    for ds in sorted(by_ds):
        js = [e.join_size for e in by_ds[ds]]
        js.sort()
        n = len(js)
        hist = Counter(js)
        ge12 = sum(v for k, v in hist.items() if k >= 12)
        ge20 = sum(v for k, v in hist.items() if k >= 20)
        print(
            f"- {ds}: n={n} join_size min/p50/p90/p95/max = "
            f"{js[0]}/{js[n//2]}/{js[int(0.90*(n-1))]}/{js[int(0.95*(n-1))]}/{js[-1]} "
            f"| >=12: {ge12} ({ge12/n:.1%}) | >=20: {ge20} ({ge20/n:.1%})"
        )


def verify(entries: list[QueryEntry]) -> None:
    by_ds: dict[str, list[QueryEntry]] = {}
    for e in entries:
        by_ds.setdefault(e.dataset, []).append(e)

    def assert_ds_count(ds: str, n: int):
        got = len(by_ds.get(ds, []))
        if got != n:
            raise SystemExit(f"[verify] dataset {ds} expected {n} queries, got {got}")

    def assert_ds_range(ds: str, min_js: int, max_js: int):
        js = [e.join_size for e in by_ds.get(ds, [])]
        if not js:
            raise SystemExit(f"[verify] dataset {ds} missing")
        if min(js) != min_js or max(js) != max_js:
            raise SystemExit(
                f"[verify] dataset {ds} expected join_size range {min_js}..{max_js}, got {min(js)}..{max(js)}"
            )

    assert_ds_count("sqlite_select5", 732)
    assert_ds_range("sqlite_select5", 4, 64)

    assert_ds_count("gpuqo_snowflake_small", 390)
    assert_ds_range("gpuqo_snowflake_small", 2, 40)

    assert_ds_count("job", 113)
    assert_ds_count("job_complex", 30)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build meta/query_manifest.csv for join-order workloads.")
    ap.add_argument("--out", default=str(REPO_ROOT / "meta" / "query_manifest.csv"))
    ap.add_argument("--summary", action="store_true", help="Print dataset-level join_size summary.")
    ap.add_argument("--verify", action="store_true", help="Run strict verification checks (counts/ranges).")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    datasets: list[QueryEntry] = []

    datasets += build_file_dataset(
        "job",
        list(iter_sql_files((REPO_ROOT / "join-order-benchmark" / "queries").glob("*.sql"))),
    )
    datasets += build_file_dataset(
        "job_complex",
        list(iter_sql_files((REPO_ROOT / "JOB-Complex" / "queries").glob("*.sql"))),
    )
    datasets += parse_select5(REPO_ROOT / "sqlite" / "queries" / "select5.sql")

    for ds_id, rel_path in [
        ("gpuqo_chain_small", "postgres-gpuqo/scripts/databases/chain-small/queries"),
        ("gpuqo_clique_small", "postgres-gpuqo/scripts/databases/clique-small/queries"),
        ("gpuqo_star_small", "postgres-gpuqo/scripts/databases/star-small/queries"),
        ("gpuqo_snowflake_small", "postgres-gpuqo/scripts/databases/snowflake-small/queries"),
    ]:
        datasets += build_file_dataset(ds_id, list(iter_sql_files((REPO_ROOT / rel_path).glob("*.sql"))))

    datasets += build_file_dataset(
        "imdb_ceb_3k",
        list(iter_sql_files((REPO_ROOT / "imdb_pg_dataset" / "ceb-imdb-3k").rglob("*.sql"))),
    )

    with out_path.open("w", newline="") as f:
        # Use LF line endings for stable diffs across platforms/tools.
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["dataset", "query_id", "query_path", "query_label", "join_size", "sql_sha1"])
        for e in datasets:
            w.writerow([e.dataset, e.query_id, e.query_path, e.query_label, str(e.join_size), e.sql_sha1])

    if args.summary:
        print_summary(datasets)
    if args.verify:
        verify(datasets)


if __name__ == "__main__":
    main()
