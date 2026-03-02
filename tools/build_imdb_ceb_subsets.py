#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = REPO_ROOT / "meta" / "query_manifest.csv"
OUT_DIR = REPO_ROOT / "meta" / "subsets" / "imdb_ceb_3k"


@dataclass(frozen=True)
class Row:
    query_id: str
    join_size: int
    sql_sha1: str


def load_rows() -> list[Row]:
    if not MANIFEST.is_file():
        raise SystemExit(f"missing manifest: {MANIFEST}")
    out: list[Row] = []
    with MANIFEST.open(newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get("dataset") != "imdb_ceb_3k":
                continue
            out.append(
                Row(
                    query_id=row["query_id"],
                    join_size=int(row["join_size"]),
                    sql_sha1=row.get("sql_sha1", "") or "",
                )
            )
    out.sort(key=lambda x: x.query_id)
    if not out:
        raise SystemExit("no imdb_ceb_3k rows in manifest")
    return out


def dedupe_rows(rows: list[Row]) -> list[Row]:
    seen: set[str] = set()
    out: list[Row] = []
    for r in rows:
        key = r.sql_sha1 or r.query_id
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def pick_evenly(rows: list[Row], k: int) -> list[Row]:
    if k >= len(rows):
        return list(rows)
    if k <= 0:
        return []
    out: list[Row] = []
    step = len(rows) / k
    used: set[int] = set()
    for i in range(k):
        idx = int(math.floor(i * step))
        while idx in used and idx + 1 < len(rows):
            idx += 1
        used.add(idx)
        out.append(rows[idx])
    return out


def stratified_by_join_size(rows: list[Row], target: int) -> list[Row]:
    if target >= len(rows):
        return list(rows)
    if target <= 0:
        return []

    groups: dict[int, list[Row]] = defaultdict(list)
    for r in rows:
        groups[r.join_size].append(r)
    sizes = sorted(groups)
    total = len(rows)

    exact = {s: target * len(groups[s]) / total for s in sizes}
    alloc = {s: int(math.floor(exact[s])) for s in sizes}

    # Give at least one sample per join size when possible.
    if target >= len(sizes):
        for s in sizes:
            if alloc[s] == 0 and len(groups[s]) > 0:
                alloc[s] = 1

    # Trim if minimum-allocation overflowed.
    while sum(alloc.values()) > target:
        for s in sorted(sizes, key=lambda x: (alloc[x], len(groups[x])), reverse=True):
            min_allowed = 1 if target >= len(sizes) else 0
            if alloc[s] > min_allowed:
                alloc[s] -= 1
                break

    # Largest remainder method for leftover slots.
    remain = target - sum(alloc.values())
    if remain > 0:
        order = sorted(
            sizes,
            key=lambda s: (exact[s] - math.floor(exact[s]), len(groups[s])),
            reverse=True,
        )
        idx = 0
        while remain > 0:
            s = order[idx % len(order)]
            if alloc[s] < len(groups[s]):
                alloc[s] += 1
                remain -= 1
            idx += 1

    picked: list[Row] = []
    for s in sizes:
        g = sorted(groups[s], key=lambda x: x.query_id)
        picked.extend(pick_evenly(g, alloc[s]))
    picked.sort(key=lambda x: x.query_id)
    return picked


def write_ids(path: Path, rows: list[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(r.query_id for r in rows)
    if text:
        text += "\n"
    path.write_text(text)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build imdb_ceb_3k subset files for fast/stratified benchmark runs.")
    ap.add_argument("--out-dir", default=str(OUT_DIR), help="output directory (default: meta/subsets/imdb_ceb_3k)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    rows = load_rows()
    uniq_rows = dedupe_rows(rows)

    write_ids(out_dir / "all.txt", rows)
    write_ids(out_dir / "all_dedup.txt", uniq_rows)

    by_js: dict[int, list[Row]] = defaultdict(list)
    for r in uniq_rows:
        by_js[r.join_size].append(r)
    for js in sorted(by_js):
        write_ids(out_dir / f"join_{js:02d}.txt", sorted(by_js[js], key=lambda x: x.query_id))

    write_ids(out_dir / "bucket_06_08.txt", [r for r in uniq_rows if 6 <= r.join_size <= 8])
    write_ids(out_dir / "bucket_09_11.txt", [r for r in uniq_rows if 9 <= r.join_size <= 11])
    write_ids(out_dir / "bucket_12_13.txt", [r for r in uniq_rows if 12 <= r.join_size <= 13])
    write_ids(out_dir / "bucket_14_16.txt", [r for r in uniq_rows if 14 <= r.join_size <= 16])
    write_ids(out_dir / "ge_12.txt", [r for r in uniq_rows if r.join_size >= 12])
    write_ids(out_dir / "ge_14.txt", [r for r in uniq_rows if r.join_size >= 14])

    for target in [300, 600, 1200]:
        picked = stratified_by_join_size(uniq_rows, target)
        write_ids(out_dir / f"stratified_{target}.txt", picked)

    summary = (
        f"imdb_ceb_3k total={len(rows)} dedup={len(uniq_rows)} "
        f"removed={len(rows)-len(uniq_rows)} out_dir={out_dir}\n"
    )
    print(summary, end="")


if __name__ == "__main__":
    main()
