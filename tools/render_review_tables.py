#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bench"))

from bench_review_tables import write_review_tables


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Render reviewer-facing per-query benchmark tables from outputs/<run_id>/summary.csv. "
            "Writes a styled XLSX workbook plus CSV companion files."
        )
    )
    ap.add_argument("run_dir", help="Path to outputs/<run_id>/")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    paths = write_review_tables(
        run_dir=Path(args.run_dir).resolve(),
        datasets=[],
        variants_csv=None,
    )
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
