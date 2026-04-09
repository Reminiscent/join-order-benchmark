#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bench"))

from bench_public_report import rerender_public_reports_for_run_dir


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Render the default public benchmark reports for outputs/<run_id>/.")
    ap.add_argument("run_dir", help="Path to outputs/<run_id>/")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir).resolve()
    md_path, json_path = rerender_public_reports_for_run_dir(run_dir)
    print(md_path)
    print(json_path)


if __name__ == "__main__":
    main()
