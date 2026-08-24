#!/usr/bin/env python3
"""Generate, verify, or offline-qualify one synthetic join-order case."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from synthetic_join_order.compile import CompileError, compile_graph, verify_case, write_case
from synthetic_join_order.graph import GenerationSpec, SpecError, generate_graph
from synthetic_join_order.qualify import QualificationError, qualify_graph


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", required=True, type=Path, help="generation spec JSON")
    parser.add_argument("--output", required=True, type=Path, help="case artifact directory")
    parser.add_argument("--verify", action="store_true", help="compare with existing artifacts")
    parser.add_argument("--qualify", action="store_true", help="run offline graph/compiler qualification")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        raw_spec = json.loads(args.spec.read_text(encoding="utf-8"))
        spec = GenerationSpec.from_dict(raw_spec)
        graph = generate_graph(spec)
        case = compile_graph(graph)
        if args.verify:
            verify_case(case, args.output)
        else:
            write_case(case, args.output)
        if args.qualify:
            report = qualify_graph(graph, case)
            print(
                f"qualified {report.instance_id}: nodes={report.node_count} "
                f"edges={report.edge_count} connected_subsets={report.connected_subset_count} "
                f"recurrences={report.recurrence_check_count}"
            )
        else:
            print(case.instance_id)
    except (OSError, json.JSONDecodeError, SpecError, CompileError, QualificationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
