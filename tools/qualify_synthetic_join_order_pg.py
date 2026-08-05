#!/usr/bin/env python3
"""Qualify one generated case against the benchmark-patched PostgreSQL.

Unless ``--skip-install`` is used, this command owns the case lifecycle:
install, plan without execution, validate the evidence, and remove the case.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_common import ConnOpts  # noqa: E402
from synthetic_join_order.compile import CompileError, compile_graph, load_graph, verify_case  # noqa: E402
from synthetic_join_order.plan import (  # noqa: E402
    PlanDriverError,
    bootstrap_metadata,
    install_case,
    plan_case,
    uninstall_case,
)
from synthetic_join_order.qualify import QualificationError, qualify_postgres  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True, type=Path, help="generated case directory")
    parser.add_argument("--db", required=True, help="clean PostgreSQL qualification database")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument(
        "--bootstrap-metadata",
        action="store_true",
        help="create the shared bench metadata schema in a clean database",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="qualify a case already installed in the target database",
    )
    parser.add_argument("--statement-timeout-ms", type=int, default=60_000)
    parser.add_argument("--audit", choices=("summary", "trace"), default="trace")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = ConnOpts(host=args.host, port=args.port, user=args.user)
    try:
        graph = load_graph(args.case / "graph.json")
        case = compile_graph(graph)
        verify_case(case, args.case)
        if args.bootstrap_metadata:
            bootstrap_metadata(args.db, conn)
        installed = not args.skip_install
        if installed:
            install_case(args.db, case, conn)
        primary_error: BaseException | None = None
        try:
            evidence = plan_case(
                args.db,
                case,
                conn,
                statement_timeout_ms=args.statement_timeout_ms,
                audit_mode=args.audit,
            )
            report = qualify_postgres(graph, case, evidence)
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            if installed:
                try:
                    uninstall_case(args.db, case, conn)
                except PlanDriverError as exc:
                    if primary_error is None:
                        raise
                    primary_error.add_note(f"case cleanup failed: {exc}")
    except (OSError, ValueError, CompileError, PlanDriverError, QualificationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        f"postgres-qualified {report.instance_id}: "
        f"base={report.base_relation_count} stats={report.stats_record_count} "
        f"joins={report.join_record_count} hashes={report.hash_record_count} "
        f"root_total_cost={report.root_total_cost}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
