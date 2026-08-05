#!/usr/bin/env python3
"""Run a deterministic size/seed matrix on benchmark-patched PostgreSQL.

The command validates the complete request before database or output mutation,
then records benchmark/source provenance, qualified Total Costs, coverage, and
failure evidence.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

from bench_common import ConnOpts  # noqa: E402
from bench_config import load_variants, select_variant_names  # noqa: E402
from synthetic_join_order.matrix import (  # noqa: E402
    MAIN_CARDINALITY_SEED_START,
    MAIN_GRAPH_SEED_START,
    MAIN_INSTANCES,
    MAIN_SIZES,
    run_matrix,
    validate_matrix_request,
    variants_for_size,
)
from synthetic_join_order.plan import (  # noqa: E402
    PlanDriverError,
    bootstrap_metadata,
    postgres_server_version,
)
from synthetic_join_order.qualify import QualificationError  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--sizes", default=",".join(str(size) for size in MAIN_SIZES))
    parser.add_argument("--instances", type=int, default=MAIN_INSTANCES)
    parser.add_argument(
        "--variants",
        default=None,
        help=(
            "comma-separated variant override; defaults to configured baselines, "
            "and sizes with fewer than two eligible variants are skipped"
        ),
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--bootstrap-metadata", action="store_true")
    parser.add_argument("--statement-timeout-ms", type=int, default=60_000)
    parser.add_argument("--graph-seed-start", type=int, default=MAIN_GRAPH_SEED_START)
    parser.add_argument(
        "--cardinality-seed-start", type=int, default=MAIN_CARDINALITY_SEED_START
    )
    parser.add_argument(
        "--postgres-source",
        required=True,
        type=Path,
        help="clean PostgreSQL source checkout containing the patch under test",
    )
    return parser.parse_args()


def _csv_ints(value: str) -> tuple[int, ...]:
    try:
        result = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as exc:
        raise ValueError(f"invalid integer list: {value}") from exc
    if not result:
        raise ValueError("integer list must not be empty")
    return result


def _command_output(command: list[str]) -> str:
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise ValueError(
            f"metadata command failed ({result.returncode}): {' '.join(command)}"
        )
    return result.stdout.strip()


def _postgres_metadata(args: argparse.Namespace) -> dict[str, object]:
    """Record the declared patch revision, without claiming a binary match."""
    source = args.postgres_source.resolve()
    if not source.is_dir():
        raise ValueError(f"PostgreSQL source directory does not exist: {source}")
    try:
        inside_work_tree = _command_output(
            ["git", "-C", str(source), "rev-parse", "--is-inside-work-tree"]
        )
    except ValueError as exc:
        raise ValueError(f"PostgreSQL source is not a git checkout: {source}") from exc
    if inside_work_tree != "true":
        raise ValueError(f"PostgreSQL source is not a git checkout: {source}")
    if _command_output(
        ["git", "-C", str(source), "status", "--porcelain", "--untracked-files=all"]
    ):
        raise ValueError(f"PostgreSQL source checkout is not clean: {source}")
    return {
        "source_revision": _command_output(
            ["git", "-C", str(source), "rev-parse", "HEAD"]
        ),
    }


def _benchmark_metadata() -> dict[str, object]:
    """Record the benchmark code revision."""
    return {
        "source_revision": _command_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"]
        ),
    }


def main() -> int:
    args = parse_args()
    conn = ConnOpts(host=args.host, port=args.port, user=args.user)
    try:
        sizes = _csv_ints(args.sizes)
        registry = load_variants()
        names = select_variant_names(registry, args.variants)
        variants = tuple(registry[name] for name in names)
        runnable_sizes, skipped_sizes = validate_matrix_request(
            sizes,
            args.instances,
            variants,
            args.output,
            statement_timeout_ms=args.statement_timeout_ms,
            graph_seed_start=args.graph_seed_start,
            cardinality_seed_start=args.cardinality_seed_start,
        )
        if skipped_sizes:
            print(
                "warning: skipping requested sizes with fewer than two eligible "
                "variants: "
                + ", ".join(
                    f"{size} ({', '.join(variant.name for variant in variants_for_size(variants, size)) or 'none'})"
                    for size in skipped_sizes
                )
                + "; executing sizes: "
                + ", ".join(str(size) for size in runnable_sizes),
                file=sys.stderr,
            )
        benchmark_metadata = _benchmark_metadata()
        postgres_metadata = _postgres_metadata(args)
        # This is live-server context, not proof that the declared source
        # revision produced the postmaster reached through --host/--port.
        postgres_metadata["server_version"] = postgres_server_version(args.db, conn)
        if args.bootstrap_metadata:
            bootstrap_metadata(args.db, conn)
        rows = run_matrix(
            args.db,
            sizes,
            args.instances,
            variants,
            args.output,
            conn,
            statement_timeout_ms=args.statement_timeout_ms,
            graph_seed_start=args.graph_seed_start,
            cardinality_seed_start=args.cardinality_seed_start,
            benchmark_metadata=benchmark_metadata,
            postgres_metadata=postgres_metadata,
        )
        run_context = json.loads((args.output / "run.json").read_text())
    except (OSError, ValueError, PlanDriverError, QualificationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    counts = {
        status: sum(row["status"] == status for row in rows)
        for status in ("ok", "timeout", "error")
    }
    print(
        f"matrix-complete rows={len(rows)} ok={counts['ok']} "
        f"timeout={counts['timeout']} error={counts['error']} "
        f"dp_anomalies={run_context['dp_anomaly_count']} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
