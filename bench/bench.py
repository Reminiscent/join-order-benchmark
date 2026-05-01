#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from bench_common import ConnOpts, die
from bench_registry import (
    load_scenarios,
    load_variants,
    print_datasets,
    print_scenarios,
    print_variants,
    resolve_dataset_runs,
    resolve_variant_names,
)
from bench_prepare import prepare_scenario
from bench_run import run_scenario


DEFAULT_WARMUP_RUNS = 1


def build_parser() -> argparse.ArgumentParser:
    scenarios = load_scenarios()

    ap = argparse.ArgumentParser(description="Scenario-driven join-order benchmark harness (PostgreSQL).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_conn_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=None, help="PostgreSQL host (for example localhost)")
        p.add_argument("--port", type=int, default=5432, help="PostgreSQL port (default: 5432)")
        p.add_argument("--user", default=None, help="PostgreSQL user")

    def add_variant_file_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--variants-file",
            type=Path,
            default=None,
            help="extra variant TOML file for patch-specific algorithms",
        )

    ap_list = sub.add_parser("list", help="List scenarios, variants, or datasets.")
    ap_list.add_argument("what", choices=["scenarios", "variants", "datasets"])
    add_variant_file_arg(ap_list)

    ap_prepare = sub.add_parser("prepare", help="Prepare databases for a scenario.")
    ap_prepare.add_argument("scenario", help="scenario name (see: list scenarios)")
    ap_prepare.add_argument("--csv-dir", default=None, help="IMDB CSV directory for IMDB-backed datasets")
    ap_prepare.add_argument(
        "--force-recreate",
        action="store_true",
        help="drop and recreate an existing benchmark database instead of skipping or failing",
    )
    add_conn_args(ap_prepare)

    ap_run = sub.add_parser("run", help="Run a scenario and write results to outputs/<run_id>/")
    ap_run.add_argument("scenario", help="scenario name (see: list scenarios)")
    ap_run.add_argument("--variants", default=None, help="variant1,variant2 (optional override)")
    add_variant_file_arg(ap_run)
    ap_run.add_argument(
        "--resume-run-id",
        default=None,
        help="resume an existing outputs/<run_id> directory from the next unfinished group boundary",
    )
    ap_run.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=None,
        help="override the per-statement guardrail timeout in milliseconds",
    )
    ap_run.add_argument("--tag", default="", help="optional local tag for this run or the build under test")
    ap_run.add_argument(
        "--fail-on-error",
        action="store_true",
        help="exit non-zero if any non-timeout query errors occur",
    )
    add_conn_args(ap_run)

    ap.set_defaults(_scenarios=scenarios)
    return ap


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    scenarios = args._scenarios
    conn = ConnOpts(host=getattr(args, "host", None), port=getattr(args, "port", None), user=getattr(args, "user", None))

    if args.cmd == "list":
        if args.what == "scenarios":
            print_scenarios(scenarios)
            return
        if args.what == "variants":
            variants = load_variants(args.variants_file)
            print_variants(variants)
            return
        if args.what == "datasets":
            print_datasets()
            return
        die(f"unknown list target: {args.what}")

    scenario_name = getattr(args, "scenario")
    if scenario_name not in scenarios:
        die(f"unknown scenario '{scenario_name}' (see: python3 bench/bench.py list scenarios)")
    scenario = scenarios[scenario_name]

    if args.cmd == "prepare":
        prepare_scenario(
            scenario,
            csv_dir=args.csv_dir,
            conn=conn,
            force_recreate=args.force_recreate,
        )
        return

    if args.cmd == "run":
        variants = load_variants(args.variants_file)
        variant_names = resolve_variant_names(scenario, variants, args.variants)
        resolved_runs = resolve_dataset_runs(
            scenario,
            variant_names,
        )
        run_scenario(
            scenario,
            variants,
            variant_names,
            resolved_runs,
            conn=conn,
            reps=scenario.reps,
            statement_timeout_ms=(
                args.statement_timeout_ms
                if args.statement_timeout_ms is not None
                else scenario.statement_timeout_ms
            ),
            stabilize=scenario.stabilize,
            variant_order_mode=scenario.variant_order_mode,
            warmup_runs=DEFAULT_WARMUP_RUNS,
            resume_run_id=args.resume_run_id,
            tag=args.tag,
            fail_on_error=args.fail_on_error,
        )
        return

    die(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
