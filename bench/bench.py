#!/usr/bin/env python3

from __future__ import annotations

import argparse

from bench_common import ConnOpts, die
from bench_config import (
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


def build_parser() -> argparse.ArgumentParser:
    scenarios = load_scenarios()

    ap = argparse.ArgumentParser(description="Scenario-driven join-order benchmark harness (PostgreSQL).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_conn_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=None, help="PostgreSQL host (for example localhost)")
        p.add_argument("--port", type=int, default=None, help="PostgreSQL port (for example 54321)")
        p.add_argument("--user", default=None, help="PostgreSQL user")

    ap_list = sub.add_parser("list", help="List scenarios, variants, or datasets.")
    ap_list.add_argument("what", choices=["scenarios", "variants", "datasets"])

    ap_prepare = sub.add_parser("prepare", help="Prepare databases for a scenario.")
    ap_prepare.add_argument("scenario", help="scenario name (see: list scenarios)")
    ap_prepare.add_argument("--datasets", default=None, help="dataset1,dataset2 (required for custom)")
    ap_prepare.add_argument("--csv-dir", default=None, help="IMDB CSV directory for IMDB-backed datasets")
    ap_prepare.add_argument(
        "--force-recreate",
        action="store_true",
        help="drop and recreate an existing benchmark database instead of skipping or failing",
    )
    add_conn_args(ap_prepare)

    ap_run = sub.add_parser("run", help="Run a scenario and write results to outputs/<run_id>/")
    ap_run.add_argument("scenario", help="scenario name (see: list scenarios)")
    ap_run.add_argument("--datasets", default=None, help="dataset1,dataset2 (required for custom)")
    ap_run.add_argument("--variants", default=None, help="variant1,variant2 (optional override)")
    ap_run.add_argument(
        "--resume-run-id",
        default=None,
        help="resume an existing non-smoke outputs/<run_id> directory from the next unfinished group boundary",
    )
    ap_run.add_argument("--min-join", type=int, default=None, help="custom scenario only")
    ap_run.add_argument("--max-join", type=int, default=None, help="custom scenario only")
    ap_run.add_argument("--reps", type=int, default=None, help="override scenario repetitions")
    ap_run.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=None,
        help="override scenario statement timeout in milliseconds",
    )
    ap_run.add_argument(
        "--stabilize",
        choices=["vacuum_freeze_analyze", "none"],
        default=None,
        help="override scenario stabilize mode",
    )
    ap_run.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="run this many full discarded workload passes before the measured repetitions",
    )
    ap_run.add_argument(
        "--skip-measured-after-warmup-timeout",
        action="store_true",
        default=True,
        help=(
            "after a warmup statement_timeout on an exact (dataset, query, variant), "
            "record measured repetitions for that same combination as skipped timeouts "
            "instead of re-running them"
        ),
    )
    ap_run.add_argument(
        "--no-skip-measured-after-warmup-timeout",
        action="store_false",
        dest="skip_measured_after_warmup_timeout",
        help="re-run measured repetitions even when the exact combination already timed out during warmup",
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
    variants = load_variants()
    conn = ConnOpts(host=getattr(args, "host", None), port=getattr(args, "port", None), user=getattr(args, "user", None))

    if args.cmd == "list":
        if args.what == "scenarios":
            print_scenarios(scenarios)
            return
        if args.what == "variants":
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
            custom_datasets_csv=args.datasets,
            csv_dir=args.csv_dir,
            conn=conn,
            force_recreate=args.force_recreate,
        )
        return

    if args.cmd == "run":
        variant_names = resolve_variant_names(scenario, variants, args.variants)
        resolved_runs = resolve_dataset_runs(
            scenario,
            variant_names,
            custom_datasets_csv=args.datasets,
            custom_min_join=args.min_join,
            custom_max_join=args.max_join,
            custom_max_queries=None,
        )
        run_scenario(
            scenario,
            variants,
            variant_names,
            resolved_runs,
            conn=conn,
            reps=args.reps if args.reps is not None else scenario.reps,
            statement_timeout_ms=(
                args.statement_timeout_ms
                if args.statement_timeout_ms is not None
                else scenario.statement_timeout_ms
            ),
            stabilize=args.stabilize if args.stabilize is not None else scenario.stabilize,
            variant_order_mode=scenario.variant_order_mode,
            warmup_runs=args.warmup_runs,
            skip_measured_after_warmup_timeout=args.skip_measured_after_warmup_timeout,
            resume_run_id=args.resume_run_id,
            tag=args.tag,
            fail_on_error=args.fail_on_error or scenario.name == "smoke",
        )
        return

    die(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
