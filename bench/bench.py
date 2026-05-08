#!/usr/bin/env python3
"""Command-line entry point for the PostgreSQL join-order benchmark harness.

It parses user commands, resolves scenarios/variants/datasets, and delegates
database setup or execution to the narrower ``bench_*`` modules.
"""

from __future__ import annotations

import argparse

from bench_common import ConnOpts, Scenario, Variant, die
from bench_config import (
    BUILT_IN_VARIANTS,
    available_datasets,
    dataset_db_name,
    load_run_settings,
    load_scenarios,
    load_variants,
    resolve_dataset_runs,
    resolve_variant_names,
)
from bench_prepare import prepare_scenario
from bench_run import run_scenario


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    scenarios = args._scenarios
    conn = ConnOpts(
        host=getattr(args, "host", None),
        port=getattr(args, "port", None),
        user=getattr(args, "user", None),
    )

    # Discovery commands are read-only; they should not connect to benchmark databases.
    if args.cmd == "list":
        if args.what == "scenarios":
            print_scenarios(scenarios)
            return
        if args.what == "variants":
            print_variants(load_variants())
            return
        if args.what == "datasets":
            print_datasets()
            return
        die(f"unknown list target: {args.what}")

    scenario_name = getattr(args, "scenario")
    if scenario_name not in scenarios:
        die(f"unknown scenario '{scenario_name}' (see: python3 bench/bench.py list scenarios)")
    scenario = scenarios[scenario_name]

    # Prepare recreates the databases required by the selected scenario.
    if args.cmd == "prepare":
        prepare_scenario(
            scenario,
            csv_dir=args.csv_dir,
            conn=conn,
        )
        return

    if args.cmd == "run":
        # The run path resolves CLI choices before handing execution
        # to bench_run.py, which owns SQL execution and artifact writing.
        variants = load_variants()
        run_session_gucs = load_run_settings()
        variant_names = resolve_variant_names(scenario, variants, args.variants)
        resolved_runs = resolve_dataset_runs(
            scenario,
            variant_names,
            min_join=args.min_join,
        )
        run_scenario(
            scenario,
            variants,
            variant_names,
            resolved_runs,
            conn=conn,
            run_session_gucs=run_session_gucs,
            tag=args.tag,
            reuse_stats=args.reuse_stats,
        )
        return

    die(f"unknown command: {args.cmd}")


def build_parser() -> argparse.ArgumentParser:
    # Load scenarios during parser construction so list/help validation and
    # command execution share the same scenario registry.
    scenarios = load_scenarios()

    ap = argparse.ArgumentParser(description="Scenario-driven join-order benchmark harness (PostgreSQL).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_conn_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=None, help="PostgreSQL host to pass to psql")
        p.add_argument("--port", type=int, default=None, help="PostgreSQL port to pass to psql")
        p.add_argument("--user", default=None, help="PostgreSQL user to pass to psql")

    ap_list = sub.add_parser("list", help="List scenarios, variants, or datasets.")
    ap_list.add_argument("what", choices=["scenarios", "variants", "datasets"])

    ap_prepare = sub.add_parser("prepare", help="Recreate databases for a scenario.")
    ap_prepare.add_argument("scenario", help="scenario name (see: list scenarios)")
    ap_prepare.add_argument("--csv-dir", default=None, help="IMDB CSV directory for IMDB-backed datasets")
    add_conn_args(ap_prepare)

    ap_run = sub.add_parser("run", help="Run a scenario and write results to outputs/<run_id>/")
    ap_run.add_argument("scenario", help="scenario name (see: list scenarios)")
    ap_run.add_argument("--variants", default=None, help="variant1,variant2 (optional override)")
    ap_run.add_argument("--min-join", type=positive_int, default=None, help="only run queries with join_size >= N")
    ap_run.add_argument(
        "--reuse-stats",
        action="store_true",
        help="reuse existing database statistics instead of running VACUUM FREEZE ANALYZE and CHECKPOINT",
    )
    ap_run.add_argument("--tag", default="", help="optional local tag for this run or the build under test")
    add_conn_args(ap_run)

    ap.set_defaults(_scenarios=scenarios)
    return ap


def positive_int(raw: str) -> int:
    """argparse type for positive integer options such as --min-join."""

    try:
        value = int(raw)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"expected integer, got {raw!r}") from e
    if value < 1:
        raise argparse.ArgumentTypeError(f"expected integer >= 1, got {value}")
    return value


def print_scenarios(scenarios: dict[str, Scenario]) -> None:
    """Print scenario names, their datasets, and short descriptions."""

    print("Scenarios")
    print("name\tdatasets\tdescription")
    for name, scenario in scenarios.items():
        dataset_names = list(dict.fromkeys(scenario.datasets))
        datasets = ", ".join(dataset_names)
        print(f"{name}\t{datasets}\t{scenario.description}")
    print()


def print_variants(variants: dict[str, Variant]) -> None:
    """Print built-in and extra variants visible to the CLI."""

    built_in_names = {variant.name for variant in BUILT_IN_VARIANTS}
    print("Variants")
    print("name\tsource\tlabel")
    for name in sorted(variants):
        variant = variants[name]
        source = "builtin" if name in built_in_names else "extra"
        print(f"{variant.name}\t{source}\t{variant.label}")
    print()


def print_datasets() -> None:
    """Print manifest datasets and their default benchmark database names."""

    print("Datasets")
    print("name\tdatabase")
    for dataset in available_datasets():
        print(f"{dataset}\t{dataset_db_name(dataset)}")
    print()


if __name__ == "__main__":
    main()
