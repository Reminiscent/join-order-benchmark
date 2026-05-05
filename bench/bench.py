#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from bench_common import ConnOpts, REPO_ROOT, Scenario, Variant, die
from bench_workloads import (
    BUILT_IN_VARIANTS,
    DEFAULT_VARIANTS_FILE,
    available_datasets,
    dataset_db_name,
    load_scenarios,
    load_variants,
    resolve_dataset_runs,
    resolve_variant_names,
    resolve_variants_file,
)
from bench_prepare import prepare_scenario
from bench_run import run_scenario


def positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"expected integer, got {raw!r}") from e
    if value < 1:
        raise argparse.ArgumentTypeError(f"expected integer >= 1, got {value}")
    return value


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def print_scenarios(scenarios: dict[str, Scenario]) -> None:
    print("Scenarios")
    print("name\tdatasets\tdescription")
    for name, scenario in scenarios.items():
        dataset_names = list(dict.fromkeys(scenario.datasets))
        datasets = ", ".join(dataset_names)
        print(f"{name}\t{datasets}\t{scenario.description}")
    print()


def print_variants(
    variants: dict[str, Variant],
    variants_file: Path | None = None,
) -> None:
    built_in_names = {variant.name for variant in BUILT_IN_VARIANTS}
    print("Variants")
    if variants_file is not None:
        print(f"extra_variants_file\t{display_path(variants_file)}")
    print("name\tsource\tlabel")
    for name in sorted(variants):
        variant = variants[name]
        source = "builtin" if name in built_in_names else "extra"
        print(f"{variant.name}\t{source}\t{variant.label}")
    if set(variants) == built_in_names:
        print(
            f"Hint: {display_path(DEFAULT_VARIANTS_FILE)} was not found; "
            "pass --variants-file PATH to include patch-specific variants."
        )
    print()


def print_datasets() -> None:
    print("Datasets")
    print("name\tdatabase")
    for dataset in available_datasets():
        print(f"{dataset}\t{dataset_db_name(dataset)}")
    print()


def build_parser() -> argparse.ArgumentParser:
    scenarios = load_scenarios()

    ap = argparse.ArgumentParser(description="Scenario-driven join-order benchmark harness (PostgreSQL).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_conn_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=None, help="PostgreSQL host to pass to psql")
        p.add_argument("--port", type=int, default=None, help="PostgreSQL port to pass to psql")
        p.add_argument("--user", default=None, help="PostgreSQL user to pass to psql")

    def add_variant_file_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--variants-file",
            type=Path,
            default=None,
            help="override the default examples/variants.toml extra variant file",
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
    ap_run.add_argument("--min-join", type=positive_int, default=None, help="only run queries with join_size >= N")
    add_variant_file_arg(ap_run)
    ap_run.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=None,
        help="override the per-statement guardrail timeout in milliseconds",
    )
    ap_run.add_argument("--tag", default="", help="optional local tag for this run or the build under test")
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
            variants_file = resolve_variants_file(args.variants_file)
            variants = load_variants(variants_file)
            print_variants(variants, variants_file)
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
            min_join=args.min_join,
        )
        run_scenario(
            scenario,
            variants,
            variant_names,
            resolved_runs,
            conn=conn,
            statement_timeout_ms=(
                args.statement_timeout_ms
                if args.statement_timeout_ms is not None
                else scenario.statement_timeout_ms
            ),
            tag=args.tag,
        )
        return

    die(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
