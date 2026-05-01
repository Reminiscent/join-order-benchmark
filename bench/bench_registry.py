from __future__ import annotations

from pathlib import Path
from typing import Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

from bench_catalog import available_datasets, dataset_db_name
from bench_common import (
    DatasetSpec,
    ResolvedDatasetRun,
    Scenario,
    Variant,
    die,
    parse_csv_list,
)


DEFAULT_SCENARIO_VARIANTS = ("dp", "geqo")
DEFAULT_SCENARIO_REPS = 3
DEFAULT_STATEMENT_TIMEOUT_MS = 600000
DEFAULT_STABILIZE = "vacuum_freeze_analyze"
DEFAULT_VARIANT_ORDER_MODE = "rotate"
DEFAULT_SESSION_GUCS = (
    ("join_collapse_limit", 100),
    ("max_parallel_workers_per_gather", 0),
    ("work_mem", "1GB"),
    ("effective_cache_size", "8GB"),
)
BUILT_IN_VARIANTS = (
    Variant(
        name="dp",
        label="Dynamic Programming",
        session_gucs=(("geqo_threshold", 100),),
        optional_session_gucs=(("enable_goo_join_search", "off"),),
    ),
    Variant(
        name="geqo",
        label="GEQO",
        session_gucs=(("geqo_threshold", 2),),
        optional_session_gucs=(("enable_goo_join_search", "off"),),
    ),
)

MAIN_DATASETS = (
    DatasetSpec(dataset="job"),
    DatasetSpec(dataset="job_complex"),
)

EXTENDED_EXTRA_DATASETS = (
    DatasetSpec(dataset="sqlite_select5"),
    DatasetSpec(dataset="gpuqo_chain_small"),
    DatasetSpec(dataset="gpuqo_star_small"),
    DatasetSpec(dataset="gpuqo_snowflake_small"),
    DatasetSpec(dataset="gpuqo_clique_small", exclude_variants=("dp",)),
    DatasetSpec(dataset="gpuqo_clique_small", variants=("dp",), max_join=12),
)


def built_in_scenario(
    *,
    name: str,
    description: str,
    datasets: tuple[DatasetSpec, ...],
) -> Scenario:
    return Scenario(
        name=name,
        description=description,
        default_variants=DEFAULT_SCENARIO_VARIANTS,
        reps=DEFAULT_SCENARIO_REPS,
        statement_timeout_ms=DEFAULT_STATEMENT_TIMEOUT_MS,
        stabilize=DEFAULT_STABILIZE,
        variant_order_mode=DEFAULT_VARIANT_ORDER_MODE,
        session_gucs=DEFAULT_SESSION_GUCS,
        datasets=datasets,
    )


def load_variants(path: Optional[Path] = None) -> dict[str, Variant]:
    out = {variant.name: variant for variant in BUILT_IN_VARIANTS}
    if path is None:
        return out

    variants_path = Path(path)
    if not variants_path.is_file():
        die(f"missing variants file: {variants_path}")
    data = tomllib.loads(variants_path.read_text())
    raw_variants = data.get("variant")
    if not isinstance(raw_variants, list) or not raw_variants:
        die(f"{variants_path} must define at least one [[variant]] entry")

    for entry in raw_variants:
        if not isinstance(entry, dict):
            die(f"bad [[variant]] entry in {variants_path}")
        name = str(entry.get("name", "")).strip()
        if not name:
            die(f"variant in {variants_path} is missing name")
        if name in out:
            die(f"variant file cannot redefine built-in or duplicate variant '{name}'")
        label = str(entry.get("label", name)).strip() or name
        raw_gucs = entry.get("session_gucs", {})
        if not isinstance(raw_gucs, dict):
            die(f"variant '{name}' has invalid session_gucs in {variants_path}")
        raw_optional_gucs = entry.get("optional_session_gucs", {})
        if not isinstance(raw_optional_gucs, dict):
            die(f"variant '{name}' has invalid optional_session_gucs in {variants_path}")
        out[name] = Variant(
            name=name,
            label=label,
            session_gucs=tuple((str(k), v) for k, v in raw_gucs.items()),
            optional_session_gucs=tuple((str(k), v) for k, v in raw_optional_gucs.items()),
        )
    return out


def load_scenarios() -> dict[str, Scenario]:
    extended_datasets = MAIN_DATASETS + EXTENDED_EXTRA_DATASETS
    scenarios = (
        built_in_scenario(
            name="main",
            description="Primary algorithm validation path on complete JOB and JOB-Complex.",
            datasets=MAIN_DATASETS,
        ),
        built_in_scenario(
            name="extended",
            description="Broader validation with self-contained planning-stress workloads, excluding CEB IMDB 3k.",
            datasets=extended_datasets,
        ),
        built_in_scenario(
            name="full",
            description="Complete built-in workload, including the heavy CEB IMDB 3k suite.",
            datasets=extended_datasets + (DatasetSpec(dataset="imdb_ceb_3k"),),
        ),
    )
    return {scenario.name: scenario for scenario in scenarios}


def resolve_variant_names(
    scenario: Scenario,
    variants: dict[str, Variant],
    override_csv: Optional[str],
) -> tuple[str, ...]:
    names = tuple(parse_csv_list(override_csv)) if override_csv else scenario.default_variants
    if not names:
        die(f"scenario '{scenario.name}' does not define default_variants and no --variants were provided")
    for name in names:
        if name not in variants:
            die(f"unknown variant '{name}' (see: python3 bench/bench.py list variants)")
    return names


def resolve_dataset_runs(
    scenario: Scenario,
    variant_names: tuple[str, ...],
) -> list[ResolvedDatasetRun]:
    resolved: list[ResolvedDatasetRun] = []

    for spec in scenario.datasets:
        if spec.variants is None:
            entry_variants = variant_names
        else:
            entry_variants = tuple(name for name in variant_names if name in spec.variants)
        if spec.exclude_variants is not None:
            excluded = set(spec.exclude_variants)
            entry_variants = tuple(name for name in entry_variants if name not in excluded)
        if not entry_variants:
            continue
        resolved.append(
            ResolvedDatasetRun(
                dataset=spec.dataset,
                db=dataset_db_name(spec.dataset),
                max_join=spec.max_join,
                variants=entry_variants,
            )
        )

    if not resolved:
        die(f"scenario '{scenario.name}' resolved to zero dataset runs with the selected variants")
    return resolved


def resolve_prepare_dataset_runs(
    scenario: Scenario,
) -> list[ResolvedDatasetRun]:
    known_datasets = set(available_datasets())
    datasets = list(dict.fromkeys(spec.dataset for spec in scenario.datasets))

    resolved: list[ResolvedDatasetRun] = []
    for dataset in datasets:
        if dataset not in known_datasets:
            die(f"unknown dataset '{dataset}' (see: python3 bench/bench.py list datasets)")
        resolved.append(
            ResolvedDatasetRun(
                dataset=dataset,
                db=dataset_db_name(dataset),
                max_join=None,
                variants=(),
            )
        )
    return resolved


def print_scenarios(scenarios: dict[str, Scenario]) -> None:
    for name, scenario in scenarios.items():
        dataset_names = list(dict.fromkeys(spec.dataset for spec in scenario.datasets))
        datasets = ", ".join(dataset_names)
        print(f"{name}\t{scenario.description}\tvariants={','.join(scenario.default_variants)}\tdatasets={datasets}")


def print_variants(variants: dict[str, Variant]) -> None:
    for name in sorted(variants):
        variant = variants[name]
        print(f"{variant.name}\t{variant.label}")


def print_datasets() -> None:
    for dataset in available_datasets():
        print(dataset)
