from __future__ import annotations

from pathlib import Path
from typing import Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

from bench_catalog import available_datasets, dataset_db_name
from bench_common import (
    SCENARIOS_CONFIG_PATH,
    DEFAULT_VARIANTS_CONFIG_PATH,
    DatasetSpec,
    ResolvedDatasetRun,
    Scenario,
    Variant,
    die,
    parse_csv_list,
)


def load_variants(path: Optional[Path] = None) -> dict[str, Variant]:
    variants_path = Path(path) if path is not None else DEFAULT_VARIANTS_CONFIG_PATH
    if not variants_path.is_file():
        die(f"missing variants config: {variants_path}")
    data = tomllib.loads(variants_path.read_text())
    raw_variants = data.get("variant")
    if not isinstance(raw_variants, list) or not raw_variants:
        die(f"{variants_path} must define at least one [[variant]] entry")

    out: dict[str, Variant] = {}
    for entry in raw_variants:
        if not isinstance(entry, dict):
            die(f"bad [[variant]] entry in {variants_path}")
        name = str(entry.get("name", "")).strip()
        if not name:
            die(f"variant in {variants_path} is missing name")
        if name in out:
            die(f"duplicate variant name '{name}' in {variants_path}")
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
    if not SCENARIOS_CONFIG_PATH.is_file():
        die(f"missing scenarios config: {SCENARIOS_CONFIG_PATH}")
    data = tomllib.loads(SCENARIOS_CONFIG_PATH.read_text())
    raw_scenarios = data.get("scenario")
    if not isinstance(raw_scenarios, dict) or not raw_scenarios:
        die(f"{SCENARIOS_CONFIG_PATH} must define at least one [scenario.<name>] entry")

    known_datasets = set(available_datasets())
    out: dict[str, Scenario] = {}
    for name, cfg in raw_scenarios.items():
        if not isinstance(cfg, dict):
            die(f"bad scenario entry '{name}' in {SCENARIOS_CONFIG_PATH}")
        description = str(cfg.get("description", "")).strip()
        raw_default_variants = cfg.get("default_variants", [])
        if not isinstance(raw_default_variants, list):
            die(f"scenario '{name}' has invalid default_variants in {SCENARIOS_CONFIG_PATH}")
        default_variants = tuple(str(item) for item in raw_default_variants if str(item).strip())
        reps = int(cfg.get("reps", 1))
        statement_timeout_ms = int(cfg.get("statement_timeout_ms", 0))
        stabilize = str(cfg.get("stabilize", "none"))
        variant_order_mode = str(cfg.get("variant_order_mode", "fixed"))
        raw_session_gucs = cfg.get("session_gucs", {})
        if not isinstance(raw_session_gucs, dict):
            die(f"scenario '{name}' has invalid session_gucs in {SCENARIOS_CONFIG_PATH}")

        raw_datasets = cfg.get("dataset", [])
        if not isinstance(raw_datasets, list):
            die(f"scenario '{name}' has invalid dataset entries in {SCENARIOS_CONFIG_PATH}")

        datasets: list[DatasetSpec] = []
        for item in raw_datasets:
            if not isinstance(item, dict):
                die(f"scenario '{name}' has invalid dataset entry in {SCENARIOS_CONFIG_PATH}")
            dataset = str(item.get("name", "")).strip()
            if not dataset:
                die(f"scenario '{name}' has dataset entry without name in {SCENARIOS_CONFIG_PATH}")
            if dataset not in known_datasets:
                die(f"scenario '{name}' references unknown dataset '{dataset}'")
            raw_variants = item.get("variants")
            variants: Optional[tuple[str, ...]]
            if raw_variants is None:
                variants = None
            else:
                if not isinstance(raw_variants, list):
                    die(f"scenario '{name}' dataset '{dataset}' has invalid variants list")
                variants = tuple(str(v) for v in raw_variants if str(v).strip())
            raw_exclude_variants = item.get("exclude_variants")
            exclude_variants: Optional[tuple[str, ...]]
            if raw_exclude_variants is None:
                exclude_variants = None
            else:
                if variants is not None:
                    die(f"scenario '{name}' dataset '{dataset}' cannot define both variants and exclude_variants")
                if not isinstance(raw_exclude_variants, list):
                    die(f"scenario '{name}' dataset '{dataset}' has invalid exclude_variants list")
                exclude_variants = tuple(str(v) for v in raw_exclude_variants if str(v).strip())
            datasets.append(
                DatasetSpec(
                    dataset=dataset,
                    min_join=int(item["min_join"]) if "min_join" in item else None,
                    max_join=int(item["max_join"]) if "max_join" in item else None,
                    max_queries=int(item["max_queries"]) if "max_queries" in item else None,
                    variants=variants,
                    exclude_variants=exclude_variants,
                )
            )

        out[name] = Scenario(
            name=name,
            description=description,
            default_variants=default_variants,
            reps=reps,
            statement_timeout_ms=statement_timeout_ms,
            stabilize=stabilize,
            variant_order_mode=variant_order_mode,
            session_gucs=tuple((str(k), v) for k, v in raw_session_gucs.items()),
            datasets=tuple(datasets),
        )
    return out


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
                min_join=spec.min_join,
                max_join=spec.max_join,
                max_queries=spec.max_queries,
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
                min_join=None,
                max_join=None,
                max_queries=None,
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
