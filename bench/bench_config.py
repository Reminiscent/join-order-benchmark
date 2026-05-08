"""Benchmark configuration registry and file-backed settings.

This module owns the benchmark surface: built-in scenarios, checked-in config
files, dataset mappings, query manifest access, variant resolution, and SQL
wrapping rules.  It does not execute PostgreSQL.
"""

from __future__ import annotations

import csv
import functools
import re
import tomllib
from pathlib import Path
from typing import Any, Optional

from bench_common import (
    MANIFEST_PATH,
    QueryMeta,
    REPO_ROOT,
    ResolvedDatasetRun,
    Scenario,
    Variant,
    die,
    parse_csv_list,
)


# Paths and scalar config values.
CONFIG_DIR = REPO_ROOT / "config"
BENCHMARK_SETTINGS_FILE = CONFIG_DIR / "benchmark_settings.toml"
VARIANTS_FILE = CONFIG_DIR / "variants.toml"
SCALAR_SETTING_TYPES = (str, int, float, bool)


# Built-in scenario surface.
MAIN_DATASETS = (
    "job",
    "job_complex",
)

CEB_DATASETS = (
    "imdb_ceb_3k",
)

PLANNING_DATASETS = (
    "sqlite_select5",
    "gpuqo_chain_small",
    "gpuqo_star_small",
    "gpuqo_snowflake_small",
    "gpuqo_clique_small",
)

IMDB_DATASETS = {"job", "job_complex", "imdb_ceb_3k"}

DEFAULT_DB_BY_DATASET = {
    "job": "imdb_bench",
    "job_complex": "imdb_bench",
    "imdb_ceb_3k": "imdb_bench",
    "sqlite_select5": "sqlite_select5_bench",
    "gpuqo_chain_small": "gpuqo_chain_small_bench",
    "gpuqo_clique_small": "gpuqo_clique_small_bench",
    "gpuqo_star_small": "gpuqo_star_small_bench",
    "gpuqo_snowflake_small": "gpuqo_snowflake_small_bench",
}


# Scenario registry.
# Scenarios select workload groups only.  Algorithm choices come from
# config/variants.toml and the CLI --variants override.


def load_scenarios() -> dict[str, Scenario]:
    """Return the public scenario registry used by the CLI."""

    scenarios = (
        Scenario(
            name="main",
            description="Primary algorithm validation path on complete JOB and JOB-Complex.",
            datasets=MAIN_DATASETS,
        ),
        Scenario(
            name="extended",
            description="Main validation plus the heavier CEB IMDB 3k workload.",
            datasets=MAIN_DATASETS + CEB_DATASETS,
        ),
        Scenario(
            name="planning",
            description="Self-contained synthetic workloads for planning/search-space stress.",
            datasets=PLANNING_DATASETS,
        ),
    )
    return {scenario.name: scenario for scenario in scenarios}


# Config file loading.
# ``config/*.toml`` is the editable benchmark configuration entry point.


def parse_guc_mapping(
    raw_gucs: dict[str, Any],
    source_path: Path,
    *,
    context: str,
) -> tuple[tuple[str, Any], ...]:
    """Parse TOML key/value pairs into validated session GUC assignments."""

    gucs: list[tuple[str, Any]] = []
    for raw_name, value in raw_gucs.items():
        name = str(raw_name).strip()
        if not name:
            die(f"{source_path} contains an empty GUC name in {context}")
        if not isinstance(value, SCALAR_SETTING_TYPES):
            die(f"{source_path} setting '{name}' in {context} must be a scalar GUC value")
        gucs.append((name, value))
    return tuple(gucs)


def load_run_settings() -> tuple[tuple[str, Any], ...]:
    """Load shared session GUCs from config/benchmark_settings.toml."""

    settings_path = BENCHMARK_SETTINGS_FILE
    if not settings_path.is_file():
        die(f"missing benchmark settings file: {settings_path}")

    data = tomllib.loads(settings_path.read_text())
    if not data:
        die(f"{settings_path} must define at least one benchmark setting")
    return parse_guc_mapping(data, settings_path, context="benchmark settings")


def load_variants() -> dict[str, Variant]:
    """Load the configured variant registry from config/variants.toml."""

    variants_path = VARIANTS_FILE
    if not variants_path.is_file():
        die(f"missing variants file: {variants_path}")
    data = tomllib.loads(variants_path.read_text())
    raw_variants = data.get("variant", [])
    if not isinstance(raw_variants, list):
        die(f"{variants_path} field 'variant' must be a [[variant]] list")
    if not raw_variants:
        die(f"{variants_path} must define at least one [[variant]]")

    out: dict[str, Variant] = {}
    for entry in raw_variants:
        if not isinstance(entry, dict):
            die(f"bad [[variant]] entry in {variants_path}")
        name = str(entry.get("name", "")).strip()
        if not name:
            die(f"variant in {variants_path} is missing name")
        if name in out:
            die(f"{variants_path} contains duplicate variant '{name}'")
        label = str(entry.get("label", name)).strip() or name
        baseline = entry.get("baseline", False)
        if not isinstance(baseline, bool):
            die(f"variant '{name}' has non-boolean baseline in {variants_path}")
        raw_gucs = entry.get("session_gucs", {})
        if not isinstance(raw_gucs, dict):
            die(f"variant '{name}' has invalid session_gucs in {variants_path}")
        out[name] = Variant(
            name=name,
            label=label,
            session_gucs=parse_guc_mapping(
                raw_gucs,
                variants_path,
                context=f"variant '{name}' session_gucs",
            ),
            baseline=baseline,
        )
    return out


# Query manifest and selection.
# This section reads checked-in query metadata and applies query filters.


@functools.lru_cache(maxsize=1)
def load_manifest_by_dataset() -> dict[str, tuple[QueryMeta, ...]]:
    """Load tools/query_manifest.csv grouped by dataset."""

    if not MANIFEST_PATH.is_file():
        die(f"missing manifest: {MANIFEST_PATH} (run python3 tools/build_query_manifest.py --verify --summary)")

    out: dict[str, list[QueryMeta]] = {}
    with MANIFEST_PATH.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                join_size = int(row["join_size"])
            except Exception as e:  # pragma: no cover
                die(f"bad join_size in manifest row: {row!r} ({e})")
            q = QueryMeta(
                dataset=row["dataset"],
                query_id=row["query_id"],
                query_path=row["query_path"],
                join_size=join_size,
            )
            out.setdefault(q.dataset, []).append(q)

    finalized: dict[str, tuple[QueryMeta, ...]] = {}
    for dataset, queries in out.items():
        queries.sort(key=lambda q: (q.query_id, q.query_path))
        finalized[dataset] = tuple(queries)
    return finalized


def available_datasets() -> tuple[str, ...]:
    """Return dataset names available in the checked-in query manifest."""

    return tuple(sorted(load_manifest_by_dataset().keys()))


def parse_manifest(dataset: str) -> list[QueryMeta]:
    """Return manifest entries for one dataset."""

    manifest = load_manifest_by_dataset()
    if dataset not in manifest:
        die(f"unknown dataset '{dataset}'")
    return list(manifest[dataset])


def select_queries(spec: ResolvedDatasetRun) -> list[QueryMeta]:
    """Select manifest queries for a run spec, applying any min_join filter."""

    queries = parse_manifest(spec.dataset)
    if spec.min_join is not None:
        queries = [q for q in queries if q.join_size >= spec.min_join]
    if not queries:
        die(
            f"no queries selected "
            f"(dataset={spec.dataset}, min_join={spec.min_join})"
        )
    return queries


# Run specification resolution.
# These helpers turn selected scenario/variant choices into concrete work.


def resolve_variant_names(
    scenario: Scenario,
    variants: dict[str, Variant],
    override_csv: Optional[str],
) -> tuple[str, ...]:
    """Resolve the effective variant order for a scenario run."""

    if override_csv:
        names = tuple(parse_csv_list(override_csv))
    else:
        names = tuple(name for name, variant in variants.items() if variant.baseline)
    if not names:
        die(
            f"scenario '{scenario.name}' has no configured baseline variants; "
            "pass --variants or mark at least one variant with baseline = true"
        )
    for name in names:
        if name not in variants:
            die(f"unknown variant '{name}' (see: python3 bench/bench.py list variants)")
    return names


def dataset_db_name(dataset: str) -> str:
    """Return the default PostgreSQL database name for a dataset."""

    if dataset not in DEFAULT_DB_BY_DATASET:
        die(f"no default benchmark database configured for dataset '{dataset}'")
    return DEFAULT_DB_BY_DATASET[dataset]


def resolve_dataset_runs(
    scenario: Scenario,
    variant_names: tuple[str, ...],
    min_join: Optional[int] = None,
) -> list[ResolvedDatasetRun]:
    """Expand a scenario into concrete dataset/database/variant run specs."""

    resolved: list[ResolvedDatasetRun] = []

    for dataset in scenario.datasets:
        resolved.append(
            ResolvedDatasetRun(
                dataset=dataset,
                db=dataset_db_name(dataset),
                variants=variant_names,
                min_join=min_join,
            )
        )

    if not resolved:
        die(f"scenario '{scenario.name}' resolved to zero dataset runs with the selected variants")
    return resolved


def resolve_prepare_dataset_runs(
    scenario: Scenario,
) -> list[ResolvedDatasetRun]:
    """Resolve scenario datasets that the prepare command recreates."""

    known_datasets = set(available_datasets())
    datasets = list(dict.fromkeys(scenario.datasets))

    resolved: list[ResolvedDatasetRun] = []
    for dataset in datasets:
        if dataset not in known_datasets:
            die(f"unknown dataset '{dataset}' (see: python3 bench/bench.py list datasets)")
        resolved.append(
            ResolvedDatasetRun(
                dataset=dataset,
                db=dataset_db_name(dataset),
                variants=(),
            )
        )
    return resolved


# SQL loading and statement shaping.
# This section turns manifest entries into the SQL sent to EXPLAIN ANALYZE.


def load_sql_for_query(query: QueryMeta) -> str:
    """Load the SQL text for a manifest query entry."""

    path = REPO_ROOT / query.query_path
    if not path.is_file():
        die(f"missing query file: {path}")
    return path.read_text(errors="ignore")


def strip_trailing_semicolon_and_comment(sql: str) -> str:
    """Remove a final semicolon and trailing SQL comment from a statement."""

    s = sql.strip()
    s = re.sub(r";\s*(--.*)?\s*\Z", "", s, flags=re.DOTALL)
    return s.strip()


def ensure_semicolon(sql: str) -> str:
    """Ensure a SQL statement ends with a semicolon."""

    s = sql.strip()
    if not s.endswith(";"):
        return s + ";"
    return s


def build_statement(dataset: str, sql: str) -> str:
    """Build the SQL text that the runner sends to EXPLAIN ANALYZE."""

    if dataset in PLANNING_DATASETS:
        inner = strip_trailing_semicolon_and_comment(sql)
        return f"SELECT count(*) FROM ({inner}) q;"
    return ensure_semicolon(sql)


# Prepare script mapping.
# This section maps logical dataset names to loader scripts.


def dataset_prepare_scripts(dataset: str) -> tuple[Path, Path, Optional[Path], bool]:
    """Return schema/load/index script paths and CSV requirement for preparation."""

    if dataset in IMDB_DATASETS:
        return (
            REPO_ROOT / "join-order-benchmark" / "schema.sql",
            REPO_ROOT / "join-order-benchmark" / "load.sql",
            REPO_ROOT / "join-order-benchmark" / "fkindexes.sql",
            True,
        )

    if dataset == "sqlite_select5":
        return (
            REPO_ROOT / "sqlite" / "schema.sql",
            REPO_ROOT / "sqlite" / "load.sql",
            None,
            False,
        )

    if dataset.startswith("gpuqo_"):
        name = dataset.removeprefix("gpuqo_").replace("_", "-")
        base = REPO_ROOT / "postgres-gpuqo" / "scripts" / "databases" / name
        return (base / "schema.sql", base / "load.sql", None, False)

    die(f"dataset '{dataset}' is not supported by prepare")
