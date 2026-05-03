from __future__ import annotations

import csv
import functools
import re
from pathlib import Path
from typing import Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

from bench_common import (
    DatasetSpec,
    MANIFEST_PATH,
    QueryMeta,
    REPO_ROOT,
    ResolvedDatasetRun,
    Scenario,
    Variant,
    die,
    parse_csv_list,
)


WRAP_COUNT_DATASETS = {
    "sqlite_select5",
    "gpuqo_chain_small",
    "gpuqo_clique_small",
    "gpuqo_star_small",
    "gpuqo_snowflake_small",
}

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

PREPARE_MARKERS = {
    "job": ("title", "aka_name"),
    "job_complex": ("title", "aka_name"),
    "imdb_ceb_3k": ("title", "aka_name"),
    "sqlite_select5": ("t1", "t64"),
    "gpuqo_chain_small": ("t1", "t40"),
    "gpuqo_clique_small": ("t1", "t40"),
    "gpuqo_star_small": ("t0", "t39"),
    "gpuqo_snowflake_small": ("t_1", "t_1_16"),
}

SELECT5_HEADER_RE = re.compile(r"^--\s*query\s+(\d+)\s+\((.*?)\)\s*$", flags=re.IGNORECASE)
DEFAULT_VARIANTS_FILE = REPO_ROOT / "examples" / "variants.toml"

DEFAULT_SCENARIO_VARIANTS = ("dp", "geqo")
DEFAULT_STATEMENT_TIMEOUT_MS = 600000
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
        statement_timeout_ms=DEFAULT_STATEMENT_TIMEOUT_MS,
        session_gucs=DEFAULT_SESSION_GUCS,
        datasets=datasets,
    )


def resolve_variants_file(path: Optional[Path] = None) -> Optional[Path]:
    if path is not None:
        return Path(path)
    if DEFAULT_VARIANTS_FILE.is_file():
        return DEFAULT_VARIANTS_FILE
    return None


def load_variants(path: Optional[Path] = None) -> dict[str, Variant]:
    out = {variant.name: variant for variant in BUILT_IN_VARIANTS}
    variants_path = resolve_variants_file(path)
    if variants_path is None:
        return out

    variants_path = Path(variants_path)
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
    print("Scenarios")
    print("name\tdatasets\tdescription")
    for name, scenario in scenarios.items():
        dataset_names = list(dict.fromkeys(spec.dataset for spec in scenario.datasets))
        datasets = ", ".join(dataset_names)
        print(f"{name}\t{datasets}\t{scenario.description}")
    print()


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def print_variants(
    variants: dict[str, Variant],
    variants_file: Optional[Path] = None,
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


@functools.lru_cache(maxsize=1)
def load_manifest_by_dataset() -> dict[str, tuple[QueryMeta, ...]]:
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
                query_label=row.get("query_label", "") or "",
                join_size=join_size,
            )
            out.setdefault(q.dataset, []).append(q)

    finalized: dict[str, tuple[QueryMeta, ...]] = {}
    for dataset, queries in out.items():
        queries.sort(key=lambda q: (q.query_id, q.query_path))
        finalized[dataset] = tuple(queries)
    return finalized


def available_datasets() -> tuple[str, ...]:
    return tuple(sorted(load_manifest_by_dataset().keys()))


def parse_manifest(dataset: str) -> list[QueryMeta]:
    manifest = load_manifest_by_dataset()
    if dataset not in manifest:
        die(f"unknown dataset '{dataset}'")
    return list(manifest[dataset])


@functools.lru_cache(maxsize=1)
def parse_select5_queries() -> dict[str, str]:
    sql_path = REPO_ROOT / "sqlite" / "queries" / "select5.sql"
    queries: dict[str, str] = {}
    cur_id: Optional[str] = None
    cur_lines: list[str] = []

    def flush() -> None:
        nonlocal cur_id, cur_lines
        if cur_id is None:
            return
        sql = "\n".join(cur_lines).strip()
        if not sql:
            die(f"empty SQL in {sql_path} for query {cur_id}")
        queries[cur_id] = sql
        cur_id = None
        cur_lines = []

    for raw in sql_path.read_text(errors="ignore").splitlines():
        m = SELECT5_HEADER_RE.match(raw.strip())
        if m:
            flush()
            cur_id = m.group(1).zfill(4)
            continue
        if cur_id is None:
            continue
        if raw.lstrip().startswith("--"):
            continue
        cur_lines.append(raw)
        if "\n".join(cur_lines).strip().endswith(";"):
            flush()

    flush()
    return queries


def strip_trailing_semicolon_and_comment(sql: str) -> str:
    s = sql.strip()
    s = re.sub(r";\s*(--.*)?\s*\Z", "", s, flags=re.DOTALL)
    return s.strip()


def ensure_semicolon(sql: str) -> str:
    s = sql.strip()
    if not s.endswith(";"):
        return s + ";"
    return s


def build_statement(dataset: str, sql: str) -> str:
    if dataset in WRAP_COUNT_DATASETS:
        inner = strip_trailing_semicolon_and_comment(sql)
        return f"SELECT count(*) FROM ({inner}) q;"
    return ensure_semicolon(sql)


def dataset_db_name(dataset: str) -> str:
    if dataset not in DEFAULT_DB_BY_DATASET:
        die(f"no default benchmark database configured for dataset '{dataset}'")
    return DEFAULT_DB_BY_DATASET[dataset]


def dataset_prepare_scripts(dataset: str) -> tuple[Path, Path, Optional[Path], bool]:
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


def select_queries(spec: ResolvedDatasetRun) -> list[QueryMeta]:
    queries = parse_manifest(spec.dataset)
    if spec.max_join is not None:
        queries = [q for q in queries if q.join_size <= spec.max_join]
    if not queries:
        die(f"no queries selected (dataset={spec.dataset}, max_join={spec.max_join})")
    return queries


def load_sql_for_query(query: QueryMeta) -> str:
    if query.dataset == "sqlite_select5":
        sql = parse_select5_queries().get(query.query_id)
        if sql is None:
            die(f"missing sqlite_select5 query_id={query.query_id} in sqlite/queries/select5.sql")
        return sql

    path = REPO_ROOT / query.query_path
    if not path.is_file():
        die(f"missing query file: {path}")
    return path.read_text(errors="ignore")
