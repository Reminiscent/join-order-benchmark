# Join Order Benchmark Collection

This repository is a PostgreSQL-focused benchmark artifact repo for **join order optimization**.

It provides:

- a stable collection of join-heavy SQL workloads
- named algorithm variants mapped to benchmark parameters
- scenario-based preparation and execution
- self-contained benchmark outputs for review and comparison

## Quick Start

```bash
python3 bench/bench.py list scenarios
python3 bench/bench.py list variants
python3 bench/bench.py prepare smoke
python3 bench/bench.py run smoke --variants dp,geqo
```

For the full reproduction flow, see [REPRODUCE.md](REPRODUCE.md).

## Requirements

You need:

- Python 3.11 or newer
- `psql` in `PATH`
- a reachable PostgreSQL instance

The documented reproduction path is Python 3.11+ only. No extra Python package is required for TOML parsing on supported Python versions.

## Benchmark Interface

[bench/bench.py](bench/bench.py)

- main CLI for `list`, `prepare`, and `run`

[config/variants.toml](config/variants.toml)

- defines named algorithm variants and their session-level parameters

[config/scenarios.toml](config/scenarios.toml)

- defines benchmark scopes such as `smoke`, `main`, `full`, and `custom`

Built-in scenarios:

- `smoke`
  Fast self-contained validation run.
- `main`
  Default public reproduction path on the full `job` + `job_complex` workloads.
- `full`
  Full public workload across all datasets in this repository.
- `custom`
  User-selected datasets, filters, and variants.

Current built-in scenario restrictions:

- `smoke`
  Intentionally samples small slices only:
  `sqlite_select5` runs `join_size >= 20` with `max_queries = 2`;
  `gpuqo_chain_small` runs `join_size >= 10` with `max_queries = 2`;
  `gpuqo_star_small` runs `join_size >= 10` with `max_queries = 2`.
- `main`
  Runs the full `job` (113 queries) and full `job_complex` (30 queries) workloads with no join-size filter.
- `full`
  Runs all public datasets, with one explicit tractability guard on `gpuqo_clique_small`:
  `geqo` and `hybrid_search` run the full 150-query set, while `dp` is limited to `join_size <= 12`.
- `custom`
  This is the only built-in scenario intended for ad hoc `--min-join` / `--max-join` filtering.

Built-in variants currently include:

- `dp`
- `geqo`
- `hybrid_search`
- `goo_cost`
- `goo_result_size`
- `goo_selectivity`
- `goo_combined`

To inspect the exact current definitions:

```bash
python3 bench/bench.py list scenarios
python3 bench/bench.py list variants
```

## External Data

This repository does **not** vendor the IMDB CSV bundle used by IMDB-backed workloads.

Datasets that require the external IMDB CSV bundle:

- `job`
- `job_complex`
- `imdb_ceb_3k`

Recommended download source:

- CedarDB mirror: [https://bonsai.cedardb.com/job/imdb.tgz](https://bonsai.cedardb.com/job/imdb.tgz)

Historical reference:

- CWI JOB page: [https://event.cwi.nl/da/job/](https://event.cwi.nl/da/job/)

Example setup:

```bash
mkdir -p data/imdb_csv
tar -xzf imdb.tgz -C data/imdb_csv
```

Then prepare IMDB-backed scenarios with:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

## Workloads

### `job`

- Source: JOB (Leis et al., PVLDB 2015)
- Query count: 113
- Join size: 4 to 17
- Type: real IMDB workload
- Strength: classic optimizer baseline with realistic correlations

### `job_complex`

- Source: JOB-Complex
- Query count: 30
- Join size: 6 to 16
- Type: real IMDB workload
- Strength: harder predicates, non-PK/FK joins, and more challenging join-order choices

### `imdb_ceb_3k`

- Source: CEB IMDB 3k subset
- Query count: 3,133
- Join size: 6 to 16
- Type: real IMDB workload
- Strength: large query volume for broader coverage and heavier campaigns

### `sqlite_select5`

- Source: converted SQLite `select5`
- Query count: 732
- Join size: 4 to 64
- Type: self-contained synthetic-style stress workload
- Strength: high-width join stress without any external data files

### `gpuqo_chain_small`

- Query count: 150
- Join size: 2 to 16
- Type: self-contained synthetic workload
- Strength: chain-shaped join graphs

### `gpuqo_clique_small`

- Query count: 150
- Join size: 2 to 16
- Type: self-contained synthetic workload
- Strength: dense clique-shaped join graphs

### `gpuqo_star_small`

- Query count: 150
- Join size: 2 to 16
- Type: self-contained synthetic workload
- Strength: star-shaped join graphs

### `gpuqo_snowflake_small`

- Query count: 390
- Join size: 2 to 40
- Type: self-contained synthetic workload
- Strength: larger snowflake-shaped join graphs with much wider joins than JOB-family workloads

The global query manifest is [meta/query_manifest.csv](meta/query_manifest.csv), and you can refresh it with:

```bash
python3 tools/build_query_manifest.py --verify --summary
```

## Current Measurement Protocol

Current default runs collect:

- planning and execution metrics from `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)`
- one full discarded workload warmup pass by default (`--warmup-runs 1`) before the measured repetitions configured by the selected scenario

The warmup pass executes the full `(dataset, query, variant)` matrix once and is not recorded in `raw.csv` or `summary.csv`.

As a result, `raw.csv` records `planning_ms`, `total_ms`, `execution_ms`, and `plan_total_cost`, while `public_report.md` reports planning and execution in separate sections.

`smoke` is intentionally different: it is a terminal-only sanity check and does not write any result files.

## Outputs

Every non-`smoke` `run` writes a self-contained result directory under `outputs/<run_id>/`:

- `run.json`
  Minimal run context needed to explain the benchmark protocol and re-render the public report.
- `raw.csv`
  Per-query, per-repetition raw benchmark records.
- `summary.csv`
  Aggregated results for comparison and reporting.
- `public_report.md`
  Auto-generated public-facing markdown report with separate execution and planning sections.
- `public_report.json`
  Machine-readable form of the same public report.

Result-file semantics:

- `raw.csv`
  Source-of-truth measurement log. Each row is one `(dataset, query, variant, rep)` execution attempt, including `status`, `error`, `planning_ms`, `total_ms`, `execution_ms`, and the variant rotation position used in that repetition.
- `summary.csv`
  One row per `(dataset, query, variant)`, derived from `raw.csv` by aggregating successful measured repetitions only. It is the compact reporting view, not the raw measurement log.
- `run.json`
  Minimal benchmark context containing:
  - `run_id`
  - `scenario`
  - `protocol` with `reps`, `statement_timeout_ms`, `stabilize`, `warmup_runs`, `warmup_scope`, `measurement_lane`, and scenario-level `session_gucs`
  - resolved `variants` with only the variant-level GUCs that actually applied on this PostgreSQL build
  - resolved `datasets` with dataset filters and selected variants
  - optional local `tag` only when explicitly provided

`run.json` is intentionally scoped to the benchmark inputs needed to explain the run and re-render the public report.
It does not try to snapshot host metadata, PostgreSQL version, cluster-level settings, or git state.

Which result columns to cite:

- `execution_ms_median`
  Preferred primary public column for execution-focused replacement comparisons. This is the per-query median execution component across successful measured reps, sourced from `Execution Time` reported by the measured `EXPLAIN ANALYZE` lane.
- `total_ms_median`
  Preferred column when you want the typical directly observed combined planning-plus-execution time from a single measured repetition.
- `planning_ms_median`
  Median planning time across successful measured reps. Use this for planner-overhead discussion and a separate planning report, not for end-to-end latency claims.
- `plan_total_cost_median`
  Median planner cost across successful measured reps. Treat this as a diagnostic planner-side signal, not as a runtime surrogate for headline comparisons.

Default public-report behavior:

- After every non-`smoke` `bench.py run`, the harness automatically writes `public_report.md` and `public_report.json` into `outputs/<run_id>/`.
- The execution section is the primary decision surface for whether an algorithm can replace the reference variant, and it is based on per-query medians rather than per-query minima.
- The planning section is reported separately as a diagnostic so planner overhead does not get conflated with execution behavior.
- The report explicitly states that these metrics come from PostgreSQL backend phase times under the measured `EXPLAIN ANALYZE` lane; they are not client end-to-end latencies.
- Query-level ratios use a small additive floor to stabilize near-zero timings, workload totals sum per-query aggregated metric values on comparable queries only, and `p99` is only shown when the selected dataset is large enough for it to be informative.

To re-render the default report for an existing run directory:

```bash
python3 tools/render_public_reports.py outputs/<run_id>
```

The default report includes all datasets and selected variants from the run, with per-dataset sections and `dp` as the reference when it is present.

## Repository Layout

```text
.
├── README.md
├── REPRODUCE.md
├── .gitignore
├── bench/
├── config/
├── meta/
├── tools/
├── join-order-benchmark/
├── JOB-Complex/
├── imdb_pg_dataset/
├── sqlite/
├── postgres-gpuqo/
├── outputs/             # runtime outputs, gitignored
└── data/                # local, gitignored
```

### Directory Summary

[bench](bench)

- benchmark runner implementation
- includes the CLI entry plus support modules for config loading, prepare, and run execution

[config](config)

- checked-in scenario and variant definitions
- see `config/README.md` for the configuration file fields

[meta](meta)

- tracked derived metadata consumed by the runner

[tools](tools)

- maintenance scripts for refreshing tracked metadata

[join-order-benchmark](join-order-benchmark)

- JOB schema, load script, indexes, and query files

[JOB-Complex](JOB-Complex)

- JOB-Complex schema, load script, and query files

[imdb_pg_dataset](imdb_pg_dataset)

- IMDB-derived suites sharing the IMDB schema/load path

[sqlite](sqlite)

- self-contained converted SQLite workload

[postgres-gpuqo](postgres-gpuqo)

- synthetic workload subsets with controlled join graph shapes

[outputs](outputs)

- runtime benchmark outputs
- automatically generated by non-`smoke` `bench.py run`
- intentionally not tracked by git

[data](data)

- local external data only
- intentionally not tracked by git

## Notes

- Cluster-level settings such as `shared_buffers` are **not** changed by the harness.
- Session-level benchmark settings are applied by the harness and recorded in `run.json`.
- Use `--tag` to label the local build or patch under test when you want to distinguish multiple result directories, for example `--tag pg18_patch_v4`.
