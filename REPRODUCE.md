# Reproduce

This document describes the public reproduction flow for this benchmark
repository.

For the runner internals behind these commands, including stabilization,
warmup, variant rotation, and timing collection, see
[BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).

## 1. Requirements

You need:

- a reachable PostgreSQL instance
- `psql` in `PATH`
- Python 3.11 or newer
- the external IMDB CSV bundle when preparing IMDB-backed datasets
- a database role that can connect to the `postgres` maintenance database and
  create the benchmark databases used by `prepare`

The documented reproduction path is Python 3.11+ only.  No extra Python package
is required for TOML parsing on supported Python versions.

Following the public `shared_buffers=4GB` setup also requires either superuser
access for `ALTER SYSTEM` or direct access to the PostgreSQL server
configuration, followed by a server restart.

If you use a custom PostgreSQL build, expose its binaries first:

```bash
export PATH="$PG_BUILD_DIR/bin:$PATH"
```

Optional connection flags supported by every command:

- `--host`
- `--port`
- `--user`

## 2. Variants

The portable baseline variants `dp` and `geqo` are built in.  Extra variants are
only needed when testing a patch-specific algorithm or parameter set.

Inspect the active variants:

```bash
python3 bench/bench.py list variants
```

Inspect built-ins plus extra example variants:

```bash
python3 bench/bench.py list variants --variants-file examples/variants.toml
```

Use an extra variant file when testing a different algorithm or parameter set:

```bash
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,geqo,my_algo
```

If `--variants` is omitted, the selected scenario uses the built-in `dp` and
`geqo` baselines.  Variant fields are documented in
[examples/README.md](examples/README.md).

## 3. Data Setup

Dataset details and IMDB CSV setup are documented in [DATASETS.md](DATASETS.md).

Typical IMDB CSV setup:

```bash
mkdir -p data/imdb_csv
tar -xzf imdb.tgz -C data/imdb_csv
```

## 4. Discover What Exists

List scenarios:

```bash
python3 bench/bench.py list scenarios
```

List datasets:

```bash
python3 bench/bench.py list datasets
```

List variants:

```bash
python3 bench/bench.py list variants
```

## 5. Main Run

`main` is the primary public validation path for a new join-order algorithm.  It
runs the complete `job` and `job_complex` workloads.

Prepare:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

Run with portable baselines:

```bash
python3 bench/bench.py run main --variants dp,geqo
```

Run with an extra variant set:

```bash
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,geqo,my_algo
```

## 6. Extended And Full Runs

`extended` is the broad validation layer after `main`.  It adds self-contained
stress workloads converted from SQLite and GPUQO sources.  These workloads have
small local data and many wide joins, so they are most useful for planning time
and join-search stress rather than realistic execution-time claims.

Prepare and run:

```bash
python3 bench/bench.py prepare extended --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run extended
```

`full` is the complete built-in campaign.  It runs `extended` plus
`imdb_ceb_3k`, which has much higher query volume and can dominate campaign
time.

Prepare and run:

```bash
python3 bench/bench.py prepare full --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run full
```

In `extended` and `full`, `gpuqo_clique_small` keeps non-`dp` variants on the
complete 150-query dataset while limiting `dp` to `join_size <= 12`.

## 7. Output Layout

Every `run` creates local artifacts under `outputs/<run_id>/`:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
```

The detailed artifact contract, column meanings, console output, and
reviewer-table examples are documented in [OUTPUTS.md](OUTPUTS.md).

## 8. Reviewer Tables

Use `tools/render_review_tables.py` to create per-query tables for community
attachments from an existing `outputs/<run_id>/` directory.  The script writes a
styled XLSX workbook and CSV companion files.

Default table export:

```bash
python3 tools/render_review_tables.py outputs/<run_id> \
  --dataset job \
  --variants dp,geqo,my_algo
```

The workbook contains one execution-time sheet and one planning-time sheet.
Execution time is the primary result; planning time is reported separately as a
diagnostic.

The exact workbook layout, CSV headers, `SUM` row semantics, and ratio color
rules are documented in [OUTPUTS.md](OUTPUTS.md).

## 9. Measurement Semantics

Current fixed measurement protocol:

- planning and execution metrics come from
  `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)`
- `bench.py run` performs one discarded warmup pass per query group by default
  before measured repetitions for that same query group
- warmup executions are not recorded in `raw.csv` or `summary.csv`
- measured `statement_timeout` rows are recorded as `status=timeout`
- non-timeout errors are recorded as `status=error`

The reason for using `EXPLAIN ANALYZE` JSON, and the tradeoff around
`TIMING OFF`, are documented in [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).

Recommended result columns:

- `execution_ms_median`
  Primary execution comparison column for public ratio tables.
- `total_ms_median`
  Typical directly observed planning-plus-execution time for a successful
  measured repetition.
- `planning_ms_median`
  Planner overhead, reported separately from execution.
- `plan_total_cost_median`
  Planner-side diagnostic signal, not a runtime substitute.

## 10. Fixed Run Protocol And PostgreSQL Settings

The selected scenario fixes the public benchmark protocol.  These values are
part of how the test is done; they are not ordinary CLI parameters:

- `3` measured repetitions per query and variant
- one discarded warmup pass per query group
- `vacuum_freeze_analyze` stabilization before measurement
- rotated variant order across query groups and repetitions
- skipped measured rows after a warmup `statement_timeout` for the same
  `(dataset, query, variant)`

PostgreSQL settings are split into session-level GUCs and restart-required
cluster settings.

The harness applies the following session-level PostgreSQL settings and records
them in `run.json`, either as protocol fields or as session GUCs:

- `statement_timeout`
- `join_collapse_limit`
- `max_parallel_workers_per_gather`
- `work_mem`
- `effective_cache_size`

`statement_timeout` is implemented as a session GUC, but it is also part of the
run protocol because it determines timeout rows in `raw.csv` and `summary.csv`.

The harness does not modify cluster-level settings such as:

- `shared_buffers`
- other restart-required PostgreSQL settings

For the public setup, set `shared_buffers` before the benchmark and restart
PostgreSQL:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

The memory-related session settings `work_mem=1GB` and
`effective_cache_size=8GB` are applied by the harness.  `effective_cache_size`
is a planner costing assumption, not memory allocated by PostgreSQL.

The same split is summarized in [README.md](README.md), and the memory-setting
rationale is documented in [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).

## 11. CLI Options

These are the supported `run` options after choosing a scenario.  They select
what to compare, where to resume, and how to label the local run.  They do not
change the fixed benchmark protocol, except for the per-statement timeout
guardrail.

| Option | Use |
| --- | --- |
| `--variants-file` | add patch-specific variants from a TOML file |
| `--variants` | choose the variants and display/order them explicitly |
| `--resume-run-id` | continue an interrupted run from a safe boundary |
| `--statement-timeout-ms` | adjust the guardrail timeout for very slow or very fast machines |
| `--tag` | record a local build or patch label in `run.json` |
| `--fail-on-error` | exit non-zero on non-timeout query errors |

`--statement-timeout-ms` is not an algorithm knob.  It only limits how long a
single bad plan can occupy the run.  If it differs from the default
`600000 ms`, include that value with the published result tables; the run also
records it in `run.json`.

If an exact `(dataset, query, variant)` hits `statement_timeout` during warmup,
the harness records later measured repetitions for that same combination as
skipped timeout rows instead of re-running them.

`--resume-run-id` resumes an existing `outputs/<run_id>/` directory from the
next unfinished safe group boundary.  The harness checkpoints after complete
warmup groups and complete measured groups, so resume never continues from the
middle of a query-group's variant set.

`--tag` labels the local build or patch under test, for example
`--tag pg18_patch_v4`.
