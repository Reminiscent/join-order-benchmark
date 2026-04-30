# Reproduce

This document describes the public reproduction flow for this benchmark
repository.

## 1. Requirements

You need:

- a reachable PostgreSQL instance
- `psql` in `PATH`
- Python 3.11 or newer
- the external IMDB CSV bundle when preparing IMDB-backed datasets

The documented reproduction path is Python 3.11+ only.  No extra Python package
is required for TOML parsing on supported Python versions.

If you use a custom PostgreSQL build, expose its binaries first:

```bash
export PATH="$PG_BUILD_DIR/bin:$PATH"
```

Optional connection flags supported by every command:

- `--host`
- `--port`
- `--user`

## 2. Variants

The default variant file is [examples/variants.toml](examples/variants.toml).
It contains portable baselines plus other example algorithms used by this
repository's experiments.

Inspect the active variants:

```bash
python3 bench/bench.py list variants
```

Use a custom variant file when testing a different algorithm or parameter set:

```bash
python3 bench/bench.py list variants --variants-file path/to/variants.toml
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,geqo,my_algo
```

The built-in scenario defaults are the portable `dp` and `geqo` baselines.  Use
an explicit `--variants` list for any patch-specific algorithm.

```bash
python3 bench/bench.py run main --variants dp,geqo
```

Variant fields are documented in [config/README.md](config/README.md).

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
runs the full `job` and `job_complex` workloads.

Prepare:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

Run with portable baselines:

```bash
python3 bench/bench.py run main --variants dp,geqo
```

Run with an explicit variant set:

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
full dataset while limiting `dp` to `join_size <= 12`.

## 7. Output Layout

Every `run` creates:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
  public_report.md
  public_report.json
```

`run.json`

- minimal run context needed to explain the benchmark protocol and re-render the
  public report
- includes scenario name, protocol settings, resolved variants, resolved
  datasets, optional `tag`, warmup failures, and progress state
- intentionally does not snapshot host metadata, PostgreSQL version,
  cluster-level settings, or git state

`raw.csv`

- source-of-truth measurement log
- one row per `(dataset, query, variant, rep)`
- includes status, error details, phase timings, planner cost, and variant
  rotation position

`summary.csv`

- aggregated reporting view derived from `raw.csv`
- one row per `(dataset, query, variant)`
- aggregates successful measured repetitions only and carries `ok_reps` /
  `err_reps`

`public_report.md`

- auto-generated after every `run`
- public markdown report with separate execution and planning sections

`public_report.json`

- machine-readable form of the same public report

## 8. Measurement Semantics

Current default measurement protocol:

- planning and execution metrics come from
  `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)`
- `bench.py run` performs one discarded warmup pass per query group by default
  before measured repetitions for that same query group
- warmup executions are not recorded in `raw.csv` or `summary.csv`
- measured `statement_timeout` rows are recorded as `status=timeout`
- non-timeout errors are recorded as `status=error`

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

To re-render the default public report:

```bash
python3 tools/render_public_reports.py outputs/<run_id>
```

## 9. Session-Level vs Cluster-Level Settings

The harness applies and records session-level benchmark settings, such as:

- `statement_timeout`
- `join_collapse_limit`
- `max_parallel_workers_per_gather`
- `work_mem`
- `effective_cache_size`

The harness does not modify cluster-level settings such as:

- `shared_buffers`
- other restart-required PostgreSQL settings

Prepare cluster-level settings outside the benchmark harness.

## 10. Useful Overrides

The following overrides are available on `run`:

- `--variants-file`
- `--variants`
- `--resume-run-id`
- `--reps`
- `--statement-timeout-ms`
- `--stabilize`
- `--warmup-runs`
- `--skip-measured-after-warmup-timeout`
- `--no-skip-measured-after-warmup-timeout`
- `--tag`
- `--fail-on-error`

`--warmup-runs` means discarded warmup pass(es) per query group before that
query group's measured repetitions.  Use `--warmup-runs 0` to disable the
default warmup phase.

`--skip-measured-after-warmup-timeout` is enabled by default.  If an exact
`(dataset, query, variant)` hits `statement_timeout` during warmup, the harness
records later measured repetitions for that same combination as skipped timeout
rows instead of re-running them.  Use `--no-skip-measured-after-warmup-timeout`
to re-run measured repetitions even after a warmup timeout.

`--resume-run-id` resumes an existing `outputs/<run_id>/` directory from the
next unfinished safe group boundary.  The harness checkpoints after complete
warmup groups and complete measured groups, so resume never continues from the
middle of a query-group's variant set.

`--tag` labels the local build or patch under test, for example
`--tag pg18_patch_v4`.
