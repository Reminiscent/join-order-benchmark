# Reproduce

This document describes the current public reproduction flow for this repository.

## 1. Requirements

You need:

- a reachable PostgreSQL instance
- `psql` in `PATH`
- Python 3.11 or newer
- the external IMDB CSV bundle only when preparing scenarios that include IMDB-backed datasets

The documented reproduction path is Python 3.11+ only. No extra Python package is required for TOML parsing on supported Python versions.

If you use a custom PostgreSQL build, expose its binaries first:

```bash
export PATH="$PG_BUILD_DIR/bin:$PATH"
```

Optional connection flags supported by every command:

- `--host`
- `--port`
- `--user`

## 2. Variants

Variants are defined in `config/variants.toml`.

To inspect them:

```bash
python3 bench/bench.py list variants
```

To run a scenario with an explicit variant set:

```bash
python3 bench/bench.py run smoke --variants dp,geqo
```

If the implementation under test changes variant parameters, update `config/variants.toml`.

## 3. External IMDB CSV Data

The following datasets require the external IMDB CSV bundle:

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

The extracted directory should contain the 21 CSV files used by the IMDB schema load scripts.

## 4. Discover What Exists

List the built-in scenarios:

```bash
python3 bench/bench.py list scenarios
```

List the built-in variants:

```bash
python3 bench/bench.py list variants
```

List the available datasets:

```bash
python3 bench/bench.py list datasets
```

## 5. Smoke Run

`smoke` is the fastest end-to-end path and does not require IMDB CSV files.
It intentionally samples only a few wider queries instead of running whole datasets.
It is a terminal-only sanity check and does not create an `outputs/<run_id>/` result directory.

If your PostgreSQL build exposes the custom benchmark GUCs, you can run the default smoke scenario:

```bash
python3 bench/bench.py prepare smoke
python3 bench/bench.py run smoke
```

If you are using a stock PostgreSQL build, run smoke with portable variants:

```bash
python3 bench/bench.py prepare smoke
python3 bench/bench.py run smoke --variants dp,geqo
```

## 6. Main Public Reproduction

`main` is the default public reproduction path and runs the full workloads for:

- `job`
- `job_complex`

These workloads share one IMDB-backed benchmark database.
There is no built-in join-size filter in `main`; it covers all 113 `job` queries and all 30 `job_complex` queries.

Prepare:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

Run with the scenario defaults:

```bash
python3 bench/bench.py run main
```

Run with an explicit variant set:

```bash
python3 bench/bench.py run main --variants dp,geqo,hybrid_search
```

## 7. Full Run

`full` runs the full public workload in this repository, including the heavier `imdb_ceb_3k` suite.
The one intentional exception is `gpuqo_clique_small`, where `dp` is capped at `join_size <= 12` while `geqo` and `hybrid_search` still run the full dataset.

Prepare:

```bash
python3 bench/bench.py prepare full --csv-dir "$(pwd)/data/imdb_csv"
```

Run:

```bash
python3 bench/bench.py run full
```

## 8. Custom Run

Use `custom` when you want to choose the datasets, join filters, or variants yourself.

Prepare a custom dataset set:

```bash
python3 bench/bench.py prepare custom --datasets job,job_complex --csv-dir "$(pwd)/data/imdb_csv"
```

Run a custom experiment:

```bash
python3 bench/bench.py run custom \
  --datasets job,job_complex \
  --min-join 12 \
  --variants dp,geqo,hybrid_search
```

Additional custom filters:

- `--max-join`

## 9. Output Layout

Every non-`smoke` run creates:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
  public_report.md
  public_report.json
```

## 10. What Each Output File Means

Current default measurement protocol:

- planning and execution metrics come from `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)`
- `bench.py run` performs one full discarded workload warmup pass by default (`--warmup-runs 1`) before the measured repetitions configured by the selected scenario
- the warmup pass executes the full `(dataset, query, variant)` matrix once and is not recorded in `raw.csv` or `summary.csv`

`run.json`

- minimal run context needed to explain the benchmark protocol and re-render the public report
- includes:
  - `run_id`
  - `scenario`
  - `protocol` with `reps`, `statement_timeout_ms`, `stabilize`, `warmup_runs`, `warmup_scope`, `measurement_lane`, and scenario-level `session_gucs`
  - resolved `variants` with only the variant-level GUCs that actually applied on this PostgreSQL build
  - resolved `datasets` with dataset filters and selected variants
  - optional local `tag` only when explicitly provided
- it intentionally does not snapshot host metadata, PostgreSQL version, cluster-level settings, or git state

`raw.csv`

- source-of-truth measurement log
- one row per `(dataset, query, variant, rep)`
- includes status, error details, and the directly recorded timing fields for that repetition
- use this when you need to inspect individual repetitions, failures, or run-order effects

`summary.csv`

- aggregated reporting view derived from `raw.csv`
- one row per `(dataset, query, variant)`
- aggregates successful measured reps only and carries `ok_reps` / `err_reps`
- use this for per-query comparison tables, workload totals, and public reports

`public_report.md`

- auto-generated after every normal `run`
- human-readable public report with separate execution and planning sections
- includes coverage, ratio summaries, tail counts, workload totals, and worst-query regressions

`public_report.json`

- machine-readable form of the same public report
- intended for downstream tooling or custom post-processing

Recommended result columns:

- `execution_ms_median`
  Preferred primary column for execution-focused comparisons such as `algo / DP` public ratio tables. It is the per-query median `Execution Time` from the measured `EXPLAIN ANALYZE` lane.
- `total_ms_median`
  Typical directly observed combined planning-plus-execution time from a single successful measured repetition.
- `planning_ms_median`
  Median planning overhead across successful measured repetitions. This should be reported separately from execution.
- `plan_total_cost_median`
  Median planner cost across successful measured repetitions. Use it as a planner-side diagnostic only.

Default public-report behavior:

- every non-`smoke` `bench.py run` automatically writes `public_report.md` and `public_report.json` into `outputs/<run_id>/`.
- The execution section is the primary replacement view and is based on per-query medians rather than minima.
- The planning section is a separate diagnostic view.
- The report states that these metrics are PostgreSQL backend phase times from the measured `EXPLAIN ANALYZE` lane, not client end-to-end latencies.
- Workload totals sum per-query aggregated metric values on comparable queries only, and `p99` is omitted for small suites where it would collapse to the max or near-max query.

To re-render the default public report for an existing run:

```bash
python3 tools/render_public_reports.py outputs/<run_id>
```

## 11. Session-Level vs Cluster-Level Settings

The harness applies and records session-level benchmark settings, such as:

- `statement_timeout`
- `join_collapse_limit`
- `max_parallel_workers_per_gather`
- `work_mem`
- `effective_cache_size`

The harness does **not** modify cluster-level settings such as:

- `shared_buffers`
- other restart-required PostgreSQL settings

Those should be prepared by the user outside the benchmark harness.

## 12. Adding a New Variant

Add a new `[[variant]]` entry in `config/variants.toml`.

Example:

```toml
[[variant]]
name = "my_new_algo"
label = "My New Algorithm"
session_gucs = { geqo_threshold = 2, enable_goo_join_search = "on", goo_greedy_strategy = "combined" }
```

If a variant needs a patch-specific cleanup GUC but should still remain runnable on stock PostgreSQL,
add it under `optional_session_gucs` instead of `session_gucs`.

Then run:

```bash
python3 bench/bench.py run main --variants dp,geqo,my_new_algo
```

## 13. Useful Overrides

The following overrides are available on `run`:

- `--resume-run-id`
- `--reps`
- `--statement-timeout-ms`
- `--stabilize`
- `--warmup-runs`
- `--skip-measured-after-warmup-timeout`
- `--no-skip-measured-after-warmup-timeout`
- `--tag`
- `--fail-on-error`

`--warmup-runs` now means discarded full-workload passes before measured repetitions. Use `--warmup-runs 0` if you want to disable the default warmup phase.

`--skip-measured-after-warmup-timeout` is enabled by default. If an exact `(dataset, query, variant)` hits `statement_timeout` during warmup, the harness records the later measured repetitions for that same combination as skipped timeout rows instead of re-running them. This saves time, but it is a conservative shortcut rather than proof that the measured phase would have timed out identically. Use `--no-skip-measured-after-warmup-timeout` to restore the older behavior.

`--resume-run-id` resumes an existing `outputs/<run_id>/` directory from the next unfinished safe group boundary. The harness only checkpoints after complete warmup groups `(warmup_pass, dataset, query_id)` and complete measured groups `(dataset, query_id, rep)`, so resume never continues from the middle of a query-group's variant set. This keeps per-query cross-variant comparisons aligned.

`--tag` is intended for the local build or patch label under test, for example `--tag pg18_patch_v4`.
These overrides are recorded in `run.json`.
