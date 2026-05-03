# Benchmark Runs

This document explains what the benchmark scripts do during a public run.  Use
[REPRODUCE.md](REPRODUCE.md) for the end-to-end reproduction checklist and
[OUTPUTS.md](OUTPUTS.md) for artifact formats.

## CLI Overview

The benchmark CLI is [bench/bench.py](bench/bench.py).  Use `list` commands to
inspect the built-in benchmark surface before preparing data or running
measurements:

```bash
python3 bench/bench.py list scenarios
python3 bench/bench.py list datasets
python3 bench/bench.py list variants
```

`list scenarios` shows workload groups such as `main`, `extended`, and `full`.
`list datasets` shows individual query/data sources and their target database
names.  `list variants` shows built-in variants plus
`examples/variants.toml` when that default file exists; pass `--variants-file`
only to use a different extra variant file.  Edit `examples/variants.toml`
directly to change the repository's default extra variants.  The `prepare` and
`run` subcommands are described in the following phases.

## Prepare Phase

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

`bench.py prepare <scenario>` resolves the datasets in the built-in scenario,
creates the target PostgreSQL databases, and loads each dataset's schema and
data.  In the example above, `main` selects the `job` and `job_complex`
workloads, both backed by the same `imdb_bench` database, so the IMDB data is
loaded once and reused by both query suites.

IMDB-backed datasets require `--csv-dir` because the CSV bundle is not vendored
in this repository.  Self-contained datasets, including SQLite select5 and GPUQO
small workloads, load local SQL files.  Scenario and dataset coverage is
documented in [WORKLOADS.md](WORKLOADS.md).

If the target database already exists and looks prepared, `prepare` skips it.
Use `--force-recreate` only when intentionally dropping and rebuilding an
existing benchmark database.

## Run Phase

Run with portable baselines:

```bash
python3 bench/bench.py run main --variants dp,geqo
```

Use the default extra variants file when comparing a patch-specific algorithm:

```bash
python3 bench/bench.py run main --variants dp,geqo,goo_cost
```

`bench.py run <scenario>` performs these steps:

1. Resolve the selected built-in scenario.  For example, `main` selects the
   complete `job` and `job_complex` query suites.
2. Load built-in variants plus the default `examples/variants.toml` entries
   when that file exists, unless `--variants-file` points to a different TOML
   file.  To adjust the default extra variant definitions, edit
   `examples/variants.toml` directly.  Then choose the explicit `--variants`
   list or the scenario's default `dp,geqo` list.
3. Build concrete dataset runs, including any scenario-defined limits.  For
   example, `extended` and `full` run non-`dp` variants on the complete
   `gpuqo_clique_small` workload, while limiting `dp` to queries with at most
   12 joins.
4. Check that benchmark databases are reachable and required GUCs exist.
5. For each prepared database in a fresh run, refresh statistics with
   `VACUUM FREEZE ANALYZE` and issue a best-effort `CHECKPOINT` before any
   query runs.
6. For each selected query, run discarded warmup pass(es), then measured
   repetitions.
7. Flush `run.json`, `raw.csv`, and `summary.csv` after safe progress
   boundaries.

The runner checkpoints progress after complete warmup groups and complete
measured groups, so `--resume-run-id` resumes from a safe group boundary rather
than from the middle of a query's variant set.

`bench_run.py` is the execution driver behind `bench.py run`.  It checks that
the prepared databases are reachable and that required GUCs exist, resolves the
full query list up front, writes an initial `run.json`, then loops by dataset,
query, warmup pass, repetition, and variant.  After each completed warmup group
or measured group it rewrites `run.json`, `raw.csv`, and `summary.csv`, which
keeps resume state recoverable without making partially executed variant sets
look complete.

## Public Run Protocol

The public benchmark protocol uses these values during `bench.py run`:

- 3 measured repetitions are collected for each selected query and variant.
- 1 warmup pass is run for each query group before measured repetitions.
- Each prepared database has its table statistics refreshed with
  `VACUUM FREEZE ANALYZE` and a best-effort `CHECKPOINT` before measurement for
  a new run.
- `--resume-run-id` preserves the existing database statistics snapshot instead
  of refreshing statistics for the resumed portion.
- Variant order is rotated across query groups and repetitions.
- If a warmup execution hits `statement_timeout`, later measured repetitions
  for the same `(dataset, query, variant)` are recorded as skipped timeout rows
  instead of re-running the same timeout-prone statement.
- Non-timeout warmup or measured errors terminate the run after writing the
  current artifacts.

The run command accepts `--statement-timeout-ms` for the guardrail timeout.
Measured repetitions, warmup count, statistics-refresh behavior, and variant-order
policy are not run options.

## Cluster Memory Baseline

The public runs are designed to work on a modest benchmark machine rather than
requiring a large-memory lab server.  The memory baseline is 16 GiB RAM or more.
That lower bound leaves room for a 4 GiB PostgreSQL buffer pool, operating-system
page cache, backend memory, and occasional large hash/sort operations while
keeping the setup accessible.

Most PostgreSQL settings used by the harness are session-level GUCs, but
`shared_buffers` is a cluster-level setting and must be configured before the
benchmark:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

Restart PostgreSQL after changing `shared_buffers`.  The goal is not to claim
that `4GB` is a universal PostgreSQL recommendation; it fixes a reproducible
buffer-pool baseline for the submitted public runs.  With the 16 GiB minimum,
`shared_buffers=4GB` is about 25% of RAM, which matches PostgreSQL's documented
starting point for a dedicated database server with at least 1 GiB of RAM.

The paired session settings `work_mem=1GB` and `effective_cache_size=8GB` are
chosen for the same 16 GiB baseline:

- `work_mem=1GB` reduces spill noise in these single-query, serial benchmark
  runs.  It is intentionally high for a normal multi-user server; the harness
  executes one measured query at a time and sets
  `max_parallel_workers_per_gather=0`, so this is a controlled benchmark setting
  rather than a production default.
- `effective_cache_size=8GB` tells the planner to assume roughly half of the
  16 GiB host can act as effective cache across PostgreSQL shared buffers and
  the operating-system page cache.  It is only a planner costing assumption; it
  does not allocate PostgreSQL memory.

References:

- PostgreSQL resource configuration:
  <https://www.postgresql.org/docs/current/runtime-config-resource.html>
- PostgreSQL query-planning configuration:
  <https://www.postgresql.org/docs/current/runtime-config-query.html>

## Session Setup

Each warmup and measured execution starts with a fresh session prelude.  This
is where the harness applies the session-level PostgreSQL settings used by the
test.  With the default timeout it is:

```sql
RESET ALL;
SET statement_timeout = 600000;
SET join_collapse_limit = 100;
SET max_parallel_workers_per_gather = 0;
SET work_mem = '1GB';
SET effective_cache_size = '8GB';
-- followed by variant-specific SET commands
```

Variant settings are applied after scenario settings, so the variant can choose
the intended algorithm.  Optional variant GUCs are applied only when the current
PostgreSQL build exposes that GUC.

The harness does not change restart-required cluster settings.  Those must be
configured outside the benchmark if a submitted run depends on them.

The run command accepts `--statement-timeout-ms` to adjust the guardrail timeout.
It exists to stop very bad plans from occupying the benchmark for too long; it
is not intended to tune algorithm quality.  If the default `600000 ms` is
changed because of machine speed or campaign time budget, record the override
with the published results.  The resolved value is written to `run.json`.

## Statistics Refresh

Fresh public runs refresh each prepared database's table statistics by
executing:

```sql
VACUUM FREEZE ANALYZE;
CHECKPOINT;
```

`VACUUM FREEZE ANALYZE` refreshes table statistics and marks eligible tuples as
frozen before measurement.  `CHECKPOINT` is best-effort; the benchmark continues
if it is not allowed for the current connection.

## Warmup And Measurement Order

The public run protocol uses one discarded warmup pass per query group:

```text
query 1: warm up all selected variants, then measure rep 1..N
query 2: warm up all selected variants, then measure rep 1..N
...
```

Warmup executions are not written to `raw.csv` or `summary.csv`.

The run protocol rotates variant order across query groups and repetitions.
This avoids always measuring the same variant first or last for every query.

If a warmup execution hits `statement_timeout`, later measured repetitions for
the exact `(dataset, query, variant)` tuple are recorded as skipped timeout rows
instead of re-running the same timeout-prone statement.

## Timing Collection

The benchmark executes each measured query through:

```sql
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON) <query>
```

This is used for three reasons:

- `EXPLAIN ANALYZE` returns separate `Planning Time` and `Execution Time`
  fields, which keeps planner overhead separate from execution behavior.
- `FORMAT JSON` gives the runner a structured format instead of parsing text.
- `TIMING OFF` disables per-node timing overhead while PostgreSQL still measures
  top-level statement runtime.

`EXPLAIN ANALYZE` is not treated as a zero-overhead latency measurement here.
PostgreSQL documents that node-level timing can add profiling overhead and that
the overhead depends on the query and platform.  This benchmark reduces that
specific source of error with `TIMING OFF`, does not request per-node timings,
does not use buffer or WAL timing, and compares medians/ratios across variants
under the same measurement path.

Some residual overhead still remains because the statement is executed through
`EXPLAIN ANALYZE`, summary timing must still be measured, and JSON output must
be produced.  This is the tradeoff for getting planning and execution phase
times from the PostgreSQL backend in one structured result.  For very short
execution-time queries, especially the small-data planning-stress workloads,
execution-time numbers should be read as diagnostic rather than as normal
client-visible latency.  Planning-time results are the main signal for those
workloads.

PostgreSQL documents that `Planning Time` is plan generation time, while
`Execution Time` excludes parsing, rewriting, and planning.  It also documents
that statement runtime is still measured when node-level timing is disabled.

References:

- PostgreSQL `EXPLAIN` documentation:
  <https://www.postgresql.org/docs/current/using-explain.html>
- PostgreSQL `EXPLAIN` command reference:
  <https://www.postgresql.org/docs/current/sql-explain.html>

## Result Artifacts

Run artifacts are local and ignored by git:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
```

Use [OUTPUTS.md](OUTPUTS.md) for column definitions, console-output semantics,
and reviewer-table layout.
