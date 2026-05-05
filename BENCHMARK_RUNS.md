# Benchmark Runs

This document explains what the benchmark runner does during a public run.  Use
[REPRODUCE.md](REPRODUCE.md) for the command checklist and [OUTPUTS.md](OUTPUTS.md)
for artifact formats.

## What Runs

The benchmark CLI is [bench/bench.py](bench/bench.py):

```bash
python3 bench/bench.py list scenarios
python3 bench/bench.py list datasets
python3 bench/bench.py list variants
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run main --variants dp,geqo
```

`list scenarios` shows workload groups such as `main`, `extended`, and `planning`.
`list datasets` shows the query/data sources.  `list variants` shows built-in
variants plus `examples/variants.toml` when that default file exists.  Edit
`examples/variants.toml` to change the default extra variants; use
`--variants-file` only for a different TOML file.

`main` is the primary public validation scenario.  It runs the complete JOB and
JOB-Complex workloads.  `extended` adds the heavier CEB IMDB 3k workload.
`planning` contains synthetic wide-join planning/search-space workloads.

## Prepare Phase

`bench.py prepare <scenario>` creates the target PostgreSQL databases and loads
the datasets selected by the scenario.  For `main`, both `job` and `job_complex`
use the same `imdb_bench` database, so IMDB data is loaded once and reused by
both query suites.

IMDB-backed datasets require `--csv-dir` because the CSV bundle is not vendored
here.  Self-contained datasets, such as SQLite select5 and GPUQO small
workloads, load local SQL files.  If a target database already looks prepared,
`prepare` skips it; use `--force-recreate` only to drop and rebuild it.

Scenario and dataset coverage is documented in [WORKLOADS.md](WORKLOADS.md).

## Run Phase

`bench.py run <scenario>` performs the measured benchmark run:

1. Resolve the scenario, datasets, variants, and query list.
2. Check that benchmark databases are reachable and required GUCs exist.
3. Unless `--reuse-stats` is passed, stabilize each distinct prepared database
   with `VACUUM FREEZE ANALYZE` and a best-effort `CHECKPOINT`.  Datasets that
   share one database, such as `job` and `job_complex`, share this one step.
4. Write the initial `run.json` so the intended run shape is visible even if
   the process is interrupted.
5. For each query group, run discarded warmup pass(es), then measured
   repetitions.
6. Rewrite `run.json`, `raw.csv`, and `summary.csv` as each group finishes, or
   before exiting on a fatal error.

[bench/bench_run.py](bench/bench_run.py) is the execution driver.  It keeps the
main run loop ordered by dataset, query, warmup pass, repetition, and variant.

## Public Protocol

These values define the public benchmark protocol:

- 3 measured repetitions per selected query and variant.
- 1 discarded warmup pass per query group.
- Variant order rotates across query groups and repetitions.
- By default, each run refreshes table statistics once per distinct database
  before any query runs.
- `statement_timeout` defaults to `600000 ms`.
- Non-timeout warmup or measured errors terminate the run after current
  artifacts are written.

Measured repetitions, warmup count, and variant order are fixed by the runner.
Use `--reuse-stats` only when comparing separate runs that should share the same
existing statistics snapshot.  `--statement-timeout-ms` only changes the
guardrail timeout; it is not an algorithm knob.

## PostgreSQL Settings

Public runs use a 16 GiB RAM baseline.  Configure the cluster-level buffer pool
outside the harness and restart PostgreSQL:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

Each warmup and measured execution starts from a fresh session prelude:

```sql
RESET ALL;
SET statement_timeout = 600000;
SET join_collapse_limit = 100;
SET max_parallel_workers_per_gather = 0;
SET work_mem = '1GB';
SET effective_cache_size = '8GB';
-- followed by variant-specific SET commands
```

Variant settings are applied after the scenario settings.  Optional variant
GUCs are applied only when the current PostgreSQL build exposes that GUC.  The
harness emits these session settings before every warmup and measured SQL
because each statement runs in its own `psql` session.  It does not change
restart-required cluster settings.

## Timing Collection

The benchmark executes each measured query through:

```sql
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON) <query>
```

This gives the runner structured `Planning Time` and `Execution Time` fields
from PostgreSQL.  `TIMING OFF` avoids per-node timing overhead while still
recording top-level planning and execution times.  All variants use the same
measurement path, so the reported medians and ratios are comparable within a
run.

`EXPLAIN ANALYZE` is not a zero-overhead client-latency measurement.  For very
short execution-time queries, especially small synthetic planning workloads,
execution-time numbers should be read as diagnostic; planning time is the main
signal for those workloads.

References:

- PostgreSQL `EXPLAIN` documentation:
  <https://www.postgresql.org/docs/current/using-explain.html>
- PostgreSQL `EXPLAIN` command reference:
  <https://www.postgresql.org/docs/current/sql-explain.html>

## Timeout Handling

If warmup for a `(dataset, query, variant)` tuple hits `statement_timeout`, the
later measured repetitions for the same tuple are recorded as skipped timeout
rows instead of re-running the same timeout-prone statement.  A measured
`statement_timeout` is recorded as a benchmark result.

Warmup errors and measured non-timeout errors stop the command after the current
artifacts are written.  Use a new run if the benchmark process is interrupted.

## Artifacts

Run artifacts are local and ignored by git:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
```

Use [OUTPUTS.md](OUTPUTS.md) for column definitions, console-output semantics,
and reviewer-table layout.  Use `tools/render_review_tables.py` to render
`outputs/<run_id>/review.xlsx` for community attachments.
