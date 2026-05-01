# Benchmark Runs

This document explains what the benchmark scripts do during a public run.  Use
[REPRODUCE.md](REPRODUCE.md) for command syntax and [OUTPUTS.md](OUTPUTS.md) for
artifact formats.

## Runner Entry Points

The public CLI is [bench/bench.py](bench/bench.py):

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run main --variants dp,geqo
```

Use a submitted variants file when comparing a patch-specific algorithm:

```bash
python3 bench/bench.py run main \
  --variants-file path/to/variants.toml \
  --variants dp,geqo,my_algo
```

## Prepare Phase

`bench.py prepare <scenario>` resolves the datasets listed in
[config/scenarios.toml](config/scenarios.toml), creates the target PostgreSQL
databases, and loads each dataset's schema and data.

IMDB-backed datasets require `--csv-dir` because the CSV bundle is not vendored
in this repository.  Self-contained datasets, including SQLite select5 and GPUQO
small workloads, load local SQL files.

## Run Phase

`bench.py run <scenario>` performs these steps:

1. Read the selected scenario from [config/scenarios.toml](config/scenarios.toml).
2. Read variant definitions from [examples/variants.toml](examples/variants.toml)
   or the submitted `--variants-file`.
3. Resolve the explicit `--variants` list, or use the scenario's
   `default_variants`.
4. Apply scenario dataset filters, such as the `gpuqo_clique_small` `dp`
   `join_size <= 12` guard.
5. Check that benchmark databases are reachable and required GUCs exist.
6. Stabilize each prepared database when requested by the scenario.
7. For each selected query, run discarded warmup pass(es), then measured
   repetitions.
8. Flush `run.json`, `raw.csv`, `summary.csv`, `public_report.md`, and
   `public_report.json` after safe progress boundaries.

The runner checkpoints progress after complete warmup groups and complete
measured groups, so `--resume-run-id` resumes from a safe group boundary rather
than from the middle of a query's variant set.

## Cluster Memory Baseline

The public runs are designed to work on a modest benchmark machine rather than
requiring a large-memory lab server.  The memory baseline is 16 GiB RAM or more.
That lower bound leaves room for a 4 GiB PostgreSQL buffer pool, operating-system
page cache, backend memory, and occasional large hash/sort operations while
keeping the setup accessible.

Most controls are session-level and are applied by the harness, but
`shared_buffers` is a cluster-level PostgreSQL setting and must be configured
before the benchmark:

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

Each warmup and measured execution starts with a fresh session prelude:

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

## Stabilization

The built-in public scenarios use:

```toml
stabilize = "vacuum_freeze_analyze"
```

That mode executes:

```sql
VACUUM FREEZE ANALYZE;
CHECKPOINT;
```

`VACUUM FREEZE ANALYZE` refreshes table statistics and marks eligible tuples as
frozen before measurement.  `CHECKPOINT` is best-effort; the benchmark continues
if it is not allowed for the current connection.

## Warmup And Measurement Order

The default public run uses one discarded warmup pass per query group:

```text
query 1: warm up all selected variants, then measure rep 1..N
query 2: warm up all selected variants, then measure rep 1..N
...
```

Warmup executions are not written to `raw.csv` or `summary.csv`.

`variant_order_mode = "rotate"` rotates variant order across query groups and
repetitions.  This avoids always measuring the same variant first or last for
every query.

If a warmup execution hits `statement_timeout`, the default behavior is to skip
later measured repetitions for the exact `(dataset, query, variant)` tuple and
record timeout rows.  Use `--no-skip-measured-after-warmup-timeout` only when
you intentionally want measured repetitions to re-run after a warmup timeout.

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
  public_report.md
  public_report.json
```

Use [OUTPUTS.md](OUTPUTS.md) for column definitions, console-output semantics,
and reviewer-table layout.
