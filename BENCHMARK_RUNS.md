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

The harness does not change restart-required cluster settings such as
`shared_buffers`.  Those must be configured outside the benchmark if a submitted
run depends on them.

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

PostgreSQL documents that `Planning Time` is plan generation time, while
`Execution Time` excludes parsing, rewriting, and planning.  PostgreSQL also
documents that node-level timing can add profiling overhead, and that statement
runtime is still measured when node-level timing is disabled.

On PostgreSQL master, commit
`294520c44487ecaade7a6ea8781b973f9ed03909` further reduced
`EXPLAIN ANALYZE` timing overhead on x86-64 by using the CPU time-stamp counter
for instrumentation timing.  The benchmark still uses `TIMING OFF` because
per-node timings are not needed for join-order comparisons.

References:

- PostgreSQL `EXPLAIN` documentation:
  <https://www.postgresql.org/docs/current/using-explain.html>
- PostgreSQL `EXPLAIN` command reference:
  <https://www.postgresql.org/docs/current/sql-explain.html>
- PostgreSQL commit message:
  <https://www.mail-archive.com/pgsql-committers%40lists.postgresql.org/msg45536.html>
- PostgreSQL commit diff:
  <https://git.postgresql.org/pg/commitdiff/294520c44487ecaade7a6ea8781b973f9ed03909>

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
