# Benchmark Harness Modules

This directory contains the Python benchmark harness.  For a reviewer checking
how benchmark results were produced, the two key implementation files are
[bench_run.py](bench_run.py) and [bench_exec.py](bench_exec.py).  Most other
files support setup, artifact writing, or optional reviewer-table rendering.

## Reviewer Reading Path

Start with [../BENCHMARK_RUNS.md](../BENCHMARK_RUNS.md) for the public run
protocol, then read [bench_run.py](bench_run.py) for run orchestration and
[bench_exec.py](bench_exec.py) for PostgreSQL session execution.

[bench_run.py](bench_run.py) owns the run orchestration:

1. Check prepared databases and required PostgreSQL GUCs before executing
   queries.
2. Refresh table statistics for each prepared database in a new run; resumed
   runs keep the existing statistics snapshot.
3. Resolve the full dataset/query/variant plan before execution and write the
   initial `run.json`.
4. Execute discarded warmup groups and measured repetitions, rotating variant
   order across query groups.
5. Checkpoint group-level progress in `run.json`, `raw.csv`, and `summary.csv`.
6. Treat `statement_timeout` as a recorded result, while non-timeout errors
   terminate the run after current artifacts are written.

Then read [bench_exec.py](bench_exec.py) for the single-statement execution
mechanics:

1. Build a clean session prelude with `RESET ALL`, `statement_timeout`, scenario
   GUCs, variant GUCs, and supported optional variant GUCs.
2. Run `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)` for
   each benchmark statement.
3. Parse planning time, execution time, total time, and plan total cost from the
   JSON result.
4. Classify PostgreSQL `statement_timeout` separately from other errors.
5. Check database reachability, validate mandatory GUCs, and refresh table
   statistics for fresh runs.

After those two files, the closest support file is:

- [bench_results.py](bench_results.py): writes `raw.csv`, `summary.csv`, and
  `run.json`, which are the durable run artifacts documented in
  [../OUTPUTS.md](../OUTPUTS.md).

## Supporting Files

| File | Role |
| --- | --- |
| [bench.py](bench.py) | Thin CLI entry point for `list`, `prepare`, and `run`. |
| [bench_catalog.py](bench_catalog.py) | Built-in scenarios and variants, dataset resolution, query manifest access, SQL loading, and query wrapping. |
| [bench_prepare.py](bench_prepare.py) | Database creation/loading and prepared-database checks. |
| [bench_common.py](bench_common.py) | Shared records, paths, SQL quoting helpers, and `psql` command helpers. |
| [bench_review_tables.py](bench_review_tables.py) | Optional post-processing for reviewer `review.xlsx`; not needed to review the run protocol. |

## Data Flow

`bench.py prepare` is the setup path: it resolves the scenario datasets, creates
or reuses PostgreSQL databases, and loads the benchmark data.

```text
bench.py prepare
    -> bench_catalog.py
    -> bench_prepare.py
    -> prepared PostgreSQL databases
```

`bench.py run` is the measurement path: it resolves variants and queries, runs
warmup/measured groups, and writes the durable artifacts for review.

```text
bench.py run
    -> bench_catalog.py
    -> bench_run.py
        -> bench_exec.py
        -> bench_results.py
    -> outputs/<run_id>/{run.json,raw.csv,summary.csv}
```

Scenario and dataset coverage is described in [../WORKLOADS.md](../WORKLOADS.md).
Extra variant file fields are described in [../examples/README.md](../examples/README.md).
