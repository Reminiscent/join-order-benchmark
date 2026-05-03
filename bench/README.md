# Benchmark Harness Modules

This directory contains the Python implementation behind `bench/bench.py`.  For
reviewers, start with [../BENCHMARK_RUNS.md](../BENCHMARK_RUNS.md) to understand
the public benchmark protocol; use this file as the code map.

## Reading Path

Read these files first:

1. [bench.py](bench.py): CLI entry point for `list`, `prepare`, and `run`.
2. [bench_workloads.py](bench_workloads.py): built-in scenarios, variants,
   dataset/query resolution, query-manifest access, and SQL wrapping.
3. [bench_run.py](bench_run.py): measured run orchestration, including output
   directory setup, database checks, statistics stabilization, warmup/measured
   group execution, and artifact flushes.
4. [bench_exec.py](bench_exec.py): one PostgreSQL statement execution path.  Its
   main entry point is `run_one_statement()`, which builds the session prelude,
   runs `EXPLAIN ANALYZE` JSON, parses timing/cost fields, and classifies
   `statement_timeout`.
5. [bench_results.py](bench_results.py): writes `run.json`, `raw.csv`, and
   `summary.csv`.

## Data Flow

`bench.py prepare` is the setup path:

```text
bench.py prepare
    -> bench_workloads.py
    -> bench_prepare.py
    -> prepared PostgreSQL databases
```

`bench.py run` is the measurement path:

```text
bench.py run
    -> bench_workloads.py
    -> bench_run.py
        -> bench_exec.py
        -> bench_results.py
    -> outputs/<run_id>/{run.json,raw.csv,summary.csv}
```

## Supporting Files

| File | Role |
| --- | --- |
| [bench_prepare.py](bench_prepare.py) | Database creation/loading and prepared-database checks. |
| [bench_common.py](bench_common.py) | Shared records, repository paths, SQL quoting, and `psql` helpers. |
| [bench_review_tables.py](bench_review_tables.py) | Optional `review.xlsx` rendering; not needed to understand the run protocol. |

Scenario and dataset coverage is described in [../WORKLOADS.md](../WORKLOADS.md).
Extra variant file fields are described in [../examples/README.md](../examples/README.md).
