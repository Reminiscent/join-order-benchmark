# Benchmark Runner Modules

This directory contains the Python implementation behind `bench/bench.py`.  For
reviewers, start with [../BENCHMARK_RUNS.md](../BENCHMARK_RUNS.md) to understand
the public benchmark protocol; use this file as the code map.

## Module Map

| File | Role |
| --- | --- |
| [bench.py](bench.py) | CLI entry point for `list`, `prepare`, and `run`. |
| [bench_common.py](bench_common.py) | Cross-module records, repository paths, SQL quoting, subprocess, and `psql` helpers.  It should stay free of workload-specific policy. |
| [bench_workloads.py](bench_workloads.py) | Scenario, variant, dataset, query-manifest, and SQL-wrapping rules.  This is workload semantics, not PostgreSQL execution. |
| [bench_prepare.py](bench_prepare.py) | Database recreation and dataset loading for `bench.py prepare`. |
| [bench_run.py](bench_run.py) | Run orchestration: output directory setup, database/GUC checks, statistics stabilization, warmup/measured groups, and artifact flushes. |
| [bench_exec.py](bench_exec.py) | One PostgreSQL statement execution path: session prelude, `EXPLAIN ANALYZE` JSON, timing/cost parsing, and `statement_timeout` classification. |
| [bench_results.py](bench_results.py) | Output schemas and writers for `run.json`, `raw.csv`, and `summary.csv`. |
| [bench_review_tables.py](bench_review_tables.py) | Optional `review.xlsx` rendering from an existing run; not needed to understand the run protocol. |

## Data Flow

`bench.py prepare` is the setup path:

```text
bench.py prepare
    -> resolve scenario datasets and target databases
       [bench_workloads.py]
    -> recreate each distinct database and run schema/load/index SQL
       [bench_prepare.py]
    -> prepared PostgreSQL databases
```

`bench.py run` is the measurement path:

```text
bench.py run
    -> resolve scenario, variants, datasets, databases, and min-join filter
       [bench_workloads.py]
    -> create outputs/<run_id>, validate databases/GUCs, and stabilize stats
       [bench_run.py, bench_exec.py]
    -> for each dataset/query/variant group:
         load and wrap SQL
         [bench_workloads.py]
         apply GUCs and run one `EXPLAIN ANALYZE` JSON statement
         [bench_exec.py]
         collect per-repetition rows and summary inputs
         [bench_run.py]
    -> write run.json, raw.csv, and summary.csv
       [bench_results.py]
```

Reviewer workbook rendering is a separate post-processing path:

```text
tools/render_review_tables.py
    -> read outputs/<run_id>/{run.json,summary.csv}
       [bench_review_tables.py]
    -> write review.xlsx
       [bench_review_tables.py]
```

Scenario and dataset coverage is described in [../WORKLOADS.md](../WORKLOADS.md).
Extra variant file fields are described in [../examples/README.md](../examples/README.md).
