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

1. Check prepared databases, required PostgreSQL GUCs, and resume context before
   executing queries.
2. Stabilize each prepared database for a new run; `--resume-run-id` keeps the
   existing statistics snapshot.
3. Resolve the full dataset/query/variant plan before execution and write the
   initial `run.json`.
4. Execute discarded warmup groups and measured repetitions, rotating variant
   order across query groups.
5. Checkpoint `run.json`, `raw.csv`, and `summary.csv` after complete warmup
   groups and complete measured groups.
6. Record timeout/error rows and rebuild progress from artifacts when resuming.

Then read [bench_exec.py](bench_exec.py) for the single-statement execution
mechanics:

1. Build a clean session prelude with `RESET ALL`, `statement_timeout`, scenario
   GUCs, variant GUCs, and supported optional variant GUCs.
2. Run `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)` for
   each benchmark statement.
3. Parse planning time, execution time, total time, and plan total cost from the
   JSON result.
4. Classify PostgreSQL `statement_timeout` separately from other errors.
5. Check database reachability, validate mandatory GUCs, and stabilize prepared
   databases for fresh runs.

After those two files, the closest support file is:

- [bench_results.py](bench_results.py): writes `raw.csv`, `summary.csv`, and
  `run.json`, which are the durable run artifacts documented in
  [../OUTPUTS.md](../OUTPUTS.md).

## Supporting Files

| File | Role |
| --- | --- |
| [bench.py](bench.py) | Thin CLI entry point for `list`, `prepare`, and `run`. |
| [bench_registry.py](bench_registry.py) | Built-in scenarios and baseline variants, plus extra-variant TOML loading and scenario resolution. |
| [bench_catalog.py](bench_catalog.py) | Dataset catalog, query manifest access, SQL loading, and query wrapping for synthetic workloads. |
| [bench_prepare.py](bench_prepare.py) | Database creation/loading and prepared-database checks. |
| [bench_common.py](bench_common.py) | Shared records, paths, SQL quoting helpers, and `psql` command helpers. |
| [bench_review_tables.py](bench_review_tables.py) | Optional post-processing for reviewer XLSX/CSV tables; not needed to review the run protocol. |

## Data Flow

```text
bench.py run
    -> bench_registry.py / bench_catalog.py
    -> bench_run.py
        -> bench_exec.py
        -> bench_results.py
    -> outputs/<run_id>/{run.json,raw.csv,summary.csv}
```

Prepare is a separate setup path:

```text
bench.py prepare
    -> bench_registry.py / bench_catalog.py
    -> bench_prepare.py
    -> prepared PostgreSQL databases
```

Scenario and dataset coverage is described in [../WORKLOADS.md](../WORKLOADS.md).
Extra variant file fields are described in [../examples/README.md](../examples/README.md).
