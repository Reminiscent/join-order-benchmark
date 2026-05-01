# Benchmark Harness Modules

This directory contains the Python benchmark harness.  The code is intentionally
split by workflow step so benchmark protocol, data preparation, execution, and
reviewer-table generation can be reviewed independently.

## Main Ideas

The harness follows four steps:

1. Choose the test scope: scenario, dataset, and query.
2. Choose the algorithm configuration: variant and PostgreSQL GUCs.
3. Execute the benchmark: prepare databases, then collect planning and execution
   metrics with `EXPLAIN ANALYZE`.
4. Produce reviewer-facing results: write `summary.csv`, then render reviewer
   tables when needed.

Most users only need [bench.py](bench.py) and
[../examples/variants.toml](../examples/variants.toml).  Scenarios are built
in; variants are the user-facing way to compare a submitted algorithm.

## Files

| File | Role |
| --- | --- |
| [bench.py](bench.py) | CLI entry point for `list`, `prepare`, and `run`.  It parses arguments, loads built-in scenarios and variant definitions, and dispatches to preparation or execution. |
| [bench_common.py](bench_common.py) | Shared dataclasses, repository paths, safe names, PostgreSQL command helpers, SQL quoting helpers, and fatal error handling. |
| [bench_config.py](bench_config.py) | Built-in scenario definitions and variant TOML loader.  It resolves scenario defaults, explicit `--variants`, built-in dataset entries, and prepare targets. |
| [bench_catalog.py](bench_catalog.py) | Dataset catalog and query manifest access.  It maps dataset ids to databases, prepare scripts, query SQL, join-size filters, and `SELECT count(*) FROM (...)` wrapping for synthetic workloads. |
| [bench_prepare.py](bench_prepare.py) | Database preparation flow.  It creates or reuses benchmark databases, loads schema/data SQL, applies IMDB CSV variables, and checks prepared markers. |
| [bench_exec.py](bench_exec.py) | Single-query execution.  It builds the per-run session prelude, executes `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)`, parses planning/execution metrics, handles timeouts, and runs stabilization SQL. |
| [bench_run.py](bench_run.py) | Scenario execution driver.  It verifies prepared databases and required GUCs, resolves selected queries and variants, performs warmup and measured repetitions, rotates variant order, records timeout/error rows, checkpoints progress, and supports `--resume-run-id`. |
| [bench_results.py](bench_results.py) | Artifact writer for `raw.csv`, `summary.csv`, and `run.json`.  It defines the durable per-run output contract used by reviewer tables. |
| [bench_review_tables.py](bench_review_tables.py) | Reviewer table renderer used by [../tools/render_review_tables.py](../tools/render_review_tables.py).  It creates styled XLSX workbooks and CSV companion files with `dp`-based ratios. |

## Data Flow

```text
built-in scenarios + variants.toml
        |
        v
bench_config.py
        |
        v
bench_prepare.py  -> prepared PostgreSQL databases
        |
        v
bench_run.py -> bench_exec.py
        |
        v
bench_results.py -> outputs/<run_id>/{run.json,raw.csv,summary.csv}
        |
        v
bench_review_tables.py -> reviewer-facing XLSX/CSV tables
```

The benchmark protocol itself is described in [../BENCHMARK_RUNS.md](../BENCHMARK_RUNS.md).
Output formats are described in [../OUTPUTS.md](../OUTPUTS.md).
