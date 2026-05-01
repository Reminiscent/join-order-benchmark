# Benchmark Harness Modules

This directory contains the Python benchmark harness.  The code is intentionally
split by workflow step so benchmark protocol, data preparation, execution, and
report generation can be reviewed independently.

## Files

| File | Role |
| --- | --- |
| [bench.py](bench.py) | CLI entry point for `list`, `prepare`, and `run`.  It parses arguments, loads scenario/variant configs, and dispatches to preparation or execution. |
| [bench_common.py](bench_common.py) | Shared dataclasses, repository paths, safe names, PostgreSQL command helpers, SQL quoting helpers, and fatal error handling. |
| [bench_config.py](bench_config.py) | TOML config loader for [../config/scenarios.toml](../config/scenarios.toml) and variant files.  It resolves scenario defaults, explicit `--variants`, dataset filters, and prepare targets. |
| [bench_catalog.py](bench_catalog.py) | Dataset catalog and query manifest access.  It maps dataset ids to databases, prepare scripts, query SQL, join-size filters, and `SELECT count(*) FROM (...)` wrapping for synthetic workloads. |
| [bench_prepare.py](bench_prepare.py) | Database preparation flow.  It creates or reuses benchmark databases, loads schema/data SQL, applies IMDB CSV variables, and checks prepared markers. |
| [bench_environment.py](bench_environment.py) | PostgreSQL environment checks.  It verifies database reachability and whether required scenario or variant GUCs exist in the current build. |
| [bench_exec.py](bench_exec.py) | Single-query execution.  It builds the per-run session prelude, executes `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)`, parses planning/execution metrics, handles timeouts, and runs stabilization SQL. |
| [bench_run.py](bench_run.py) | Scenario execution driver.  It resolves selected queries and variants, performs warmup and measured repetitions, rotates variant order, records timeout/error rows, checkpoints progress, and supports `--resume-run-id`. |
| [bench_results.py](bench_results.py) | Artifact writer for `raw.csv`, `summary.csv`, and `run.json`.  It defines the durable per-run output contract used by reports and reviewer tables. |
| [bench_public_report.py](bench_public_report.py) | Public report renderer for `public_report.md` and `public_report.json`, including ratio summaries, totals, tail counts, and planning-share diagnostics. |
| [bench_review_tables.py](bench_review_tables.py) | Reviewer table renderer used by [../tools/render_review_tables.py](../tools/render_review_tables.py).  It creates styled XLSX workbooks and CSV companion files with `dp`-based ratios. |

## Data Flow

```text
config/scenarios.toml + variants.toml
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
        +-> bench_public_report.py
        +-> bench_review_tables.py
```

The benchmark protocol itself is described in [../BENCHMARK_RUNS.md](../BENCHMARK_RUNS.md).
Output formats are described in [../OUTPUTS.md](../OUTPUTS.md).

