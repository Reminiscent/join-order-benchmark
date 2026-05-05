# Outputs

This document describes the files produced by a benchmark run and the reviewer
tables derived from those files.  Result directories are local artifacts under
`outputs/` and are ignored by git.

## Run Output Directory

`bench.py run` writes one directory per run:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
```

`<run_id>` is generated as:

```text
YYYYMMDD_HHMMSS_microseconds_<scenario>
```

The timestamp is UTC.

For example:

```text
outputs/20260412_142110_777847_main/
```

## Console Output

During a run, the harness prints status and the output directory:

```text
[run] scenario=main
[run] variants=dp,geqo,my_algo
[run] warmup_passes=1 measured_reps=3
[run] stats_refresh=before_run
[run] outputs=/path/to/join_order_benchmark/outputs/20260412_142110_777847_main
[run] dataset=job db=imdb_bench queries=113 variants=dp,geqo,my_algo min_join=None
[run] dataset=job_complex db=imdb_bench queries=30 variants=dp,geqo,my_algo min_join=None
[run] completed without errors
```

If failures occur, the harness prints grouped summaries such as:

```text
[run] warmup_timeouts=1
[run] warmup_timeout dataset=job variant=geqo query=29a: ERROR: canceling statement due to statement timeout
[run] skipped_timeouts=1
[run] skipped_timeout dataset=job variant=geqo query=29a: skipped measured run after warmup timeout: ERROR: canceling statement due to statement timeout
[run] completed with non-fatal failures
```

A warmup error or measured non-timeout error makes the command exit non-zero
after writing the current artifacts.  `statement_timeout` rows remain non-fatal
because they are benchmark guardrail results.

## `run.json`

`run.json` is the run context used to explain and render reviewer tables.  It is
intentionally not a full machine or git provenance snapshot.

Important fields:

| Field | Meaning |
| --- | --- |
| `run_id` | Output directory id. |
| `scenario` | Scenario name, such as `main`, `extended`, or `planning`. |
| `scenario_description` | Description from the built-in scenario definition. |
| `statement_timeout_ms` | Per-statement guardrail timeout used during measurement. |
| `protocol` | Measured reps, warmup runs, timing mode, variant order, and stats refresh rule used by the run. |
| `variants` | Resolved variant names, labels, and session GUCs used in this run. |
| `datasets` | Resolved dataset entries, the min-join filter, and the variants actually run for each entry. |
| `tag` | Optional user-provided build or patch label. |
| `warmup_failures` | Warmup timeout/error records, if any. |
| `termination` | Fatal termination record, if the run stopped early. |

Use `run.json` when a reviewer needs to check which scenario, variants,
datasets, adjustable timeout, and core execution protocol produced a result
table.  The full protocol rationale is documented in
[BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).

Restart-required cluster settings, such as `shared_buffers`, are not changed by
the harness and should be recorded with the published result set when they
matter for review.

## `raw.csv`

`raw.csv` is the source-of-truth measurement log.  It contains one row per
measured repetition:

```text
(dataset, query, variant, rep)
```

Columns:

| Column | Meaning |
| --- | --- |
| `dataset` | Dataset id. |
| `query_id` | Query id inside the dataset. |
| `variant` | Variant id. |
| `rep` | Measured repetition number. |
| `planning_ms` | PostgreSQL `Planning Time` from EXPLAIN JSON. |
| `execution_ms` | PostgreSQL `Execution Time` from EXPLAIN JSON. |
| `total_ms` | Planning plus execution time for the measured repetition. |
| `plan_total_cost` | Root plan `Total Cost` from EXPLAIN JSON. |
| `status` | `ok`, `timeout`, or `error`. |
| `error` | Error text for timeout/error rows, otherwise empty. |

Failure rows use these meanings:

| `status` | Meaning |
| --- | --- |
| `timeout` | The measured SQL hit `statement_timeout`, or the measured row was skipped because the same query/variant timed out during warmup. |
| `error` | A non-timeout error occurred; the run exits non-zero after writing the current artifacts. |

Use `raw.csv` when auditing a specific query, timeout, or repetition.

## `summary.csv`

`summary.csv` is the per-query comparison layer derived from `raw.csv`.  It has
one row per:

```text
(dataset, query, variant)
```

Columns:

| Column | Meaning |
| --- | --- |
| `dataset`, `query_id`, `join_size`, `variant` | Query and variant identifiers needed for reviewer tables. |
| `planning_ms_median` | Median `planning_ms` over successful measured repetitions. |
| `execution_ms_median` | Median `execution_ms` over successful measured repetitions. |
| `total_ms_median` | Median `total_ms` over successful measured repetitions. |
| `plan_total_cost_median` | Median `plan_total_cost` over successful measured repetitions. |
| `ok_reps` | Number of successful measured repetitions. |
| `timeout_reps` | Number of measured rows whose status is `timeout`. |
| `error_reps` | Number of measured rows whose status is `error`. |

`join_size` is the base-relation count recorded in
[tools/query_manifest.csv](tools/query_manifest.csv).  It is computed from the
flat comma-separated `FROM` lists used by the built-in workloads.  The reviewer
workbook labels the same value as `joins` for compactness, but it should be read
as query width in base relations, not as a parsed binary join-operator count.

If a query/variant has no successful measured repetition, the median columns are
empty and `ok_reps` is `0`.

Use `summary.csv` for per-query ratio tables and reviewer table rendering.

## Reviewer Tables

Reviewer tables are generated explicitly from an existing run directory:

```bash
python3 tools/render_review_tables.py outputs/<run_id>
```

The command writes one combined workbook into the run directory:

```text
outputs/<run_id>/review.xlsx
```

The XLSX workbook contains two sheets:

- `execution`
- `planning`

The workbook is the reviewer-facing attachment: it has frozen headers, grouped
metric and ratio columns, number formats, a `SUM` row, and ratio colors.  The
XLSX export uses optional `XlsxWriter`; benchmark prepare/run does not need it.

Reviewer tables require `dp` in the selected variant list.  Ratios are direct
`variant/dp` ratios.  Execution time is the primary result; planning time is a
separate diagnostic sheet.  All datasets recorded in `run.json` are shown in
one table, with `dataset` as the first column, so the uploaded attachment count
stays small even for larger runs.

### Example Table Shape

Suppose `summary.csv` contains these median execution times:

| dataset | query | joins | dp | geqo | my_algo |
| --- | --- | ---: | ---: | ---: | ---: |
| `job` | `2a` | 12 | 200.00 | 260.00 | 150.00 |
| `job` | `10a` | 7 | 100.00 | 110.00 | 80.00 |

The execution sheet in `review.xlsx` is shaped like this:

| dataset | query | joins | DP | GEQO | My Algorithm | GEQO/DP | My Algorithm/DP |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `job` | `2a` | 12 | 200.00 | 260.00 | 150.00 | 1.30 | 0.75 |
| `job` | `10a` | 7 | 100.00 | 110.00 | 80.00 | 1.10 | 0.80 |
| `SUM` |  |  | 300.00 | 370.00 | 230.00 | 1.23 | 0.77 |

The ratio cells are computed as:

```text
GEQO/DP for 2a = 260.00 / 200.00 = 1.30
My Algorithm/DP for 2a = 150.00 / 200.00 = 0.75
GEQO/DP SUM = (260.00 + 110.00) / (200.00 + 100.00) = 1.23
My Algorithm/DP SUM = (150.00 + 80.00) / (200.00 + 100.00) = 0.77
```

The planning sheet uses the same layout with `planning_ms_median` values.

### Ratio Colors

The workbook colors ratio cells to make large changes visible:

| Ratio | Meaning | Color group |
| ---: | --- | --- |
| `< 0.50` | much faster than `dp` | dark green |
| `0.50` through `< 0.80` | faster than `dp` | green |
| `0.80` through `< 1.20` | roughly equivalent | neutral |
| `1.20` through `< 2.00` | slower than `dp` | light red |
| `2.00` through `< 10.00` | much slower than `dp` | red |
| `>= 10.00` | severe slowdown | dark red |

Missing metric values are left blank and styled as missing cells.  The `SUM`
ratio for a variant is computed only over rows where both that variant and `dp`
have values.
