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
  public_report.md
  public_report.json
```

`<run_id>` is generated as:

```text
YYYYMMDD_HHMMSS_microseconds_<scenario>
```

For example:

```text
outputs/20260412_142110_777847_main/
```

When `--resume-run-id` is used, the same directory is reused and rewritten from
the saved progress boundary.  The harness checkpoints after complete warmup
groups and complete measured groups, so resume does not continue from the middle
of a query group's variant set.

## Console Output

During a run, the harness prints progress and the output directory:

```text
[run] scenario=main
[run] variants=dp,geqo,my_algo
[run] warmup_passes=1 measured_reps=3
[run] outputs=outputs/20260412_142110_777847_main
[run] dataset=job db=imdb_bench queries=113 variants=dp,geqo,my_algo max_join=None
[run] dataset=job_complex db=imdb_bench queries=30 variants=dp,geqo,my_algo max_join=None
[run] completed without errors
```

If failures occur, the harness prints grouped summaries such as:

```text
[run] warmup_timeouts=1
[run] warmup_timeout dataset=job variant=geqo query=29a: ERROR: canceling statement due to statement timeout
[run] timeouts=1
[run] timeout dataset=job variant=geqo query=29a: skipped measured run after warmup timeout: ERROR: canceling statement due to statement timeout
[run] completed with non-fatal failures
```

With `--fail-on-error`, a warmup error or measured non-timeout error makes the
command exit non-zero after writing the current artifacts.

## `run.json`

`run.json` is the run context used to explain and re-render reports.  It is
intentionally not a full machine or git provenance snapshot.

Important fields:

| Field | Meaning |
| --- | --- |
| `run_id` | Output directory id. |
| `scenario` | Scenario name, such as `main`, `extended`, or `full`. |
| `scenario_description` | Description from `config/scenarios.toml`. |
| `protocol.reps` | Number of measured repetitions per query and variant. |
| `protocol.statement_timeout_ms` | Per-statement timeout used during measurement. |
| `protocol.stabilize` | Stabilization mode, for example `vacuum_freeze_analyze`. |
| `protocol.warmup_runs` | Number of discarded warmup passes per query group. |
| `protocol.skip_measured_after_warmup_timeout` | Whether measured repetitions are skipped after a warmup timeout for the same `(dataset, query, variant)`. |
| `protocol.warmup_scope` | Current warmup scope; normally `query_group_discarded_pass`. |
| `protocol.measurement_lane` | The PostgreSQL measurement command shape, currently `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)`. |
| `protocol.session_gucs` | Scenario-level session GUCs applied before variant settings. |
| `variants` | Resolved variant names, labels, and session GUCs used in this run. |
| `datasets` | Resolved dataset entries and the variants actually run for each entry. |
| `tag` | Optional user-provided build or patch label. |
| `warmup_failures` | Warmup timeout/error records, if any. |
| `termination` | Fatal termination record, if the run stopped early. |
| `progress` | Resume state and whether the run completed. |

Use `run.json` when a reviewer needs to check which scenario, variants, session
GUCs, timeouts, warmup settings, and dataset filters produced a result table.
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
| `run_id` | Run id. |
| `scenario` | Scenario name. |
| `dataset` | Dataset id. |
| `db` | PostgreSQL database used for that dataset. |
| `variant` | Variant id. |
| `query_id` | Query id inside the dataset. |
| `query_label` | Optional query label from the manifest. |
| `query_path` | SQL file path relative to the repository. |
| `join_size` | Number of joined relations recorded in the query manifest. |
| `variant_position` | Position of this variant in the rotated execution order for the query group. |
| `rep` | Measured repetition number. |
| `planning_ms` | PostgreSQL `Planning Time` from EXPLAIN JSON. |
| `total_ms` | Planning plus execution time for the measured repetition. |
| `execution_ms` | PostgreSQL `Execution Time` from EXPLAIN JSON. |
| `execution_measurement_mode` | Measurement mode; currently backend EXPLAIN phase timings. |
| `plan_total_cost` | Root plan `Total Cost` from EXPLAIN JSON. |
| `status` | `ok`, `timeout`, or `error`. |
| `error` | Error text for timeout/error rows, otherwise empty. |

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
| `run_id`, `scenario`, `dataset`, `db`, `variant` | Same identifiers as `raw.csv`. |
| `query_id`, `query_label`, `query_path`, `join_size` | Query metadata from the manifest. |
| `planning_ms_median` | Median `planning_ms` over successful measured repetitions. |
| `execution_ms_median` | Median `execution_ms` over successful measured repetitions. |
| `total_ms_median` | Median `total_ms` over successful measured repetitions. |
| `plan_total_cost_median` | Median `plan_total_cost` over successful measured repetitions. |
| `ok_reps` | Number of successful measured repetitions. |
| `err_reps` | Number of timeout/error measured repetitions. |

If a query/variant has no successful measured repetition, the median columns are
empty and `ok_reps` is `0`.

Use `summary.csv` for per-query ratio tables and public reports.

## Public Reports

`public_report.md` and `public_report.json` are generated from `summary.csv`.
They contain the same information in human-readable Markdown and
machine-readable JSON.

The public report is a compact overview, not the final spreadsheet attachment.
For each dataset and for execution/planning metrics separately, it includes:

- coverage: successful, missing, and comparable query counts
- ratio summary: wins, within 5%, slower than 5%, geometric mean, mean, p50,
  p90, p95, optional p99, and max ratio
- tail counts: number of queries above fixed slowdown thresholds
- workload totals: summed median metric values over comparable queries
- worst queries for execution time
- planning share: planning time divided by planning plus execution time

The reference variant is `dp` when present; otherwise the first resolved variant
is used.  Public-report ratios are direct `variant/reference` ratios, and rows
with non-positive reference metrics are omitted from ratio summaries.

Re-render the public reports from an existing run directory with:

```bash
python3 tools/render_public_reports.py outputs/<run_id>
```

## Reviewer Tables

Reviewer tables are generated explicitly from an existing run directory:

```bash
python3 tools/render_review_tables.py outputs/<run_id> \
  --dataset job \
  --variants dp,geqo,my_algo
```

For each selected dataset, the command writes:

```text
outputs/<run_id>/review_tables/
  review_<dataset>.xlsx
  review_<dataset>_execution.csv
  review_<dataset>_planning.csv
```

The XLSX workbook contains two sheets:

- `<dataset> execution`
- `<dataset> planning`

Reviewer tables require `dp` in the selected variant list.  Ratios are direct
`variant/dp` ratios.  Execution time is the primary result; planning time is a
separate diagnostic sheet.

### Example Table Shape

Suppose `summary.csv` contains these median execution times:

| query | joins | dp | geqo | my_algo |
| --- | ---: | ---: | ---: | ---: |
| `2a` | 12 | 200.00 | 260.00 | 150.00 |
| `10a` | 7 | 100.00 | 110.00 | 80.00 |

The execution sheet in `review_job.xlsx` is shaped like this:

| query | joins | DP | GEQO | My Algorithm | GEQO/DP | My Algorithm/DP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `2a` | 12 | 200.00 | 260.00 | 150.00 | 1.30 | 0.75 |
| `10a` | 7 | 100.00 | 110.00 | 80.00 | 1.10 | 0.80 |
| `SUM` |  | 300.00 | 370.00 | 230.00 | 1.23 | 0.77 |

The ratio cells are computed as:

```text
GEQO/DP for 2a = 260.00 / 200.00 = 1.30
My Algorithm/DP for 2a = 150.00 / 200.00 = 0.75
GEQO/DP SUM = (260.00 + 110.00) / (200.00 + 100.00) = 1.23
My Algorithm/DP SUM = (150.00 + 80.00) / (200.00 + 100.00) = 0.77
```

The CSV companion uses the same values without workbook styling:

```csv
query,join_size,dp_execution_ms_median,geqo_execution_ms_median,my_algo_execution_ms_median,geqo_to_dp,my_algo_to_dp
2a,12,200,260,150,1.3,0.75
10a,7,100,110,80,1.1,0.8
SUM,,300,370,230,1.23,0.77
```

The planning CSV and planning sheet use the same layout with
`planning_ms_median` values.

### Ratio Colors

The workbook colors ratio cells to make large changes visible:

| Ratio | Meaning | Color group |
| ---: | --- | --- |
| `< 0.75` | much faster than `dp` | strong green |
| `< 0.95` | faster than `dp` | green |
| `0.95` through `1.05` | roughly equivalent | neutral |
| `> 1.05` through `1.25` | slower than `dp` | yellow |
| `> 1.25` through `2.00` | much slower than `dp` | orange |
| `> 2.00` | severe slowdown | red |

Missing metric values are left blank and styled as missing cells.  The `SUM`
ratio for a variant is computed only over rows where both that variant and `dp`
have values.
