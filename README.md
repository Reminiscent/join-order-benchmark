# Join Order Benchmark Collection

This repository is a PostgreSQL benchmark artifact for evaluating join order
optimization algorithms.  It keeps the workloads, scenario definitions, variant
parameters, and runner together so reviewer-facing benchmark results can be
explained and reproduced from one place.

The intended use is narrow: when proposing or reviewing a new join order
algorithm, run a named scenario, compare variants, and publish the resulting
tables separately in the community thread.  Local run artifacts are kept as
traceability evidence, not as files that should normally be committed here.

## For Reviewers

Start here when this repository is linked from a PostgreSQL community thread.
The thread or attachment should contain the result tables; this repository
documents how those tables were produced.

| Review question | Evidence in this repository |
| --- | --- |
| What was tested first? | `main` is the primary validation scenario.  It runs the full JOB and JOB-Complex workloads. |
| Which workloads are included? | [SCENARIOS.md](SCENARIOS.md) describes `main`, `extended`, and `full`; [DATASETS.md](DATASETS.md) records source repositories, related papers, query counts, join sizes, and data requirements. |
| Which algorithm variants were compared? | A submitted run should state its `--variants` list.  Baselines and other experiment-specific variants are described below. |
| What benchmark parameters were used? | The public run defaults are listed below.  The exact resolved values for a submitted run are in its `run.json`. |
| How were timings collected? | The measurement protocol below uses PostgreSQL `EXPLAIN ANALYZE` JSON output with planning and execution reported separately. |
| How were the uploaded tables produced? | [tools/render_review_tables.py](tools/render_review_tables.py) renders styled Excel workbooks with execution-time and planning-time sheets from `summary.csv`. |
| Where are the results? | Results are expected to be uploaded separately, for example as Excel/PDF tables.  If exact tracing is needed, inspect the uploaded `run.json` and `raw.csv` from the same run. |

The primary execution metric is `execution_ms_median`.  Planning time is a
separate diagnostic metric, so planner overhead does not get mixed into
execution behavior.

## Public Run Defaults

The built-in scenarios currently use the same run protocol unless the command
line overrides it:

| Parameter | Default |
| --- | --- |
| measured repetitions | `3` |
| statement timeout | `600000 ms` |
| warmup | `1` discarded warmup pass per query group |
| stabilization | runs `VACUUM (FREEZE, ANALYZE)` on prepared benchmark tables before measurement |
| variant order | rotated per query group |
| session settings | `join_collapse_limit=100`, `max_parallel_workers_per_gather=0`, `work_mem=1GB`, `effective_cache_size=8GB` |

Variant sets are experiment-specific.  A submitted result should state the
selected variants explicitly, and the exact definitions should come from the
submitted variants file or the run's `run.json`.

For orientation, [examples/variants.toml](examples/variants.toml) separates
portable baselines from patch-specific algorithms:

| Group | Role |
| --- | --- |
| `dp`, `geqo` | Baseline PostgreSQL join-search modes used for comparison.  These are the built-in scenario defaults. |
| Other variants | Experiment-specific algorithms or parameter sets, for example GOO or hybrid-search variants from a patched build. |

Exact example definitions live in [examples/variants.toml](examples/variants.toml).

## Minimal Reproduction

Requirements: Python 3.11+, `psql` in `PATH`, a reachable PostgreSQL instance,
and the IMDB CSV bundle for IMDB-backed workloads.

Prepare and run the primary validation scenario with portable baselines:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run main --variants dp,geqo
```

Run a patch-specific algorithm by passing the submitted variants file and an
explicit variant set:

```bash
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,geqo,my_algo
```

See [REPRODUCE.md](REPRODUCE.md) for the full command flow, connection flags,
resume behavior, and useful overrides.

## Scenarios

Scenarios are the public workload layers.  `main` is the first-line validation
run and contains the full JOB and JOB-Complex workloads.  The exact `main`,
`extended`, and `full` definitions are documented in [SCENARIOS.md](SCENARIOS.md).

## Variants

The default variant file is [examples/variants.toml](examples/variants.toml).
It contains portable baselines plus other example algorithms used by this
repository's experiments, not a fixed requirement for every submitted result.

Use a custom variant file when testing another algorithm or parameter set:

```bash
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,geqo,my_algo
```

Variant fields are documented in [config/README.md](config/README.md).

## Measurement Protocol

Default runs collect planning and execution metrics from PostgreSQL backend
phase times under:

```sql
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON, SETTINGS ON)
```

By default, the runner performs one discarded warmup pass per query group before
that query group's measured repetitions.  Warmup executions are not recorded in
`raw.csv` or `summary.csv`.

Measured `statement_timeout` rows are recorded as `status=timeout`.  Non-timeout
errors are recorded as `status=error`, and `--fail-on-error` makes the command
exit non-zero after writing result artifacts.

The harness applies session-level benchmark settings and records them in
`run.json`.  It does not modify cluster-level settings such as `shared_buffers`
or other restart-required PostgreSQL settings.

## Reviewer Tables

Use [tools/render_review_tables.py](tools/render_review_tables.py) to turn a
completed run into per-query tables suitable for community attachments.  Each
workbook contains an execution-time sheet and a planning-time sheet generated
from the same `summary.csv`.

The generated workbook groups metric columns and ratio columns, colors ratio
cells relative to `dp`, and includes a `SUM` row.  CSV
companions are written next to the workbook for plain-text inspection.

Example:

```bash
python3 tools/render_review_tables.py outputs/<run_id> \
  --dataset job \
  --variants dp,geqo,my_algo
```

## Outputs

Run outputs are local and ignored by git.  They are mainly for auditability and
for producing the separate result tables posted to the community thread.

Each `run` creates `outputs/<run_id>/` with `run.json`, `raw.csv`,
`summary.csv`, and rendered public-report files.  Keep `run.json` with the
published result set when exact scenario, parameter, and variant resolution
needs to be checked; use `raw.csv` when per-query measurements need to be
audited.  The full layout is documented in [REPRODUCE.md](REPRODUCE.md).

## More Detail

- [REPRODUCE.md](REPRODUCE.md): full reproduction workflow and CLI overrides
- [SCENARIOS.md](SCENARIOS.md): scenario layers
- [DATASETS.md](DATASETS.md): workload coverage, IMDB CSV setup, and query counts
- [config/README.md](config/README.md): scenario and variant file fields
