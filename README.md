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
| What was tested first? | `main` is the primary validation scenario.  It runs the complete JOB and JOB-Complex workloads. |
| Which workloads are included? | [SCENARIOS.md](SCENARIOS.md) describes `main`, `extended`, and `full`; [DATASETS.md](DATASETS.md) records source repositories, related papers, query counts, join sizes, and data requirements. |
| Which algorithm variants were compared? | Built-in baselines are `dp` and `geqo`.  Other variants are patch-specific algorithms or parameter sets supplied through an explicit `--variants` list and variants file. |
| How was the benchmark run? | [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md) maps the public commands to the runner steps: prepare data, stabilize tables, warm up, measure, and write artifacts. |
| What benchmark parameters were used? | Most PostgreSQL settings remain whatever the server was started with.  The harness applies only the session-level benchmark controls listed below; exact resolved values for a submitted run are in its `run.json`. |
| How were timings collected? | PostgreSQL `EXPLAIN ANALYZE` JSON output is used because it reports planning and execution phase times separately.  Node-level timing is disabled with `TIMING OFF`. |

The primary execution metric is `execution_ms_median`.  Planning time is a
separate diagnostic metric, so planner overhead does not get mixed into
execution behavior.

## Public Run Defaults

The built-in scenarios use the same public protocol unless the command line
overrides it.  The goal is to make join-order behavior comparable while keeping
execution-side noise low.  Cluster-level settings such as `shared_buffers` are
not changed by the harness.

| Setting | Default | Purpose |
| --- | --- | --- |
| measured repetitions | `3` | compute per-query medians without making public runs too long |
| statement timeout | `600000 ms` | bound pathological plans and record them as timeouts |
| warmup | `1` discarded warmup pass per query group | reduce first-run effects before recorded repetitions |
| stabilization | `vacuum_freeze_analyze` | run `VACUUM FREEZE ANALYZE` and a best-effort `CHECKPOINT` before measurement |
| variant order | `rotate` | avoid always giving the same variant the same position in a query group |
| `join_collapse_limit` | `100` | allow the join-order algorithm under test to see wide join search spaces |
| `max_parallel_workers_per_gather` | `0` | reduce execution-time noise from parallel workers |
| `work_mem` | `1GB` | avoid memory spill differences dominating join-order comparisons |
| `effective_cache_size` | `8GB` | keep costing assumptions stable across public runs |

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
run and contains the complete JOB and JOB-Complex workloads.  The exact `main`,
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

`TIMING OFF` disables per-plan-node timing while PostgreSQL still reports
top-level planning and execution summary times.  The detailed rationale is in
[BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).

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

The generated workbook requires `dp` in the selected variant list, groups metric
columns and ratio columns, colors ratio cells relative to `dp`, and includes a
`SUM` row.  CSV companions are written next to the workbook for plain-text
inspection.

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
audited.  The full layout is documented in [OUTPUTS.md](OUTPUTS.md).

## More Detail

- [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md): how the benchmark scripts execute a run
- [REPRODUCE.md](REPRODUCE.md): full reproduction workflow and CLI overrides
- [SCENARIOS.md](SCENARIOS.md): scenario layers
- [DATASETS.md](DATASETS.md): workload coverage, IMDB CSV setup, and query counts
- [OUTPUTS.md](OUTPUTS.md): run artifacts, public reports, and reviewer tables
- [config/README.md](config/README.md): scenario and variant file fields
