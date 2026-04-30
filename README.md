# Join Order Benchmark Collection

This repository is a PostgreSQL benchmark artifact for evaluating join order
optimization algorithms.  It keeps the workloads, scenario definitions, variant
parameters, runner, and generated reports together so reviewer-facing benchmark
results can be inspected and reproduced from one place.

The intended use is narrow: when proposing or reviewing a new join order
algorithm, run a named scenario, compare variants, and cite the generated
`public_report.md`, `summary.csv`, and `run.json`.

## For Reviewers

Start here when this repository is linked from a PostgreSQL community thread:

| Question | Where to look |
| --- | --- |
| What was the main validation run? | `main`, defined in [config/scenarios.toml](config/scenarios.toml) |
| What workloads were used? | [DATASETS.md](DATASETS.md) |
| What algorithm variants were compared? | [examples/variants.toml](examples/variants.toml), or the submitted custom variants file |
| What result should be read first? | `outputs/<run_id>/public_report.md` |
| What is the per-query source of truth? | `outputs/<run_id>/raw.csv` |
| What is the compact comparison table? | `outputs/<run_id>/summary.csv` |
| What exact scenario and parameters were resolved? | `outputs/<run_id>/run.json` |

The primary execution metric is `execution_ms_median`.  Planning time is reported
separately so planner overhead does not get mixed into execution behavior.

## Minimal Reproduction

Requirements: Python 3.11+, `psql` in `PATH`, a reachable PostgreSQL instance,
and the IMDB CSV bundle for IMDB-backed workloads.

Prepare and run the primary validation scenario:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run main
```

Run with an explicit variant set:

```bash
python3 bench/bench.py run main --variants dp,geqo,hybrid_search
```

See [REPRODUCE.md](REPRODUCE.md) for the full command flow, connection flags,
resume behavior, and useful overrides.

## Scenarios

Scenarios are the public workload layers.  They are defined in
[config/scenarios.toml](config/scenarios.toml).

| Scenario | Purpose | Workload scope |
| --- | --- | --- |
| `main` | First-line validation for a new algorithm | full `job` and `job_complex` |
| `extended` | Broader planning/search-space validation after `main` looks good | `main` plus self-contained stress workloads, excluding `imdb_ceb_3k` |
| `full` | Complete built-in campaign | `extended` plus `imdb_ceb_3k` |

The extra workloads in `extended` are converted from existing upstream
benchmarks and contain small self-contained data.  They are most useful for
planning time and join-search stress because they include many wide join queries.
`full` adds `imdb_ceb_3k`, which has much higher query volume and can dominate
campaign time.

In `extended` and `full`, `gpuqo_clique_small` has one tractability guard:
`geqo` and `hybrid_search` run the full dataset, while `dp` is limited to
`join_size <= 12`.

## Variants

The default variant file is [examples/variants.toml](examples/variants.toml).
It is an example of the algorithms used by this repository's experiments, not a
fixed requirement for every user.

Use a custom variant file when testing another algorithm or parameter set:

```bash
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,my_algo
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

## Outputs

Every `run` writes a self-contained result directory under `outputs/<run_id>/`:

| File | Role |
| --- | --- |
| `run.json` | resolved scenario, protocol settings, variants, datasets, tag, failures, and progress state |
| `raw.csv` | source-of-truth per-query, per-variant, per-repetition measurement log |
| `summary.csv` | aggregated comparison table derived from successful measured repetitions |
| `public_report.md` | reviewer-facing report with separate execution and planning sections |
| `public_report.json` | machine-readable form of the public report |

Re-render an existing report with:

```bash
python3 tools/render_public_reports.py outputs/<run_id>
```

## More Detail

- [REPRODUCE.md](REPRODUCE.md): full reproduction workflow and CLI overrides
- [DATASETS.md](DATASETS.md): workload coverage, IMDB CSV setup, and query counts
- [config/README.md](config/README.md): scenario and variant file fields
