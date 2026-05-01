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
| Which broader workloads are included? | `extended` adds small-data planning/search-space stress workloads; `full` adds the heavier CEB IMDB 3k subset.  See [SCENARIOS.md](SCENARIOS.md) and [DATASETS.md](DATASETS.md). |
| Which algorithm variants were compared? | Built-in baselines are `dp` and `geqo`.  Other variants are patch-specific algorithms or parameter sets supplied through an explicit `--variants` list and variants file. |
| How was the benchmark run? | [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md) maps the public commands to the runner steps: prepare data, stabilize tables, warm up, measure, and write artifacts. |
| What run settings were used? | The public run protocol and PostgreSQL settings are listed below.  `run.json` records the exact scenario, variants, dataset filters, repetition count, timeout, warmup policy, and session GUCs used by a run. |
| How were timings collected? | PostgreSQL `EXPLAIN ANALYZE` JSON output is used to separate planning and execution time.  `TIMING OFF` reduces node-level timing overhead; caveats are in [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md). |

The primary execution metric is `execution_ms_median`.  Planning time is a
separate diagnostic metric, so planner overhead does not get mixed into
execution behavior.

## Public Run Settings

There are two kinds of settings:

- **Run protocol settings** are benchmark-harness behavior: how many repetitions
  are measured, how warmup works, how timeouts are recorded, and how variants
  are ordered.
- **PostgreSQL settings** are GUCs or server settings that affect planning and
  execution.  Session-level GUCs are applied by the harness; restart-required
  settings must be configured before the run.

### Run Protocol

| Setting | Public default | Purpose |
| --- | --- | --- |
| measured repetitions | `3` | compute per-query medians without making public runs too long |
| statement timeout | `600000 ms` | bound pathological plans and record them as timeouts |
| warmup | `1` discarded warmup pass per query group | reduce first-run effects before recorded repetitions |
| stabilization | `vacuum_freeze_analyze` | run `VACUUM FREEZE ANALYZE` and a best-effort `CHECKPOINT` before measurement |
| variant order | `rotate` | avoid always giving the same variant the same position in a query group |

### PostgreSQL Settings

The public defaults are chosen so the benchmark can run on a machine with at
least 16 GiB of RAM without tuning memory values per machine.

| Setting | Public default | Applied by | Purpose |
| --- | --- | --- | --- |
| `shared_buffers` | `4GB` | user before run, requires restart | cluster-level buffer pool baseline, about 25% of the 16 GiB minimum |
| `join_collapse_limit` | `100` | harness session GUC | allow the join-order algorithm under test to see wide join search spaces |
| `max_parallel_workers_per_gather` | `0` | harness session GUC | reduce execution-time noise from parallel workers |
| `work_mem` | `1GB` | harness session GUC | reduce spill noise for single-query serial benchmark runs |
| `effective_cache_size` | `8GB` | harness session GUC | keep planner cache-size assumptions stable for the 16 GiB baseline |

If following the public setup, set `shared_buffers` outside the harness and
restart PostgreSQL:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

The session GUCs are applied by the harness for every warmup and measured
execution and are recorded in `run.json`.  See
[BENCHMARK_RUNS.md](BENCHMARK_RUNS.md) for the memory-setting rationale and
caveats.

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

## Artifacts

Run outputs are local and ignored by git.  They are mainly for auditability and
for producing the separate result tables posted to the community thread.

Each `run` creates `outputs/<run_id>/` with `run.json`, `raw.csv`,
`summary.csv`, and rendered public-report files.  Reviewer-facing Excel/CSV
tables can be rendered from `summary.csv` with
[tools/render_review_tables.py](tools/render_review_tables.py).  The full
artifact layout and table format are documented in [OUTPUTS.md](OUTPUTS.md).

## Repository Layout

Top-level folders are split by responsibility:

| Path | Purpose |
| --- | --- |
| `bench/` | benchmark CLI and harness modules for prepare, run, timing collection, and result summarization |
| `config/` | built-in scenario definitions |
| `examples/` | default and example variant definitions, including portable `dp` and `geqo` baselines |
| `meta/` | generated query manifest used for dataset metadata such as join size and query count |
| `tools/` | helper scripts for refreshing metadata and rendering reviewer-facing reports or tables |
| `tests/` | unit tests for scenario parsing, run behavior, and reviewer table rendering |
| `join-order-benchmark/` | local JOB workload adaptation |
| `JOB-Complex/` | local JOB-Complex workload adaptation |
| `imdb_pg_dataset/` | IMDB-backed schema, load scripts, and CEB IMDB 3k query subset |
| `sqlite/` | local SQLite select5-derived workload adaptation |
| `postgres-gpuqo/` | local GPUQO-derived synthetic workload adaptations |
| `data/` | ignored local input data directory, commonly used for the external IMDB CSV bundle |
| `outputs/` | ignored local benchmark output directory |

## More Detail

- [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md): how the benchmark scripts execute a run
- [REPRODUCE.md](REPRODUCE.md): full reproduction workflow and CLI overrides
- [SCENARIOS.md](SCENARIOS.md): scenario layers
- [DATASETS.md](DATASETS.md): workload coverage, IMDB CSV setup, and query counts
- [OUTPUTS.md](OUTPUTS.md): run artifacts, public reports, and reviewer tables
- [config/README.md](config/README.md): scenario and variant file fields
- [bench/README.md](bench/README.md): benchmark harness module layout
