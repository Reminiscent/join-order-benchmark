# Join Order Benchmark Collection

This repository is a PostgreSQL benchmark artifact for evaluating join order
optimization algorithms.  It keeps the workloads, scenario definitions, variant
settings, and runner together so reviewer-facing benchmark results can be
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
| Which broader workloads are included? | `extended` adds small-data planning/search-space stress workloads; `full` adds the heavier CEB IMDB 3k subset.  See [WORKLOADS.md](WORKLOADS.md). |
| Which algorithm variants were compared? | Built-in baselines are `dp` and `geqo`.  Other variants are patch-specific algorithms or parameter sets supplied through an optional `--variants-file` and explicit `--variants` list. |
| How was the benchmark run? | [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md) describes the public run protocol: prepare data, stabilize tables, warm up, measure, handle timeouts, and write artifacts. |
| How can I reproduce it? | [REPRODUCE.md](REPRODUCE.md) is the command-oriented reproduction guide. |

## Cluster Setup

The concrete run sequence is documented in [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).
For public runs, the only PostgreSQL setting that must be applied outside the
harness is `shared_buffers=4GB`, because it requires a server restart:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

The benchmark is intended to run on a machine with at least 16 GiB of RAM.  The
harness applies the remaining session-level PostgreSQL settings before every
warmup and measured execution; see [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md) for
the exact session prelude and rationale.

## Minimal Reproduction

Requirements: Python 3.11+, `psql` in `PATH`, a reachable PostgreSQL instance,
a database role that can create the benchmark databases, and the IMDB CSV bundle
for IMDB-backed workloads.  If following the public `shared_buffers=4GB` setup,
the PostgreSQL server must be configured and restarted before the measured run.

Prepare and run the primary validation scenario with portable baselines:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run main --variants dp,geqo
```

Run a patch-specific algorithm by passing an extra variants file and an explicit
variant set:

```bash
python3 bench/bench.py run main \
  --variants-file path/to/variants.toml \
  --variants dp,geqo,my_algo
```

See [REPRODUCE.md](REPRODUCE.md) for the full command flow, connection flags,
resume behavior, and supported CLI options.

## Artifacts

Run outputs are local and ignored by git.  They are mainly for auditability and
for producing the separate result tables posted to the community thread.

Each `run` creates `outputs/<run_id>/` with `run.json`, `raw.csv`, and
`summary.csv`.  A reviewer-facing `review.xlsx` workbook can be rendered from
`summary.csv` with [tools/render_review_tables.py](tools/render_review_tables.py).
The full artifact layout and table format are documented in [OUTPUTS.md](OUTPUTS.md).

## Repository Layout

Top-level folders are split by responsibility:

| Path | Purpose |
| --- | --- |
| `bench/` | benchmark CLI and harness modules for prepare, run, timing collection, and result summarization |
| `examples/` | example extra variant definitions for patch-specific algorithms |
| `tools/` | query manifest plus helper scripts for refreshing metadata and rendering reviewer-facing tables |
| `tests/` | unit tests for built-in scenario resolution, run behavior, and reviewer table rendering |
| `join-order-benchmark/` | local JOB workload adaptation |
| `JOB-Complex/` | local JOB-Complex workload adaptation |
| `imdb_pg_dataset/` | IMDB-backed schema, load scripts, and CEB IMDB 3k query subset |
| `sqlite/` | local SQLite select5-derived workload adaptation |
| `postgres-gpuqo/` | local GPUQO-derived synthetic workload adaptations |
| `data/` | ignored local input data directory, commonly used for the external IMDB CSV bundle |
| `outputs/` | ignored local benchmark output directory |

## More Detail

- [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md): run protocol and timing semantics
- [REPRODUCE.md](REPRODUCE.md): command-oriented reproduction workflow
- [WORKLOADS.md](WORKLOADS.md): scenario layers, workload coverage, IMDB CSV setup, and query counts
- [OUTPUTS.md](OUTPUTS.md): run artifacts and reviewer tables
- [examples/README.md](examples/README.md): extra variant file fields
- [bench/README.md](bench/README.md): benchmark harness module layout
- [tools/README.md](tools/README.md): query manifest and helper commands
