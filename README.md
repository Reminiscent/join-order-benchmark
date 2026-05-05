# Join Order Benchmark Collection

This repository is a PostgreSQL benchmark artifact for evaluating join order
optimization algorithms.  It keeps the workloads, scenario definitions, variant
settings, and runner together so public benchmark tables can be explained and
reproduced from one place.

Local run artifacts are traceability evidence and are ignored by git.  The
reviewer-facing result tables should be attached separately to the community
discussion.

## Start Here

| Need | Read |
| --- | --- |
| Reproduce a run | [REPRODUCE.md](REPRODUCE.md) |
| Understand the run protocol | [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md) |
| Check workload coverage | [WORKLOADS.md](WORKLOADS.md) |
| Inspect output files and `review.xlsx` | [OUTPUTS.md](OUTPUTS.md) |
| Read the Python harness | [bench/README.md](bench/README.md) |

## Scenarios

| Scenario | Purpose |
| --- | --- |
| `main` | Primary validation path on the complete JOB and JOB-Complex workloads. |
| `extended` | Adds the heavier CEB IMDB 3k workload to `main`. |
| `planning` | Self-contained synthetic planning/search-space stress workloads. |

## Quick Run

Requirements: Python 3.11+, `psql` in `PATH`, a reachable PostgreSQL instance,
a database role that can create benchmark databases, and the IMDB CSV bundle
for IMDB-backed workloads.  Public runs also configure `shared_buffers=4GB`
before measurement; see [REPRODUCE.md](REPRODUCE.md) and
[BENCHMARK_RUNS.md](BENCHMARK_RUNS.md) for the full checklist.

Prepare and run the primary scenario with portable baselines:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run main --variants dp,geqo
```

The built-in baselines are `dp` and `geqo`.  The CLI also loads
`examples/variants.toml` by default when that file exists; edit that file to
change the repository's default extra variants.

To focus a scenario on larger joins, add a manifest join-size lower bound:

```bash
python3 bench/bench.py run extended --variants geqo,goo_combined --min-join 12
```

## Outputs

Each `run` writes local artifacts under `outputs/<run_id>/`:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
```

Install `XlsxWriter` if needed, then create the reviewer workbook:

```bash
python3 -m pip install XlsxWriter
python3 tools/render_review_tables.py outputs/<run_id>
```

The script writes `outputs/<run_id>/review.xlsx`.  Artifact fields, workbook
layout, and ratio color rules are documented in [OUTPUTS.md](OUTPUTS.md).

## Repository Map

| Area | Purpose |
| --- | --- |
| `bench/` | benchmark CLI and Python harness |
| `examples/` | default extra variant definitions |
| `tools/` | query manifest and reviewer-table helpers |
| `tests/` | harness and reviewer-table tests |
| workload directories | JOB, JOB-Complex, CEB IMDB 3k, SQLite select5, and GPUQO-derived workloads |
| `data/` | ignored local input data, commonly the external IMDB CSV bundle |
| `outputs/` | ignored local benchmark output directories |
