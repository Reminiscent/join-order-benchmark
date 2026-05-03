# Reproduce

This is the command checklist for reproducing a public benchmark run.  For the
runner protocol behind these commands, see [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).
For artifact formats and reviewer workbook details, see [OUTPUTS.md](OUTPUTS.md).

## Requirements

You need:

- a reachable PostgreSQL instance
- `psql` in `PATH`
- Python 3.11 or newer
- the external IMDB CSV bundle for IMDB-backed workloads
- a database role that can connect to the `postgres` maintenance database and
  create benchmark databases

Prepare and run do not require extra Python packages.  Rendering `review.xlsx`
requires `XlsxWriter`.

For the public protocol, configure PostgreSQL with `shared_buffers=4GB` before
running the benchmark:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

Restart PostgreSQL after changing `shared_buffers`.

If a local PostgreSQL build is reachable through normal libpq defaults, no
connection flags are needed.  If the server uses a TCP host, non-default port,
or different database role, pass the same connection flags to both phases:

```bash
python3 bench/bench.py prepare main --host 127.0.0.1 --port 5433 --user postgres --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run main --host 127.0.0.1 --port 5433 --user postgres --variants dp,geqo
```

Libpq environment variables such as `PGHOST`, `PGPORT`, `PGUSER`, and
`PGSERVICE` work too.  The harness still selects the database names itself.

## Discover

List the built-in benchmark surface:

```bash
python3 bench/bench.py list scenarios
python3 bench/bench.py list datasets
python3 bench/bench.py list variants
```

`main` is the primary public validation scenario.  `extended` adds smaller
planning-stress workloads.  `full` also adds the heavier CEB IMDB 3k workload.
Dataset details are in [WORKLOADS.md](WORKLOADS.md).

## Variants

The portable baseline variants `dp` and `geqo` are built in.  The CLI also
loads `examples/variants.toml` by default when that file exists.  Edit
`examples/variants.toml` directly to change the repository's default extra
variants.  Use `--variants-file` only to replace that default with another TOML
file.

Inspect variants from a different file:

```bash
python3 bench/bench.py list variants --variants-file path/to/variants.toml
```

Variant fields are documented in [examples/README.md](examples/README.md).

## Main Scenario

`main` runs the complete JOB and JOB-Complex workloads.

Prepare the data:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

If an already prepared database is present, `prepare` skips it.  Use
`--force-recreate` only when intentionally dropping and rebuilding the benchmark
database.

Run the portable baselines:

```bash
python3 bench/bench.py run main --variants dp,geqo
```

Run a patch-specific variant from the default extra variants file:

```bash
python3 bench/bench.py run main --variants dp,geqo,goo_cost
```

## Broader Scenarios

Run `extended` after `main` when broader planning/search-space coverage is
needed:

```bash
python3 bench/bench.py prepare extended --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run extended
```

Run `full` only for the complete campaign:

```bash
python3 bench/bench.py prepare full --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run full
```

In `extended` and `full`, `gpuqo_clique_small` runs non-`dp` variants on the
complete workload and limits `dp` to queries with at most 12 joins.

## Reviewer Workbook

Each run writes local artifacts under `outputs/<run_id>/`:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
```

Create the reviewer workbook from an existing run:

```bash
python3 -m pip install XlsxWriter
python3 tools/render_review_tables.py outputs/<run_id>
```

The script writes `outputs/<run_id>/review.xlsx`.  Workbook layout, `SUM` row
semantics, and ratio color rules are documented in [OUTPUTS.md](OUTPUTS.md).

## Common Run Options

| Option | Use |
| --- | --- |
| `--variants` | choose the variants and display/order them explicitly |
| `--variants-file` | use a different extra variant TOML file |
| `--resume-run-id` | continue an interrupted run from a safe boundary |
| `--statement-timeout-ms` | adjust the per-statement guardrail timeout |
| `--tag` | record a local build or patch label in `run.json` |

Resume an interrupted run by passing the output directory name:

```bash
python3 bench/bench.py run main \
  --variants dp,geqo,goo_cost \
  --resume-run-id 20260412_142110_777847_main
```

Use the same scenario, variant list, extra variants file if overridden,
connection flags, tag, and statement timeout as the original run.  Timeout and
resume semantics are described in [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).
