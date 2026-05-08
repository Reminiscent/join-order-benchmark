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

Explicit `--host`, `--port`, and `--user` values are passed to libpq.  If those
flags are omitted, libpq uses its normal environment variables and defaults.
The harness still selects the database names itself.

## Discover

List the built-in benchmark surface:

```bash
python3 bench/bench.py list scenarios
python3 bench/bench.py list datasets
python3 bench/bench.py list variants
```

`main` is the primary public validation scenario.  `extended` adds the heavier
CEB IMDB 3k workload.  `planning` contains self-contained synthetic wide-join
workloads for planning/search-space checks.
Dataset details are in [WORKLOADS.md](WORKLOADS.md).

## Configuration Files

`examples/benchmark_settings.toml` defines shared PostgreSQL session GUCs used
by every variant.  Keep run-protocol settings there when they should stay
identical across algorithms or parameter sweeps.

`examples/variants.toml` defines extra variants.  A variant is one named run
configuration: a label plus algorithm-specific session GUCs.  It can represent
a different join-order algorithm, or the same algorithm with different
parameters.  The portable baseline variants `dp` and `geqo` are built in.

Inspect variants from a different file:

```bash
python3 bench/bench.py list variants --variants-file path/to/variants.toml
```

Both file formats and defaults are documented in
[examples/README.md](examples/README.md).  The runner validates configured GUCs
before refreshing statistics or executing measured SQL.

## Main Scenario

`main` runs the complete JOB and JOB-Complex workloads.

Prepare the data:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

`prepare` always recreates the scenario's benchmark databases.  If you want to
reuse existing data, skip `prepare` and run the benchmark directly.

Run the portable baselines:

```bash
python3 bench/bench.py run main --variants dp,geqo
```

Run a patch-specific variant from the default extra variants file:

```bash
python3 bench/bench.py run main --variants dp,geqo,goo_cost
```

## Broader Scenarios

Run `extended` after `main` when broader IMDB-backed coverage is needed:

```bash
python3 bench/bench.py prepare extended --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run extended
```

Run `planning` separately for synthetic wide-join planning checks:

```bash
python3 bench/bench.py prepare planning
python3 bench/bench.py run planning --variants geqo,goo_combined
```

All scenarios share the same default variants.  Use `--variants` to choose the
algorithms or parameter settings for a run; for `planning`, omitting `dp` is
usually more useful because large synthetic joins make dynamic programming very
slow.

## Join-Size Filter

`--min-join N` selects only queries whose manifest `join_size >= N`.  Here
`join_size` is the number of base relations in the query's flat `FROM` list,
recorded in [tools/query_manifest.csv](tools/query_manifest.csv).  For example,
`--min-join 12` focuses a run on queries involving at least 12 joined tables.

## Reusing Statistics

By default, each `run` refreshes table statistics once per distinct database
with `VACUUM FREEZE ANALYZE` and a best-effort `CHECKPOINT`.

When comparing separate runs that only change algorithm parameters, run one
baseline normally, then pass `--reuse-stats` to later runs so they reuse the
existing statistics snapshot:

```bash
python3 bench/bench.py run extended --variants my_algo_p1 --min-join 12 --tag p1
python3 bench/bench.py run extended --variants my_algo_p2 --min-join 12 --reuse-stats --tag p2
```

Do not run `prepare`, recreate data, or run `ANALYZE` between runs if the
comparison depends on stable statistics.

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
Ratio columns compare non-reference variants to `dp` and/or `geqo` when those
variants are selected.

## Common Run Options

| Option | Use |
| --- | --- |
| `--variants` | choose the variants and display/order them explicitly |
| `--variants-file` | use a different extra variant TOML file |
| `--min-join` | run only queries with manifest `join_size >= N` |
| `--reuse-stats` | reuse existing database statistics instead of refreshing them |
| `--tag` | record a local build or patch label in `run.json` |

Timeout handling is described in [BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).
