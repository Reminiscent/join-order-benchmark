# Reproduce

This document describes the public reproduction flow for this benchmark
repository.

This is a command-oriented guide.  For how the runner performs stabilization,
warmup, variant rotation, timeout handling, and timing collection, see
[BENCHMARK_RUNS.md](BENCHMARK_RUNS.md).

## 1. Requirements

You need:

- a reachable PostgreSQL instance
- `psql` in `PATH`
- Python 3.11 or newer
- the external IMDB CSV bundle when preparing IMDB-backed datasets
- a database role that can connect to the `postgres` maintenance database and
  create the benchmark databases used by `prepare`

The documented reproduction path is Python 3.11+ only.  No extra Python package
is required for TOML parsing on supported Python versions.

Following the public `shared_buffers=4GB` setup also requires either superuser
access for `ALTER SYSTEM` or direct access to the PostgreSQL server
configuration, followed by a server restart.

`prepare` and `run` both connect to PostgreSQL.  The harness does not define
its own connection defaults: `--host`, `--port`, and `--user` are optional
pass-through flags to `psql`.  If they are omitted, they are not passed.

Use the same connection flags for both phases when the server is not reachable
through the native PostgreSQL defaults:

| Flag | If omitted |
| --- | --- |
| `--host` | libpq uses its normal local connection behavior, usually a Unix-domain socket on Unix-like systems |
| `--port` | libpq uses PostgreSQL's default port, usually `5432`, unless overridden by the environment |
| `--user` | libpq uses the current operating-system user, unless `PGUSER` is set |

This matches a direct local PostgreSQL source build started with default
connection settings.  If your server listens on TCP, a non-default port, or a
different database role, pass the corresponding values explicitly.

If you prefer one shared connection configuration, use libpq's native
environment variables or service files, for example `PGHOST`, `PGPORT`,
`PGUSER`, `PGPASSWORD`, `PGSSLMODE`, or `PGSERVICE`.  The harness still owns the
database name because it switches between the `postgres` maintenance database
and the benchmark databases during prepare and run.

## 2. Variants

The portable baseline variants `dp` and `geqo` are built in.  Extra variants are
only needed when testing a patch-specific algorithm or parameter set.

Inspect the active variants:

```bash
python3 bench/bench.py list variants
```

Inspect built-ins plus extra example variants:

```bash
python3 bench/bench.py list variants --variants-file examples/variants.toml
```

If `--variants` is omitted, the selected scenario uses the built-in `dp` and
`geqo` baselines.  Variant fields are documented in
[examples/README.md](examples/README.md).

## 3. Data Setup

Dataset details and IMDB CSV setup are documented in [WORKLOADS.md](WORKLOADS.md).

Typical IMDB CSV setup:

```bash
mkdir -p data/imdb_csv
tar -xzf imdb.tgz -C data/imdb_csv
```

## 4. Discover What Exists

List scenarios:

```bash
python3 bench/bench.py list scenarios
```

List datasets:

```bash
python3 bench/bench.py list datasets
```

List variants:

```bash
python3 bench/bench.py list variants
```

## 5. Main Run

`main` is the primary public validation path for a new join-order algorithm.  It
runs the complete `job` and `job_complex` workloads.

Prepare:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

Run with portable baselines:

```bash
python3 bench/bench.py run main --variants dp,geqo
```

Run with an extra variant set:

```bash
python3 bench/bench.py run main \
  --variants-file path/to/variants.toml \
  --variants dp,geqo,my_algo
```

## 6. Extended And Full Runs

`extended` is the broad validation layer after `main`.  It adds self-contained
stress workloads converted from SQLite and GPUQO sources.  These workloads have
small local data and many wide joins, so they are most useful for planning time
and join-search stress rather than realistic execution-time claims.

Prepare and run:

```bash
python3 bench/bench.py prepare extended --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run extended
```

`full` is the complete built-in campaign.  It runs `extended` plus
`imdb_ceb_3k`, which has much higher query volume and can dominate campaign
time.

Prepare and run:

```bash
python3 bench/bench.py prepare full --csv-dir "$(pwd)/data/imdb_csv"
python3 bench/bench.py run full
```

In `extended` and `full`, `gpuqo_clique_small` keeps non-`dp` variants on the
complete 150-query dataset while limiting `dp` to `join_size <= 12`.

## 7. Output Layout

Every `run` creates local artifacts under `outputs/<run_id>/`:

```text
outputs/<run_id>/
  run.json
  raw.csv
  summary.csv
```

The detailed artifact contract, column meanings, console output, and
reviewer-table examples are documented in [OUTPUTS.md](OUTPUTS.md).

## 8. Reviewer Tables

Use `tools/render_review_tables.py` to create per-query tables for community
attachments from an existing `outputs/<run_id>/` directory.  The script writes a
styled XLSX workbook and CSV companion files.

Default table export:

```bash
python3 tools/render_review_tables.py outputs/<run_id>
```

The command renders the datasets and variants recorded in `run.json`.  The
workbook contains one execution-time sheet and one planning-time sheet, with
`dataset` as the first table column.
Execution time is the primary result; planning time is reported separately as a
diagnostic.

The exact workbook layout, CSV headers, `SUM` row semantics, and ratio color
rules are documented in [OUTPUTS.md](OUTPUTS.md).

## 9. CLI Options

These are the supported `run` options after choosing a scenario.  They select
what to compare, where to resume, and how to label the local run.  They do not
change the fixed benchmark protocol, except for the per-statement timeout
guardrail.

| Option | Use |
| --- | --- |
| `--variants-file` | add patch-specific variants from a TOML file |
| `--variants` | choose the variants and display/order them explicitly |
| `--resume-run-id` | continue an interrupted run from a safe boundary |
| `--statement-timeout-ms` | adjust the guardrail timeout for very slow or very fast machines |
| `--tag` | record a local build or patch label in `run.json` |
| `--fail-on-error` | exit non-zero on non-timeout query errors |

Resume an interrupted run by passing the output directory name:

```bash
python3 bench/bench.py run main \
  --variants dp,geqo,my_algo \
  --variants-file path/to/variants.toml \
  --resume-run-id 20260412_142110_777847_main
```

Use the same scenario, variant list, variants file, connection flags, tag, and
statement timeout as the original run.  The resume id is the directory name
under `outputs/`.  The harness validates the run context and continues from the
next unfinished safe group boundary.

`--statement-timeout-ms` is not an algorithm knob.  It only limits how long a
single bad plan can occupy the run.  If it differs from the default
`600000 ms`, include that value with the published result tables; the run also
records it in `run.json`.

If an exact `(dataset, query, variant)` hits `statement_timeout` during warmup,
the harness records later measured repetitions for that same combination as
skipped timeout rows instead of re-running them.

`--resume-run-id` resumes an existing `outputs/<run_id>/` directory from the
next unfinished safe group boundary.  The harness checkpoints after complete
warmup groups and complete measured groups, so resume never continues from the
middle of a query-group's variant set.

`--tag` labels the local build or patch under test, for example
`--tag pg18_patch_v4`.
