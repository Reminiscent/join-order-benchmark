# Join Order Benchmark Collection (PostgreSQL-focused)

This repository curates SQL workloads for **join order optimization** benchmarking, with PostgreSQL as the primary target engine.

## Goal

The goal is to keep a practical, test-oriented corpus for evaluating:

- join enumeration quality
- planner stability under many-table joins
- sensitivity to predicate complexity and data correlation
- differences across benchmark families that share similar schemas

## Standardized Layout

Where possible, each benchmark now follows the same execution contract:

- `schema.sql` - create tables
- `load.sql` - load/import data
- `queries/` (or `*.sql` query folders) - executable SELECT statements
- `README.md` - source, structure, query characteristics, run notes

## Included Workloads

### 1) `join-order-benchmark/`

- Source: JOB (Leis et al., PVLDB 2015)
- Size: 113 queries
- Typical join width: 4 to 17 tables
- Strength: classic IMDB multi-join baseline

### 2) `JOB-Complex/`

- Source: JOB-Complex (AIDB@VLDB 2025)
- Size: 30 queries
- Typical join width: 6 to 16 tables
- Strength: harder predicates, non-PK/FK and string-based joins

### 3) `imdb_pg_dataset/`

IMDB-derived suites sharing one schema/load path:

- `ceb-imdb-3k/` (Flow-Loss/CEB): 3,133 queries

### 4) `sqlite/`

- Source: SQLite `select5.test` (converted to PostgreSQL SQL)
- Size: 732 queries
- Join width: 4 to 64 tables
- Strength: high-width join stress without external data files

### 5) `postgres-gpuqo/`

- Source: gpuqo synthetic workload generators (SIGMOD 2022 project)
- Kept subsets: `chain-small`, `clique-small`, `star-small`, `snowflake-small`
- Strength: controllable synthetic join graph shapes

## Query Metadata (`join_size`)

Join-order algorithms diverge most when the join search space is large. To make this explicit, the repo provides a
global query manifest with `join_size` (number of relations in the top-level `FROM` clause):

- `meta/query_manifest.csv`
- Regenerate with: `python3 tools/build_query_manifest.py --verify --summary`

## Large-Join First Guidance

If the goal is to stress join enumeration and join-order selection (instead of spending time on many small joins),
start with:

- `sqlite_select5` with `join_size >= 20` (lots of 20..64-way joins, self-contained)
- `gpuqo_snowflake_small` with `join_size >= 20` (controllable synthetic schema with 20..40-way joins)
- `job` and `job_complex` filtered to `join_size >= 12` (realistic IMDB-schema sanity checks; max join size <= 17/16)

`meta/query_manifest.csv` makes it easy to filter/stratify by join size and avoid accidentally mixing in lots of
small joins when comparing DP vs GEQO-threshold behavior.

## Why Some Suites Were Removed

- Redundant: a duplicate copy of JOB under `imdb_pg_dataset/job/` was removed (same queries, different formatting).
- Not large-join-extending: suites like JOB-D and CEB-13k add many queries but do not increase max join size.
- Repo usability: removing huge directories reduces checkout size and makes analysis/benchmark iteration practical.
- Recoverability: removed suites remain available via git history (and their upstream sources) if needed later.

## Quick Start (PostgreSQL)

```bash
# Example: JOB
createdb job_demo
psql -d job_demo -f join-order-benchmark/schema.sql
psql -d job_demo -v csv_dir=/absolute/path/to/imdb_csv -f join-order-benchmark/load.sql
psql -d job_demo -f join-order-benchmark/queries/1a.sql
```

```bash
# Example: converted SQLite select5 (self-contained)
createdb select5_demo
psql -d select5_demo -f sqlite/select5.test
```

## Benchmark Harness (GUC-driven, minimal v0)

This repository includes a minimal harness for running the same workload under multiple **GUC sets** and collecting:

- `planning_ms` from `EXPLAIN (SUMMARY)`
- `total_ms` from `psql \timing`
- `execution_ms = total_ms - planning_ms`

Results are written to `results/<run_id>/{raw.csv,summary.csv,run.json}`.

For an end-to-end MVP walkthrough (prepare + smoke run across all datasets, including host/port connection params),
see `docs/mvp_run.md`.

### Example: `sqlite_select5`

```bash
python3 bench/bench.py prepare sqlite_select5 select5_bench

python3 bench/bench.py run sqlite_select5 select5_bench \
  --algo dp:geqo=off \
  --algo geqo:geqo=on,geqo_threshold=2
```

### Example: JOB (requires IMDB CSVs)

```bash
python3 bench/bench.py prepare job job_bench --csv-dir /absolute/path/to/imdb_csv

python3 bench/bench.py run job job_bench \
  --algo dp:geqo=off \
  --algo geqo:geqo=on,geqo_threshold=2 \
  --min-join 12

python3 bench/bench.py smoke job job_bench \
  --algo dp:geqo=off \
  --algo geqo:geqo=on,geqo_threshold=2
```

Notes:
- `join_size` is taken from `meta/query_manifest.csv` (regenerate via `python3 tools/build_query_manifest.py --verify --summary`).
- Defaults: `run` uses 3 repetitions and no query filter unless you pass `--min-join`.
- `smoke` is a lightweight check (`--queries 1`, `reps=1`, `stabilize=none`).
- `--dedupe-sql` removes exact duplicate SQLs by manifest `sql_sha1`.
- `--query-id-file` lets you run a curated subset (one `query_id` per line).

### IMDB CEB-3k Subsets

To make `imdb_ceb_3k` easier to run, prebuilt subsets are available under
`meta/subsets/imdb_ceb_3k/` (stratified and join-size buckets).

Regenerate subset files with:

```bash
python3 tools/build_imdb_ceb_subsets.py
```
