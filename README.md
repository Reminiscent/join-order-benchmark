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

## Usage (TODO)

A fuller end-to-end usage section will be added later, including:

- repeatable dataset bootstrap helpers
- batch execution scripts
- plan capture conventions (`EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)`)
- unified result collection format
