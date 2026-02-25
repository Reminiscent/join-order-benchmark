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

- `job/` (JOB): 113 queries
- `job_extended/` (Neo): 24 queries
- `job_d/` (HybridQO): 20,000 queries
- `ceb-imdb-3k/` (Flow-Loss/CEB): 3,133 queries
- `ceb-imdb-13k/` (Flow-Loss/CEB full): 13,646 queries

### 4) `sqlite/`

- Source: SQLite `select5.test` (converted to PostgreSQL SQL)
- Size: 732 queries
- Join width: 4 to 64 tables
- Strength: high-width join stress without external data files

### 5) `postgres-gpuqo/`

- Source: gpuqo synthetic workload generators (SIGMOD 2022 project)
- Kept subsets: `chain-small`, `clique-small`, `star-small`, `snowflake-small`
- Strength: controllable synthetic join graph shapes

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
