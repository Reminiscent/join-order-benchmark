# IMDB-Derived Query Suites

This directory collects multiple IMDB-based SQL benchmark suites used in join-order and cardinality-estimation research.

## Purpose

Use these workloads to compare optimizer behavior across related but differently distributed query families while keeping one shared schema and data-loading path.

## Shared Execution Layout

- `schema.sql`: imports the shared IMDB schema
- `load.sql`: imports shared CSV loading script
- Query suites live in subdirectories listed below

## Included Suites

- `job/`: classic JOB (113 queries)
- `job_extended/`: extended JOB from Neo (24 queries)
- `job_d/`: JOB-D from HybridQO (20,000 queries)
- `ceb-imdb-3k/`: CEB "unique plans" subset (3,133 SQL files)
- `ceb-imdb-13k/`: full CEB set (13,646 SQL files)

## How To Run (PostgreSQL)

1. Create schema:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/schema.sql`
2. Load IMDB CSV data:
   - `psql -d <db> -v csv_dir=/absolute/path/to/imdb_csv -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/load.sql`
3. Run a query file from any suite:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/job/1a.sql`
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/job_d/00001.sql`

