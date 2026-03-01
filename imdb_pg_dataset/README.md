# IMDB-Derived Query Suites

This directory contains a curated IMDB-based SQL suite used in join-order and cardinality-estimation research.

## Purpose

Use these queries as an IMDB-schema workload that is easy to keep in-tree (moderate size) while still being
representative for optimizer/cardinality work.

## Shared Execution Layout

- `schema.sql`: imports the shared IMDB schema
- `load.sql`: imports shared CSV loading script
- Query suites live in subdirectories listed below

## Included Suites

- `ceb-imdb-3k/`: CEB "unique plans" subset (3,133 SQL files)

## Removed Suites (Rationale)

To keep this repository focused on large-join benchmarking and practical iteration, the following suites were removed:

- `job/`: duplicate of `join-order-benchmark/` (same queries, different formatting)
- `job_extended/`: no >=12-way joins (max join size 11)
- `job_d/`: very large file count but join sizes still capped at 17
- `ceb-imdb-13k/`: very large file count but join sizes still capped at 16

They remain available via git history (and their upstream sources) if needed.

## How To Run (PostgreSQL)

1. Create schema:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/schema.sql`
2. Load IMDB CSV data:
   - `psql -d <db> -v csv_dir=/absolute/path/to/imdb_csv -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/load.sql`
3. Run a query file:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/ceb-imdb-3k/1a/1a1.sql`
