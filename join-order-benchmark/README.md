# Join Order Benchmark (JOB)

This directory contains the original **Join Order Benchmark (JOB)** query set for PostgreSQL.

## Purpose

Use this workload to evaluate join enumeration and join-order planning decisions on real IMDB-style relational data.

## Source

- Paper: *How Good Are Query Optimizers, Really?* (PVLDB 2015)
- Authors: Viktor Leis, Andrey Gubichev, Atanas Mirchev, Peter Boncz, Alfons Kemper, Thomas Neumann
- Workload origin: IMDB snapshot from May 2013 used by JOB

## File Layout

- `schema.sql`: table definitions for the IMDB relational schema used by JOB
- `fkindexes.sql`: recommended supporting indexes on key foreign-key columns
- `load.sql`: PostgreSQL `\copy` commands (expects external CSV files)
- `queries/*.sql`: 113 JOB query files (`1a.sql` ... `33c.sql`)

## Query Characteristics

- Query count: **113**
- Join width: roughly **4 to 17 tables**
- Main pattern: multi-way joins over IMDB entities (title, cast, company, keyword, info)
- Predicates: equality joins + selective string and range filters
- Join graph style: mostly PK/FK-heavy with realistic correlation effects

## How To Run (PostgreSQL)

1. Create a database.
2. Create tables:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/join-order-benchmark/schema.sql`
3. (Optional but recommended) create supporting indexes:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/join-order-benchmark/fkindexes.sql`
4. Load data (CSV directory must contain all 21 IMDB CSV files):
   - `psql -d <db> -v csv_dir=/absolute/path/to/imdb_csv -f /Users/yanchengpeng/projects/oss/join_order_benchmark/join-order-benchmark/load.sql`
5. Run queries:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/join-order-benchmark/queries/1a.sql`
