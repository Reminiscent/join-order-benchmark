# JOB-Complex

This directory contains the **JOB-Complex** workload for PostgreSQL-focused join-order testing.

## Purpose

JOB-Complex extends classic JOB with harder, more realistic query structures so optimizer plan choices become significantly more challenging.

## Source

- Paper: *JOB-Complex: A Challenging Benchmark for Traditional & Learned Query Optimization* (AIDB@VLDB 2025)
- Authors: Johannes Wehrstein, Timo Eckmann, Roman Heinrich, Carsten Binnig
- Original project: DataManagementLab/JOB-Complex

## File Layout

- `schema.sql`: imports the shared IMDB schema (`../join-order-benchmark/schema.sql`)
- `load.sql`: imports shared IMDB CSV loading script (`../join-order-benchmark/load.sql`)
- `JOB-Complex.sql`: original combined SQL file from the upstream project
- `queries/q01.sql` ... `queries/q30.sql`: split query files for easier execution and tooling

## Query Characteristics

- Query count: **30**
- Join width: mostly **6 to 16 tables**
- Join types: includes PK/FK joins plus non-PK/FK and string-based joins
- Predicate style: heavier use of `LIKE`, `IN`, and multi-condition filters than JOB
- Goal: stress join-ordering robustness under complex real-world predicates

## How To Run (PostgreSQL)

1. Create schema:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/JOB-Complex/schema.sql`
2. Load IMDB CSV data:
   - `psql -d <db> -v csv_dir=/absolute/path/to/imdb_csv -f /Users/yanchengpeng/projects/oss/join_order_benchmark/JOB-Complex/load.sql`
3. Run one query:
   - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/JOB-Complex/queries/q01.sql`
