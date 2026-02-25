# SQLite select5 (PostgreSQL-converted)

This directory provides a PostgreSQL-ready conversion of SQLite's classic `select5` multi-way join stress test.

## Purpose

Use this workload to stress join-order planning on many-table equi-join graphs without requiring an external dataset.

## Source

- Original upstream file: SQLite test suite `test/select5.test`
- Original check-in context: `599e260e37` (2008-12-03)
- Original format: `sqllogictest`

## File Layout

- `schema.sql`: creates tables `t1` ... `t64`
- `load.sql`: inserts deterministic toy data (10 rows per table)
- `queries/select5.sql`: 732 SELECT statements converted to plain PostgreSQL SQL
- `select5.test`: PostgreSQL entry file that includes schema/load/queries
- `select5.sqlite.test`: untouched original SQLite sqllogictest source

## Query Characteristics

- Table count: **64**
- Data size: **10 rows per table**
- Query count: **732**
- Join width: **4 to 64 tables**
- Predicate style: dense equality predicates across alias-heavy table permutations
- Main usage: parser/planner stress and join-enumeration scalability testing

## How To Run (PostgreSQL)

- Run everything end-to-end:
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/sqlite/select5.test`
- Or run in stages:
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/sqlite/schema.sql`
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/sqlite/load.sql`
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/sqlite/queries/select5.sql`

