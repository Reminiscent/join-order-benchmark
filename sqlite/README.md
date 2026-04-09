# SQLite select5

This directory contains a PostgreSQL-ready conversion of SQLite's `select5` join stress workload.

## Summary

- Source: SQLite `select5.test`
- Query count: 732
- Join size: 4 to 64
- Type: self-contained workload
- Main use: many-table join stress without any external dataset

## Files

- `schema.sql`
  Creates tables `t1` through `t64`.
- `load.sql`
  Loads deterministic toy data.
- `queries/select5.sql`
  Converted PostgreSQL query file.
- `select5.test`
  PostgreSQL entry file for the whole workload.
- `select5.sqlite.test`
  Original SQLite source file.

## How To Run

```bash
psql -d <db> -f sqlite/select5.test
```

Or run it in stages:

```bash
psql -d <db> -f sqlite/schema.sql
psql -d <db> -f sqlite/load.sql
psql -d <db> -f sqlite/queries/select5.sql
```
