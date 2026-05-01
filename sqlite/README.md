# SQLite select5

This directory contains a PostgreSQL-ready conversion of the SQLite
sqllogictest `select5` join stress workload.

## Summary

- Source: SQLite sqllogictest `test/select5.test` artifact
  `5063a1dec5159873c5a8f75d666b71f3e2538d77`
- Query count: 732
- Join size: 4 to 64
- Type: self-contained workload
- Main use: many-table join stress without any external dataset

This workload is not the current SQLite core source-tree
`src/test/select5.test` regression file.  The local source file is the vendored
sqllogictest/logictest version in `select5.sqlite.test`.

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
