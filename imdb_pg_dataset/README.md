# IMDB-Derived Suites

This directory contains IMDB-based query suites that share the same IMDB schema and CSV load path.

## Summary

- Included suite: `ceb-imdb-3k`
- Query count: 3,133
- Join size: 6 to 16
- Type: real IMDB workload
- Main use: larger IMDB campaign coverage beyond JOB and JOB-Complex

## Files

- `schema.sql`
  Imports the shared IMDB schema.
- `load.sql`
  Imports the shared IMDB CSV load script.
- `ceb-imdb-3k/`
  Query files for the CEB IMDB 3k subset.

## External Data

This workload requires the same external IMDB CSV bundle used by JOB.

Recommended download source:

- [https://bonsai.cedardb.com/job/imdb.tgz](https://bonsai.cedardb.com/job/imdb.tgz)

## How To Run

```bash
psql -d <db> -f imdb_pg_dataset/schema.sql
psql -d <db> -v csv_dir=/absolute/path/to/imdb_csv -f imdb_pg_dataset/load.sql
psql -d <db> -f imdb_pg_dataset/ceb-imdb-3k/1a/1a1.sql
```
