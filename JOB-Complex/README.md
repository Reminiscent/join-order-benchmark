# JOB-Complex

This directory contains the PostgreSQL-formatted JOB-Complex workload.

## Summary

- Source: *JOB-Complex: A Challenging Benchmark for Traditional & Learned Query Optimization*
- Query count: 30
- Join size: 6 to 16
- Type: real IMDB workload
- Main use: harder join-order evaluation with more complex predicates and non-PK/FK joins

## Files

- `schema.sql`
  Imports the shared IMDB schema.
- `load.sql`
  Imports the shared IMDB CSV load script.
- `JOB-Complex.sql`
  Upstream combined SQL file.
- `queries/*.sql`
  Split query files for execution and tooling.

## External Data

This workload requires the same external IMDB CSV bundle used by JOB.

Recommended download source:

- [https://bonsai.cedardb.com/job/imdb.tgz](https://bonsai.cedardb.com/job/imdb.tgz)

## How To Run

```bash
psql -d <db> -f JOB-Complex/schema.sql
psql -d <db> -v csv_dir=/absolute/path/to/imdb_csv -f JOB-Complex/load.sql
psql -d <db> -f JOB-Complex/queries/q01.sql
```
