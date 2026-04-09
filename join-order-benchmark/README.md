# JOB

This directory contains the PostgreSQL-formatted Join Order Benchmark (JOB) workload.

## Summary

- Source: *How Good Are Query Optimizers, Really?* (PVLDB 2015)
- Query count: 113
- Join size: 4 to 17
- Type: real IMDB workload
- Main use: optimizer baseline for join-order evaluation on correlated real-world data

## Files

- `schema.sql`
  IMDB schema used by JOB.
- `fkindexes.sql`
  Recommended supporting indexes.
- `load.sql`
  PostgreSQL `\copy` script for the 21 IMDB CSV files.
- `queries/*.sql`
  Individual JOB query files.

## External Data

This workload requires the external IMDB CSV bundle.

Recommended download source:

- [https://bonsai.cedardb.com/job/imdb.tgz](https://bonsai.cedardb.com/job/imdb.tgz)

Historical reference:

- [https://event.cwi.nl/da/job/](https://event.cwi.nl/da/job/)

## How To Run

```bash
psql -d <db> -f join-order-benchmark/schema.sql
psql -d <db> -f join-order-benchmark/fkindexes.sql
psql -d <db> -v csv_dir=/absolute/path/to/imdb_csv -f join-order-benchmark/load.sql
psql -d <db> -f join-order-benchmark/queries/1a.sql
```
