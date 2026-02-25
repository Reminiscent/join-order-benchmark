# CEB-IMDB 3k (Unique Plans)

## Source

- Workload: CEB IMDB "unique plans" subset
- Paper: *Flow-Loss: Learning Cardinality Estimates That Matter* (PVLDB 2021)
- Upstream benchmark: [learnedsystems/CEB](https://github.com/learnedsystems/CEB)

## File Structure

- `<template_id>/`: query family folders (`1a`, `2a`, `3b`, ...)
- `<template_id>/*.sql`: concrete query instances (query instance files (for example `1a1.sql`))

## SQL Characteristics

- Query count: **3,133**
- Join width: about **6 to 16 tables**
- Style: template-based IMDB join queries with many predicate instantiations
- Focus: cardinality-estimation and join-order sensitivity at moderate workload size

## Run

- Requires the shared IMDB schema/data loaded via:
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/schema.sql`
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/load.sql`
- Example:
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/ceb-imdb-3k/1a/1a1.sql`

