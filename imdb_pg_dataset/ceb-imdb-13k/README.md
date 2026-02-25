# CEB-IMDB 13k (Full)

## Source

- Workload: full CEB IMDB query set
- Paper: *Flow-Loss: Learning Cardinality Estimates That Matter* (PVLDB 2021)
- Upstream benchmark: [learnedsystems/CEB](https://github.com/learnedsystems/CEB)

## File Structure

- `<template_id>/`: query family folders (`1a`, `2a`, `3b`, ...)
- `<template_id>/*.sql`: concrete query instances (query instance files (for example `1a1.sql`))

## SQL Characteristics

- Query count: **13,646**
- Join width: about **6 to 16 tables**
- Style: same template family as 3k set with many more instantiations
- Focus: large-scale workload coverage for optimizer/cardinality studies

## Run

- Requires the shared IMDB schema/data loaded via:
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/schema.sql`
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/load.sql`
- Example:
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/ceb-imdb-13k/1a/1a1.sql`

