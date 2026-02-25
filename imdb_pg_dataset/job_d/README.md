# JOB-D

## Source

- Workload: JOB-D
- Paper: *Cost-based or Learning-based? A Hybrid Query Optimizer for Query Plan Selection* (PVLDB 2022)

## File Structure

- `*.sql`: 20,000 generated query files, each named by numeric ID (for example `00001.sql`)

## SQL Characteristics

- Query count: **20,000**
- Join width: about **4 to 17 tables**
- Style: IMDB-based multi-way joins with broader predicate/value variation than classic JOB
- Focus: large-sample optimizer benchmarking and plan-selection evaluation

## Run

- Requires the shared IMDB schema/data loaded via:
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/schema.sql`
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/load.sql`
- Example:
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/job_d/00001.sql`

