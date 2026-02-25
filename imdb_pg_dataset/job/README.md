# JOB (IMDB)

## Source

- Workload: Join Order Benchmark (JOB)
- Paper: *How Good Are Query Optimizers, Really?* (PVLDB 2015)

## File Structure

- `*.sql`: one query per file (`1a.sql` ... `33c.sql`)

## SQL Characteristics

- Query count: **113**
- Join width: about **4 to 17 tables**
- Style: traditional multi-way IMDB joins with selective filters
- Focus: baseline join-order planning quality on realistic PK/FK-heavy joins

## Run

- Requires the shared IMDB schema/data loaded via:
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/schema.sql`
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/load.sql`
- Example:
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/job/1a.sql`

