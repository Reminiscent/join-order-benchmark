# JOB-Extended

## Source

- Workload: JOB-Extended query set used by Neo
- Paper: *Neo: A Learned Query Optimizer* (PVLDB 2019)

## File Structure

- `e*.sql`: 24 query files (`e1a.sql` ... `e12b.sql`)

## SQL Characteristics

- Query count: **24**
- Join width: about **3 to 11 tables**
- Style: same IMDB schema as JOB, but different query semantics and selectivity patterns
- Focus: robustness testing for systems tuned on classic JOB

## Run

- Requires the shared IMDB schema/data loaded via:
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/schema.sql`
  - `/Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/load.sql`
- Example:
  - `psql -d <db> -f /Users/yanchengpeng/projects/oss/join_order_benchmark/imdb_pg_dataset/job_extended/e1a.sql`

