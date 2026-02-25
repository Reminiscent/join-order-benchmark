# Star-Small

## Structure

- `star.py`: generator
- generated `schema.sql`, `load.sql`, `queries/*.sql`

## Query Shape

- One fact table (`T0`) with many dimension tables
- Query width: 2..N-1 tables
- Join type: fact-to-dimension PK/FK equi-joins

## Generate & Run

```bash
cd /Users/yanchengpeng/projects/oss/join_order_benchmark/postgres-gpuqo/scripts/databases/star-small
python3 star.py
psql -d <db> -f schema.sql
psql -d <db> -f load.sql
psql -d <db> -f queries/10aa.sql
```

