# Snowflake-Small

## Structure

- `snowflake.py`: generator
- generated `schema.sql`, `load.sql`, `constraints.sql`, `queries/*.sql`

## Query Shape

- Hierarchical snowflake schema (fact + multi-level dimensions)
- Query width controlled by `GPUQO_MAX_QUERY_SIZE`
- Join type: parent-child PK/FK equi-joins

## Generate & Run

```bash
cd /Users/yanchengpeng/projects/oss/join_order_benchmark/postgres-gpuqo/scripts/databases/snowflake-small
python3 snowflake.py
psql -d <db> -f schema.sql
psql -d <db> -f load.sql
# optional:
# psql -d <db> -f constraints.sql
psql -d <db> -f queries/010aa.sql
```
