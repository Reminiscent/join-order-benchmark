# Clique-Small

## Structure

- `clique.py`: generator
- generated `schema.sql`, `load.sql`, `constraints.sql`, `queries/*.sql`

## Query Shape

- Dense clique-like join graph
- Query width: 2..N-1 sampled tables
- Join type: many pairwise equi-join predicates

## Generate & Run

```bash
cd /Users/yanchengpeng/projects/oss/join_order_benchmark/postgres-gpuqo/scripts/databases/clique-small
python3 clique.py
psql -d <db> -f schema.sql
psql -d <db> -f load.sql
# optional (very expensive on dense clique data):
# psql -d <db> -f constraints.sql
psql -d <db> -f queries/10aa.sql
```
