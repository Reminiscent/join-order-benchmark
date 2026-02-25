# Chain-Small

## Structure

- `chain.py`: generator
- generated `schema.sql`, `load.sql`, `queries/*.sql`

## Query Shape

- Chain join graph (`T1 -> T2 -> ... -> TN`)
- Typical query width: 2..N-1 tables
- Join type: PK/FK equi-joins on integer keys

## Generate & Run

```bash
cd /Users/yanchengpeng/projects/oss/join_order_benchmark/postgres-gpuqo/scripts/databases/chain-small
python3 chain.py
psql -d <db> -f schema.sql
psql -d <db> -f load.sql
psql -d <db> -f queries/10aa.sql
```

