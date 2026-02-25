# Synthetic Dataset Generators (PostgreSQL)

All generators here follow one workflow:

1. `cd` into one dataset directory.
2. Run its Python generator to produce:
   - `schema.sql`
   - `load.sql`
   - `constraints.sql` (for datasets where constraints are generated separately)
   - `queries/*.sql`
3. Load and run with PostgreSQL.

## Common Environment Variables

- `GPUQO_TABLES`: number of tables (chain/clique/star)
- `GPUQO_ROWS`: rows per table (chain/clique/star)
- `GPUQO_QUERIES_PER_SIZE`: query files per join width
- `GPUQO_MAX_QUERY_SIZE`: largest generated join width
- `GPUQO_SEED`: RNG seed (`0x...` or decimal)
- `GPUQO_MIN_ROWS`, `GPUQO_MAX_ROWS`: snowflake data cardinality range
- `GPUQO_MAX_QUERY_SIZE`: largest query width for snowflake

## Example

```bash
cd /Users/yanchengpeng/projects/oss/join_order_benchmark/postgres-gpuqo/scripts/databases/chain-small
python3 chain.py
psql -d <db> -f schema.sql
psql -d <db> -f load.sql
psql -d <db> -f queries/10aa.sql
```
