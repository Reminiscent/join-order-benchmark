# GPUQO Synthetic Workloads

This directory keeps the synthetic PostgreSQL workload subsets derived from the `postgres-gpuqo` project.

## Summary

Included subsets:

- `gpuqo_chain_small`
  150 queries, join size 2 to 16, chain-shaped join graphs.
- `gpuqo_clique_small`
  150 queries, join size 2 to 16, clique-shaped join graphs.
- `gpuqo_star_small`
  150 queries, join size 2 to 16, star-shaped join graphs.
- `gpuqo_snowflake_small`
  390 queries, join size 2 to 40, snowflake-shaped join graphs.

These workloads are self-contained and are useful when you want controlled synthetic join graph structure instead of IMDB-derived real data.

## Files

Each dataset directory under `scripts/databases/` provides:

- `schema.sql`
- `load.sql`
- `queries/*.sql`

## How To Run

Example for `chain-small`:

```bash
psql -d <db> -f postgres-gpuqo/scripts/databases/chain-small/schema.sql
psql -d <db> -f postgres-gpuqo/scripts/databases/chain-small/load.sql
psql -d <db> -f postgres-gpuqo/scripts/databases/chain-small/queries/10aa.sql
```
