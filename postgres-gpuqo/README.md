# PostgreSQL GPUQO Synthetic Workloads (Trimmed)

This directory keeps only the workload-generation pieces that are useful for join-order benchmark testing on PostgreSQL.

## Source

- Original project: `postgres-gpuqo`
- Paper: *Efficient Massively Parallel Join Optimization for Large Queries* (SIGMOD 2022)

## What Is Kept

- `scripts/databases/chain-small`
- `scripts/databases/clique-small`
- `scripts/databases/star-small`
- `scripts/databases/snowflake-small`

All PostgreSQL engine source code and non-workload assets were removed to keep this repository focused on benchmark content.

## Unified Workflow

Each generator now emits the same output contract inside its own folder:

- `schema.sql`: table DDL (+ FK constraints)
- `load.sql`: synthetic data inserts
- `queries/*.sql`: SELECT workload files

See `scripts/databases/README.md` for exact commands.

