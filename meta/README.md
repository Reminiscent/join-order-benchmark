# Query Manifest

This directory contains **derived metadata** about the query workloads in this repository.

## `query_manifest.csv`

`query_manifest.csv` is a global manifest that makes `join_size` (number of relations in the top-level `FROM` clause)
explicit and machine-readable.

Schema:

- `dataset`: stable dataset id (used by runners/summaries)
- `query_id`: stable id within the dataset (usually filename stem; for `sqlite_select5` it is `0001..0732`)
- `query_path`: repo-relative path to the SQL file (for `sqlite_select5` it is always `sqlite/queries/select5.sql`)
- `query_label`: optional label (used by `sqlite_select5`, e.g. `join-64-4`)
- `join_size`: number of relations in the query's top-level `FROM` clause
- `sql_sha1`: sha1 of canonicalized SQL (lowercase + remove all whitespace + strip `--` line comments)

## Regenerating

Run:

```bash
python3 tools/build_query_manifest.py --verify --summary
```

