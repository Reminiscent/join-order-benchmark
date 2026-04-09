# Meta

This directory contains tracked derived metadata used by the benchmark runner.

## Files

`query_manifest.csv`

- global machine-readable manifest for all benchmark queries
- used for dataset discovery and join-size filtering
- checked into git as part of the benchmark artifact

Schema:

- `dataset`
  Stable dataset id used by the runner.
- `query_id`
  Stable query id within that dataset.
- `query_path`
  Repo-relative path to the SQL file.
- `query_label`
  Optional label, mainly for `sqlite_select5`.
- `join_size`
  Number of relations in the query's top-level `FROM` clause.
- `sql_sha1`
  SHA1 of canonicalized SQL after removing formatting-only noise.

## Refresh

To rebuild the query manifest:

```bash
python3 tools/build_query_manifest.py --verify --summary
```
