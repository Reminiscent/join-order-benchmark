# Tools

This directory contains supporting files for the benchmark runner: the tracked
query manifest and small helper commands.

## Files

`query_manifest.csv`

- global machine-readable manifest for all benchmark queries
- used for dataset discovery and join-size filtering
- checked into git as part of the benchmark artifact

`build_query_manifest.py`

- rebuilds `query_manifest.csv` from the workload SQL files
- verifies expected dataset counts and join-size ranges

`render_review_tables.py`

- renders `outputs/<run_id>/review.xlsx` from `outputs/<run_id>/summary.csv`
- reads datasets and variant order from `outputs/<run_id>/run.json`

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
