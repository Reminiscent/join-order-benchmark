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
  Base-relation count used for join-size filters and reviewer tables.  The
  manifest builder removes full-line `--` comments, captures the first `FROM`
  chunk up to `WHERE`, `;`, or end of SQL, and counts the comma-separated items
  in that chunk.
- `sql_sha1`
  SHA1 of canonicalized SQL after removing formatting-only noise.

`join_size` is intentionally a lightweight workload-manifest metric, not a SQL
parser result and not the number of binary join operators in a parsed plan.  All
tracked built-in workloads have been checked to use flat comma-separated
inner-join `FROM` lists, so this count works for the current benchmark datasets
and matches the number of base relations in the join-order problem.  The metric
does not guarantee correct semantics for explicit `JOIN` syntax, outer joins,
nested subqueries, lateral items, or other SQL shapes that are not a flat
comma-separated `FROM` list.

## Refresh

To rebuild the query manifest:

```bash
python3 tools/build_query_manifest.py --verify --summary
```
