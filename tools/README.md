# Tools

This directory contains small support artifacts for running and reviewing
benchmark results.  The important split is:

- `query_manifest.csv` is the tracked workload metadata used by the runner.
- `build_query_manifest.py` rebuilds and checks that metadata from SQL files.
- `render_review_tables.py` turns run outputs into the reviewer workbook.

## Query Manifest

`query_manifest.csv` is the global machine-readable manifest for all built-in
benchmark queries.  It is checked into git as part of the benchmark artifact so
reviewers can inspect workload coverage without scanning every SQL file.

The runner uses it to:

- discover available datasets
- resolve the query list for a scenario
- apply `--min-join` filters
- write `join_size` into `summary.csv` and reviewer tables

## Manifest Builder

`build_query_manifest.py` rebuilds `query_manifest.csv` from the workload SQL
files.  It also verifies expected dataset counts and join-size ranges so
accidental workload drift is easier to catch.

Run it after adding, removing, or editing workload SQL:

```bash
python3 tools/build_query_manifest.py --verify --summary
```

## Review Workbook

`render_review_tables.py` renders `outputs/<run_id>/review.xlsx` from an
existing run directory.  It reads metrics from `summary.csv` and the dataset /
variant order from `run.json`.

## Manifest Schema

Example row:

```csv
dataset,query_id,query_path,query_label,join_size,sql_sha1
job,10a,join-order-benchmark/queries/10a.sql,,7,1fe1fac887e587704d8acfcf101d8ddb889af0bb
```

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
