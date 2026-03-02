# IMDB CEB-3k Subsets

This directory contains prebuilt query-id lists for `imdb_ceb_3k`, generated from
`meta/query_manifest.csv`.

Regenerate with:

```bash
python3 tools/build_imdb_ceb_subsets.py
```

## Files

- `all.txt`: all query ids in dataset order.
- `all_dedup.txt`: same as `all.txt`, after exact-SQL dedupe by `sql_sha1`.
- `join_XX.txt`: query ids for a single `join_size` bucket.
- `bucket_06_08.txt`, `bucket_09_11.txt`, `bucket_12_13.txt`, `bucket_14_16.txt`:
  broad join-width ranges.
- `ge_12.txt`, `ge_14.txt`: larger-join focus subsets.
- `stratified_300.txt`, `stratified_600.txt`, `stratified_1200.txt`:
  deterministic stratified samples by `join_size`.

## Usage

Use with `bench.py run` / `bench.py smoke`:

```bash
python3 bench/bench.py run imdb_ceb_3k imdb_mvp \
  --host localhost --port 54321 \
  --query-id-file meta/subsets/imdb_ceb_3k/stratified_600.txt \
  --dedupe-sql \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off"
```
