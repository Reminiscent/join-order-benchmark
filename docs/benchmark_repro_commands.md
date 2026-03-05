# Benchmark Repro Commands (GOO vs GEQO/DP)

This doc captures the exact command patterns to reproduce the benchmark matrix with
`statement_timeout = 10min`, and to continue running other datasets when some
queries timeout.

## 1) Environment

```bash
export PATH="$PG_BUILD_DIR/bin:$PATH"
cd /Users/yanchengpeng/projects/oss/join_order_benchmark

# Optional connection overrides (defaults below are already used by scripts)
export PG_BENCH_HOST=localhost
export PG_BENCH_PORT=54321
```

Quick connectivity check:

```bash
$PG_BUILD_DIR/bin/psql -X -h localhost -p 54321 postgres -c "select version();"
$PG_BUILD_DIR/bin/psql -X -h localhost -p 54321 postgres -c "show enable_goo_join_search;"
$PG_BUILD_DIR/bin/psql -X -h localhost -p 54321 postgres -c "show goo_greedy_strategy;"
```

## 2) Algorithms (same matrix as benchmark reports)

- `dp`: `SET geqo_threshold = 100; SET enable_goo_join_search = off;`
- `geqo`: `SET geqo_threshold = 2; SET enable_goo_join_search = off;`
- `goo_cost`: `SET geqo_threshold = 2; SET enable_goo_join_search = on; SET goo_greedy_strategy = 'cost';`
- `goo_result_size`: `SET geqo_threshold = 2; SET enable_goo_join_search = on; SET goo_greedy_strategy = 'result_size';`
- `goo_selectivity`: `SET geqo_threshold = 2; SET enable_goo_join_search = on; SET goo_greedy_strategy = 'selectivity';`
- `goo_combined`: `SET geqo_threshold = 2; SET enable_goo_join_search = on; SET goo_greedy_strategy = 'combined';`

## 3) Full-run helper script

`tools/run_mvp_full.sh` supports:

- default matrix execution
- per-dataset selection (`ONLY_DATASETS`)
- dataset exclusion (`SKIP_DATASETS`)
- optional inclusion of `imdb_ceb_3k`
- timeout via `STATEMENT_TIMEOUT_MS`

### 3.1 Default matrix (without imdb_ceb_3k)

```bash
BENCH_REPS=1 \
BENCH_STABILIZE=vacuum_freeze_analyze \
STATEMENT_TIMEOUT_MS=600000 \
tools/run_mvp_full.sh
```

### 3.2 Include imdb_ceb_3k

```bash
BENCH_REPS=1 \
BENCH_STABILIZE=vacuum_freeze_analyze \
STATEMENT_TIMEOUT_MS=600000 \
INCLUDE_IMDB_CEB_3K=1 \
tools/run_mvp_full.sh
```

### 3.3 Run only selected datasets

```bash
BENCH_REPS=1 \
BENCH_STABILIZE=vacuum_freeze_analyze \
STATEMENT_TIMEOUT_MS=600000 \
ONLY_DATASETS="imdb_ceb_3k,gpuqo_clique_small" \
tools/run_mvp_full.sh
```

## 4) Direct bench.py command (single dataset)

```bash
python3 bench/bench.py run imdb_ceb_3k imdb_mvp \
  --host localhost --port 54321 \
  --reps 1 \
  --stabilize vacuum_freeze_analyze \
  --statement-timeout-ms 600000 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

Optional batching knobs for long datasets:

- `--max-queries N`: cap selected queries for a run
- `--min-join / --max-join`: run one join-size slice at a time

Example (slice mode):

```bash
python3 bench/bench.py run imdb_ceb_3k imdb_mvp \
  --host localhost --port 54321 \
  --reps 1 \
  --min-join 6 --max-join 7 \
  --stabilize vacuum_freeze_analyze \
  --statement-timeout-ms 600000 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

## 5) Timeout/skip behavior

- Timeout is per statement via `SET statement_timeout = 600000`.
- Timeout/error rows are recorded in `raw.csv`/`summary.csv` as `status=error` / `err_reps > 0`.
- The harness continues to the next (query, algo), i.e. slow queries are skipped and the run proceeds.

## 6) Result files

Each run writes:

- `results/<run_id>/run.json`
- `results/<run_id>/raw.csv`
- `results/<run_id>/summary.csv`

Quick error summary:

```bash
awk -F, 'NR>1{tot[$4]++; if($13=="error") err[$4]++} END{for (a in tot) printf "%s total=%d err=%d\n", a, tot[a], err[a]}' \
  results/<run_id>/raw.csv | sort
```

Quick timeout/error query list from summary:

```bash
awk -F, 'NR>1 && $13>0 {print $4","$5","$8","$13}' results/<run_id>/summary.csv
```
