# Join Order Benchmark (PostgreSQL) - MVP 跑通指南

本文档的目标是：让你在**同一套数据集**上，快速对比不同 join order 搜索算法（DP / GEQO / Goo-*），并且保证本仓库 `meta/query_manifest.csv` 里列出的 **全部 workload 都能 prepare + run 跑通不报错**。

> 说明：本文档只覆盖 “MVP 必须部分”。更多参数（timeout、GPUQO constraints、结果分析等）后续再加。

## 1. 前置条件

你需要一个可以连接的 PostgreSQL 实例（包含你新增的 Goo Join Search 相关 GUC），例如：

- Host: `localhost`
- Port: `54321`

并且本机需要能执行 `psql/createdb/dropdb`（bench harness 会调用它们）。如果你用的是自编译 PG，建议：

```bash
export PATH="$PG_BUILD_DIR/bin:$PATH"
```

先检查数据库能连通，以及 GUC 存在：

```bash
psql -h localhost -p 54321 postgres -c "SELECT version();"
psql -h localhost -p 54321 postgres -c "SHOW enable_goo_join_search;"
psql -h localhost -p 54321 postgres -c "SHOW goo_greedy_strategy;"
```

## 2. 准备 IMDB CSV（仅 IMDB workload 需要）

IMDB 系 workload（`job` / `job_complex` / `imdb_ceb_3k`）共用同一套 21 个 CSV。

按约定把 CSV 解压到 repo 内（但 **不要提交**，已在 `.gitignore` 里忽略 `data/`）：

```bash
mkdir -p data/imdb_csv
# 把 zip 路径替换成你自己的 IMDB CSV zip 文件路径
unzip /Users/yanchengpeng/Downloads/csv_files.zip -d data/imdb_csv
ls data/imdb_csv/*.csv | wc -l
```

`wc -l` 预期输出 `21`。

## 3. 统一的算法配置（6 套）

bench harness 通过 `--algo name:key=value,key=value` 传入一组 GUC。

下面这 6 套就是你要对比的 join order 搜索算法配置：

```bash
--algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
--algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
--algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
--algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
--algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
--algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

## 4. Prepare：建库 + 导入数据

### 4.1 一次性准备 IMDB 数据库（job/job_complex/imdb_ceb_3k 共用）

只需要 prepare 一次（用 `job` 的 schema/load/index），后续 3 个 IMDB workload 都复用同一个 DB：

```bash
python3 bench/bench.py prepare job imdb_mvp \
  --csv-dir "$(pwd)/data/imdb_csv" \
  --host localhost --port 54321
```

说明：
- 这个步骤会导入较大的 CSV（总量约数 GB），需要一定时间和磁盘空间。
- `prepare job` 会自动执行 `join-order-benchmark/fkindexes.sql`（IMDB schema 推荐索引）。

### 4.2 准备自包含数据集（sqlite + GPUQO）

sqlite（完全自包含）：

```bash
python3 bench/bench.py prepare sqlite_select5 select5_mvp --host localhost --port 54321
```

GPUQO（四个 synthetic workload，完全自包含）：

```bash
python3 bench/bench.py prepare gpuqo_chain_small gpuqo_chain_mvp --host localhost --port 54321
python3 bench/bench.py prepare gpuqo_clique_small gpuqo_clique_mvp --host localhost --port 54321
python3 bench/bench.py prepare gpuqo_star_small gpuqo_star_mvp --host localhost --port 54321
python3 bench/bench.py prepare gpuqo_snowflake_small gpuqo_snowflake_mvp --host localhost --port 54321
```

> MVP 不加载 `constraints.sql`（可选且可能很慢），后续需要再加开关。

## 5. Smoke：验证“所有 workload 都能跑起来不报错”

Smoke 的目标是 **先跑通**（不关心效果/耗时差异）：

- 使用 `smoke` 子命令（默认只跑 1 条 query，固定 `reps=1` 且 `stabilize=none`）
- 可用 `--queries N` 把 smoke 扩大到 N 条 query
- IMDB workload 的 `--min-join` 建议先用较小值（例如 6）避免碰到特别慢的 query；跑质量对比时再用 12+

### 5.1 IMDB workload（共用 imdb_mvp DB）

```bash
python3 bench/bench.py smoke job imdb_mvp \
  --host localhost --port 54321 --min-join 6 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

```bash
python3 bench/bench.py smoke job_complex imdb_mvp \
  --host localhost --port 54321 --min-join 6 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

```bash
python3 bench/bench.py smoke imdb_ceb_3k imdb_mvp \
  --host localhost --port 54321 --min-join 6 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

### 5.2 sqlite_select5（自包含）

```bash
python3 bench/bench.py smoke sqlite_select5 select5_mvp \
  --host localhost --port 54321 --min-join 4 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

### 5.3 GPUQO（自包含）

```bash
python3 bench/bench.py smoke gpuqo_chain_small gpuqo_chain_mvp \
  --host localhost --port 54321 --min-join 2 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

```bash
python3 bench/bench.py smoke gpuqo_clique_small gpuqo_clique_mvp \
  --host localhost --port 54321 --min-join 2 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

```bash
python3 bench/bench.py smoke gpuqo_star_small gpuqo_star_mvp \
  --host localhost --port 54321 --min-join 2 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

```bash
python3 bench/bench.py smoke gpuqo_snowflake_small gpuqo_snowflake_mvp \
  --host localhost --port 54321 --min-join 2 --queries 1 \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```

## 6. 如何判断成功

每次 `run` 或 `smoke` 会输出一个 `run_id`，并写入目录：

- `results/<run_id>/raw.csv`
- `results/<run_id>/summary.csv`
- `results/<run_id>/run.json`

最简单的检查方式：看 `raw.csv` 是否出现 `status=error`。

例如：

```bash
rg \",error,\" results/*/raw.csv
```

## 7. 正式对比（建议）

当 smoke 全部跑通后，再开始对比 plan quality：

- IMDB：建议 `--min-join 12`（join 更宽，更容易拉开 join order 的差异）
- `sqlite_select5` / `gpuqo_snowflake_small`：join 很宽（20..64 / up to 40），`dp` 可能非常慢
  - 可以先只跑 `geqo + goo_*`，或者把 `--min-join` 调小到 12/16 再加上 `dp`

如果 `imdb_ceb_3k` 全量太慢，可以用子集文件：

- 子集目录：`meta/subsets/imdb_ceb_3k/`
- 重新生成：`python3 tools/build_imdb_ceb_subsets.py`
- 示例（600 条分层子集）：

```bash
python3 bench/bench.py run imdb_ceb_3k imdb_mvp \
  --host localhost --port 54321 \
  --query-id-file meta/subsets/imdb_ceb_3k/stratified_600.txt \
  --dedupe-sql \
  --algo "dp:geqo_threshold=100,enable_goo_join_search=off" \
  --algo "geqo:geqo_threshold=2,enable_goo_join_search=off" \
  --algo "goo_cost:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=cost" \
  --algo "goo_result_size:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=result_size" \
  --algo "goo_selectivity:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=selectivity" \
  --algo "goo_combined:geqo_threshold=2,enable_goo_join_search=on,goo_greedy_strategy=combined"
```
