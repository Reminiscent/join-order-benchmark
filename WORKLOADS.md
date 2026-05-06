# Workloads

This document describes the built-in benchmark scenarios and the datasets behind
them.  Scenarios are built into the harness; choose one by name with
`bench.py prepare` and `bench.py run`.

Dataset-level query counts and join sizes come from
[tools/query_manifest.csv](tools/query_manifest.csv).

## Scenario Layers

| Scenario | When to use it | Included datasets |
| --- | --- | --- |
| `main` | First validation run for a new join-order algorithm | `job`, `job_complex` |
| `extended` | Larger IMDB-backed validation after `main` looks good | `main` plus `imdb_ceb_3k` |
| `planning` | Synthetic wide-join planning/search-space checks | `sqlite_select5`, `gpuqo_chain_small`, `gpuqo_star_small`, `gpuqo_snowflake_small`, `gpuqo_clique_small` |

`main` is the primary public validation path.  It keeps the campaign small
enough for iteration while still covering realistic join-order choices.

`extended` keeps all `main` datasets and adds `imdb_ceb_3k`, which has much
higher query volume than the other IMDB-backed workloads and can dominate
campaign time.

`planning` contains self-contained workloads adapted from existing upstream
sources.  These datasets use small local data and many wide joins, so they are
mainly for planning-time and join-search-space validation.  Execution-time
results from them are diagnostic only and should not be presented as realistic
end-to-end workload performance.

## Workload Overview

| Dataset | Queries | Join Size | Join Size >= 12 | Data | Role |
| --- | ---: | --- | ---: | --- | --- |
| `job` | 113 | 4-17 | 20 | external IMDB CSV | classic optimizer baseline with realistic correlations |
| `job_complex` | 30 | 6-16 | 13 | external IMDB CSV | harder predicates, non-PK/FK joins, and challenging join-order choices |
| `imdb_ceb_3k` | 3,133 | 6-16 | 842 | external IMDB CSV | large-volume CEB coverage for long validation campaigns |
| `sqlite_select5` | 732 | 4-64 | 636 | self-contained toy data | high-width join stress without external data files |
| `gpuqo_chain_small` | 150 | 2-16 | 50 | self-contained synthetic data | chain-shaped join graph stress, mainly useful for planning/search-space behavior |
| `gpuqo_clique_small` | 150 | 2-16 | 50 | self-contained synthetic data | dense clique-shaped join graph stress, mainly useful for planning/search-space behavior |
| `gpuqo_star_small` | 150 | 2-16 | 50 | self-contained synthetic data | star-shaped join graph stress, mainly useful for planning/search-space behavior |
| `gpuqo_snowflake_small` | 390 | 2-40 | 290 | self-contained synthetic data | wider snowflake-shaped join graph stress, mainly useful for planning/search-space behavior |

The GPUQO-derived small datasets are not bare query files without data: each
local dataset has generated `schema.sql`, `load.sql`, and `queries/*.sql`.
However, the data is small and synthetic, so their main purpose in this
repository is to stress join-search behavior and planning time on generated join
graph shapes.

The self-contained planning datasets use these local data sizes:

- `sqlite_select5` has 64 tables with 10 rows per table.
- `gpuqo_chain_small`, `gpuqo_clique_small`, and `gpuqo_star_small` have 40
  tables with 200 rows per table.
- `gpuqo_snowflake_small` uses generated snowflake tables with 20 to 200 rows
  per table.

## Workload Sources

All workloads in this repository are imported or adapted from existing sources.
The local files may be converted to PostgreSQL syntax, split into one query per
file, or regenerated as smaller self-contained subsets.

| Local dataset | Upstream source | Related paper | Local adaptation |
| --- | --- | --- | --- |
| `job` | [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark), using the IMDB CSV data referenced by the JOB artifact | [How Good Are Query Optimizers, Really?](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | PostgreSQL schema/load scripts and per-query files are kept locally. |
| `job_complex` | [DataManagementLab/JOB-Complex](https://github.com/DataManagementLab/JOB-Complex) | [JOB-Complex: A Challenging Benchmark for Traditional & Learned Query Optimization](https://arxiv.org/abs/2507.07471) | The combined SQL file is split into per-query files and run on the shared IMDB schema. |
| `imdb_ceb_3k` | [learnedsystems/CEB](https://github.com/learnedsystems/CEB) | [Flow-Loss: Learning Cardinality Estimates That Matter](https://arxiv.org/abs/2101.04964) | The CEB IMDB 3k subset is stored as executable SQL query files under the shared IMDB schema. |
| `sqlite_select5` | SQLite select5 artifact [`test/select5.test`](https://www.sqlite.org/sqllogictest/artifact/5063a1dec5159873) | No dedicated paper source is confirmed for this repository. | Converted from the vendored source file [sqlite/select5.sqlite.test](sqlite/select5.sqlite.test) to PostgreSQL schema/load/query SQL.  This is not the current SQLite core `src/test/select5.test` regression file. |
| `gpuqo_chain_small`, `gpuqo_clique_small`, `gpuqo_star_small`, `gpuqo_snowflake_small` | [mageirakos/postgres-gpuqo](https://github.com/mageirakos/postgres-gpuqo) | Project-related papers: [Efficient Massively Parallel Join Optimization for Large Queries](https://arxiv.org/abs/2202.13511), [Efficient GPU-accelerated Join Optimization for Complex Queries](https://mageirakos.github.io/publication/2022-gpuqo-demo) | Small deterministic PostgreSQL subsets generated from the GPUQO synthetic workload shapes, including local `schema.sql`, `load.sql`, and query files. |

## External IMDB CSV Data

This repository does not vendor the IMDB CSV bundle used by IMDB-backed
workloads.

Datasets that require the external IMDB CSV bundle:

- `job`
- `job_complex`
- `imdb_ceb_3k`

Recommended download source:

- CedarDB mirror: [https://bonsai.cedardb.com/job/imdb.tgz](https://bonsai.cedardb.com/job/imdb.tgz)

Historical reference:

- CWI JOB page: [https://event.cwi.nl/da/job/](https://event.cwi.nl/da/job/)

Example setup:

```bash
mkdir -p data/imdb_csv
tar -xzf imdb.tgz -C data/imdb_csv
```

The extracted directory should contain the 21 CSV files used by the IMDB schema
load scripts.

## Query Manifest

The global query manifest is [tools/query_manifest.csv](tools/query_manifest.csv).
Refresh and verify it with:

```bash
python3 tools/build_query_manifest.py --verify --summary
```
