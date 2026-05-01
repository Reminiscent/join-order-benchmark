# Datasets

This document keeps workload and data-source details out of the top-level
README.  Use [README.md](README.md) for the benchmark interface and this file for
dataset coverage and setup.

## Workload Sources

All workloads in this repository are imported or adapted from existing sources.
The local files may be converted to PostgreSQL syntax, split into one query per
file, or regenerated as smaller self-contained subsets.

| Local dataset | Upstream source | Related paper | Local adaptation |
| --- | --- | --- | --- |
| `job` | [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark), using the IMDB CSV data referenced by the JOB artifact | [How Good Are Query Optimizers, Really?](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) | PostgreSQL schema/load scripts and per-query files are kept locally. |
| `job_complex` | [DataManagementLab/JOB-Complex](https://github.com/DataManagementLab/JOB-Complex) | [JOB-Complex: A Challenging Benchmark for Traditional & Learned Query Optimization](https://arxiv.org/abs/2507.07471) | The combined SQL file is split into per-query files and run on the shared IMDB schema. |
| `imdb_ceb_3k` | [learnedsystems/CEB](https://github.com/learnedsystems/CEB) | [Flow-Loss: Learning Cardinality Estimates That Matter](https://arxiv.org/abs/2101.04964) | The CEB IMDB 3k subset is stored as executable SQL query files under the shared IMDB schema. |
| `sqlite_select5` | SQLite upstream `test/select5.test` in the [SQLite source tree](https://www.sqlite.org/src/dir?name=test) | No dedicated paper source is used here; this is an SQLite regression/stress test. | Converted from SQLite test format to PostgreSQL schema/load/query SQL. |
| `gpuqo_chain_small`, `gpuqo_clique_small`, `gpuqo_star_small`, `gpuqo_snowflake_small` | [mageirakos/postgres-gpuqo](https://github.com/mageirakos/postgres-gpuqo) | Project-related papers: [Efficient Massively Parallel Join Optimization for Large Queries](https://arxiv.org/abs/2202.13511), [Efficient GPU-accelerated Join Optimization for Complex Queries](https://mageirakos.github.io/publication/2022-gpuqo-demo) | Small deterministic PostgreSQL subsets generated from the GPUQO synthetic workload shapes. |

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

## Workload Overview

| Dataset | Queries | Join Size | Data | Role |
| --- | ---: | --- | --- | --- |
| `job` | 113 | 4-17 | external IMDB CSV | classic optimizer baseline with realistic correlations |
| `job_complex` | 30 | 6-16 | external IMDB CSV | harder predicates, non-PK/FK joins, and challenging join-order choices |
| `imdb_ceb_3k` | 3,133 | 6-16 | external IMDB CSV | large-volume CEB coverage for long validation campaigns |
| `sqlite_select5` | 732 | 4-64 | self-contained | high-width join stress without external data files |
| `gpuqo_chain_small` | 150 | 2-16 | self-contained | chain-shaped join graphs |
| `gpuqo_clique_small` | 150 | 2-16 | self-contained | dense clique-shaped join graphs |
| `gpuqo_star_small` | 150 | 2-16 | self-contained | star-shaped join graphs |
| `gpuqo_snowflake_small` | 390 | 2-40 | self-contained | wider snowflake-shaped join graphs |

## Scenario Coverage

Scenario-level coverage is documented in [SCENARIOS.md](SCENARIOS.md).  This
file only records dataset sources, local adaptations, data requirements, and
query-count metadata.

## Prepare Commands

Prepare `main`:

```bash
python3 bench/bench.py prepare main --csv-dir "$(pwd)/data/imdb_csv"
```

Prepare `extended`:

```bash
python3 bench/bench.py prepare extended --csv-dir "$(pwd)/data/imdb_csv"
```

Prepare `full`:

```bash
python3 bench/bench.py prepare full --csv-dir "$(pwd)/data/imdb_csv"
```

## Query Manifest

The global query manifest is [meta/query_manifest.csv](meta/query_manifest.csv).
Refresh and verify it with:

```bash
python3 tools/build_query_manifest.py --verify --summary
```
