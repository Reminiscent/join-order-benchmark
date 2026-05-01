# Scenarios

This document describes the built-in benchmark scenarios.  The top-level README
keeps only the reviewer entry points.

Scenario definitions are stored in [config/scenarios.toml](config/scenarios.toml).
Dataset-level query counts and join sizes come from
[meta/query_manifest.csv](meta/query_manifest.csv).

## Built-In Scenario Layers

| Scenario | When to use it | Included datasets |
| --- | --- | --- |
| `main` | First validation run for a new join-order algorithm | `job`, `job_complex` |
| `extended` | Broader planning/search-space validation after `main` looks good | `main` plus `sqlite_select5`, `gpuqo_chain_small`, `gpuqo_clique_small`, `gpuqo_star_small`, `gpuqo_snowflake_small` |
| `full` | Complete built-in campaign | `extended` plus `imdb_ceb_3k` |

## `main`

`main` is the primary public validation path.  It runs the complete IMDB-backed JOB
and JOB-Complex workloads:

| Dataset | Queries | Join Size | Data |
| --- | ---: | --- | --- |
| `job` | 113 | 4-17 | external IMDB CSV |
| `job_complex` | 30 | 6-16 | external IMDB CSV |

Use this scenario first when evaluating a new algorithm.  It keeps the campaign
small enough for iteration while still covering realistic join-order choices.

## `extended`

`extended` keeps all `main` datasets and adds self-contained planning-stress
workloads:

| Dataset | Queries | Join Size | Data | Role |
| --- | ---: | --- | --- | --- |
| `sqlite_select5` | 732 | 4-64 | self-contained toy data | high-width join stress converted from SQLite select5 |
| `gpuqo_chain_small` | 150 | 2-16 | self-contained synthetic data | chain-shaped join graph stress |
| `gpuqo_clique_small` | 150 | 2-16 | self-contained synthetic data | dense clique-shaped join graph stress |
| `gpuqo_star_small` | 150 | 2-16 | self-contained synthetic data | star-shaped join graph stress |
| `gpuqo_snowflake_small` | 390 | 2-40 | self-contained synthetic data | wider snowflake-shaped join graph stress |

These extra workloads are adapted from existing upstream sources rather than
invented locally.  They intentionally use small local data:

- `sqlite_select5` has 64 tables with 10 rows per table.
- `gpuqo_chain_small`, `gpuqo_clique_small`, and `gpuqo_star_small` have 40
  tables with 200 rows per table.
- `gpuqo_snowflake_small` uses generated snowflake tables with 20 to 200 rows
  per table.

Because the data is small and many queries are wide, this layer is mainly for
planning-time and join-search-space validation.  Execution-time results from
these workloads are diagnostic only and should not be presented as realistic
end-to-end workload performance.

Some planning-stress workloads are intentionally not run with every variant.
Dynamic programming can become prohibitively slow on dense wide joins; since the
purpose of this layer is to stress planner behavior rather than prove execution
performance on synthetic data, the current `extended` scenario limits `dp` on
`gpuqo_clique_small` to queries with `join_size <= 12`.  Non-`dp` variants run
the complete 150-query clique set.

## `full`

`full` runs everything in `extended` and adds the CEB IMDB 3k subset:

| Dataset | Queries | Join Size | Data | Role |
| --- | ---: | --- | --- | --- |
| `imdb_ceb_3k` | 3,133 | 6-16 | external IMDB CSV | large-volume CEB coverage for long validation campaigns |

`imdb_ceb_3k` has much higher query volume than the other workloads and can
dominate campaign time.
