# Synthetic join-order implementation

This file is a code map. To understand benchmark semantics, start with the
[workload model](../../synthetic-join-order/README.md); to review the
measurement protocol and claims, read the
[design](../../SYNTHETIC_JOIN_ORDER_BENCHMARK.md). Read this package only when
reviewing implementation.

The implementation flows as follows:

```text
generate graph -> compute subset rows -> compile PostgreSQL case
                                      -> plan and qualify one algorithm
                                      -> compare algorithms for one case
                                      -> repeat across matrix -> report
```

| File | Responsibility |
| --- | --- |
| `graph.py` | Generate one deterministic random-tree cardinality graph. |
| `cardinality.py` | Compute join-order-independent subset cardinalities. |
| `compile.py` | Emit the graph reading view, empty schema, statistics, cardinality metadata, and query. |
| `plan.py` | Install a case and collect PostgreSQL `EXPLAIN` evidence. |
| `qualify.py` | Check the exhaustive golden trace or bounded matrix evidence. |
| `run.py` | Plan one case with all eligible algorithms and compute Total Cost Q. |
| `matrix.py` | Repeat the same case protocol across sizes and seeds. |
| `report.py` | Derive qualification/review views and the concise Markdown report. |

The public entry points are the scripts under `tools/`; Section 9 of the design
document gives concise commands.
