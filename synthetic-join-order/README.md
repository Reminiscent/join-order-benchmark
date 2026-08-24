# Synthetic join-order plan-only benchmark

This directory defines deterministic synthetic workloads for comparing
join-order search inside PostgreSQL. Each generated case is qualified and
planned with `EXPLAIN`; queries are never executed. The matrix runner repeats
the same case protocol across relation counts, seeds, and algorithm variants.
The
[design document](../SYNTHETIC_JOIN_ORDER_BENCHMARK.md) defines the complete
measurement protocol and claim boundaries.

## How to read this workload

For a first reading, follow this page from top to bottom. Its sections follow
the generation flow—from the three-field input, through graph and cardinality
generation, to the PostgreSQL realization—and use the checked-in `golden_n5`
case as a running example. No code is required.

Afterward, inspect [`golden_n5`](cases/golden_n5/README.md) to see the complete
generated result:

1. `graph.md` shows the topology, node rows, and important edge values;
2. `graph.json` provides the exact machine-readable graph;
3. `query.sql` and `schema.sql` show how that graph becomes PostgreSQL input.

The remaining generated statistics and metadata are integration artifacts.
Read them, or the Python implementation, only when reviewing the corresponding
boundary:

- graph generation: `graph.py` → `cardinality.py` → `compile.py`;
- PostgreSQL integration: [`patches/README.md`](../patches/README.md) →
  `plan.py` → `qualify.py`;
- matrix policy and reporting: `run.py` → `matrix.py` → `report.py`;
- reproduction and claim boundaries:
  [design Section 9](../SYNTHETIC_JOIN_ORDER_BENCHMARK.md#9-reproduce-and-inspect).

## What the workload models

The workload follows the broad scalability-test method used by Neumann and
Radke: increase the size of random tree-shaped Join graphs and compare the Join
orders found by different algorithms. The exact generator below is a
benchmark-owned controlled model, not a reproduction of an unpublished paper
generator or of JOB.

The logical workload models ordinary equality joins whose key domains can
partially overlap. An edge is defined first by what an algorithm experiences:
adding its child after its parent contracts, preserves, or expands the current
cardinality. PostgreSQL still supplies the only quality objective: root `Total
Cost`.

## Input

One case has only three user-controlled values:

```json
{
  "n": 5,
  "graph_seed": 7,
  "cardinality_seed": 1002
}
```

`n` sets the relation count. `graph_seed` fixes the Prüfer tree.
`cardinality_seed` feeds separate deterministic random streams for base rows,
the anchor, edge roles, growth factors, and overlap. The budget, role policy,
overlap bounds, and anchor policy are code-owned workload constants and are
not configuration.

## 1. Build and orient a random tree

A uniform labeled Prüfer sequence produces a connected acyclic graph with
`n - 1` edges. The generator then chooses one relation uniformly at random as
the generation anchor, independently of topology and base rows, and directs
the tree away from it. Direction exists only in `graph.json` to define growth;
the generated SQL remains one flat undirected inner-Join query.

## 2. Assign base rows

Each relation receives rounded log-uniform rows in `[1,000,1,000,000]`. These
rows provide three decades of cardinality scale. Each relation independently
receives a projected payload width in `[8,256]`; Section 6 explains its
physical realization.

## 3. Assign bounded logical growth

For an oriented edge `parent -> child`:

```text
growth = rows(current JOIN child) / rows(current)
selectivity = growth / child_base_rows
```

The edge roles are:

```text
selective: growth < 1
expanding: growth > 1
```

For more than one edge, an independent fair random sign assigns every edge to
the selective or expanding group. Generation only repairs the degenerate case
where every sign is the same. There is no fixed role ratio and no
exact-neutral quota. At `n = 2`, the single edge directly receives the
query-level root growth.

```text
maximum contraction or expansion log budget = log(100)
```

Each query samples a root growth log-uniformly in `(1/100,100)`. One sign uses
the full budget and the other receives the amount needed to realize that root.
For `n = 2`, the only edge directly receives this factor. For larger queries,
inverse-uniform random weights divide each sign's budget into deliberately
unequal shares. This keeps a few meaningful effects possible at large `n`
instead of mechanically averaging the budget across every edge. Contraction
and expansion allocations are independent, not reciprocal pairs.

```text
product(all growth factors) = root growth
full-query rows = anchor rows * root growth
```

The budget is constructive, not a threshold followed by repair. Generation
never rescales or retries a graph. Validation fails closed if root identity or
connected-subset bounds do not hold.

## 4. Realize growth with partial overlap and NDV

Growth is the logical authority; overlap is a separate physical realization.
For every edge, a target overlap is sampled from the fixed log-uniform interval
ending at `min(1,growth)`. Its lower bound is `0.1`, except that contractions
below `0.1` use their growth as both bounds so the implied NDV remains legal.
Expanding edges can also have partial key-domain overlap.

Conceptually:

```text
effective multiplicity = growth / overlap
child NDV approximately = child rows / effective multiplicity
```

The child NDV is rounded down to a legal repeated-key integer. Parent NDV is
the smaller of that value and `parent_rows - 1`. The final effective overlap is
then recomputed so that this identity is exact:

```text
selectivity = overlap / max(parent_NDV, child_NDV)
            = growth / child_rows
```

This creates generic overlapping equality joins rather than declaring strict
PK/FK constraints. An expanding edge can have partial overlap whose repetition
more than compensates; a selective edge remains net contracting. Endpoint NDVs
continue to drive native PostgreSQL equality and Hash Join bucket costing,
while exactcard uses the logical selectivity.

## 5. Qualify cardinality before PostgreSQL

The complete subset formula is unchanged:

```text
rows(S)
  = product(base rows in S)
    * product(selectivities of edges fully inside S)
```

An O(n) tree dynamic program records and validates the exact minimum and
maximum connected-subset cardinalities. With base rows in
`[1,000,1,000,000]` and budget 100, every connected subset is constructed to
stay within `[10,100,000,000]`. This guarantee does not cover disconnected
Cartesian-product subsets, so selected-plan clamp diagnostics remain in the
matrix output.

## 6. Convert the graph for PostgreSQL

PostgreSQL cannot plan `graph.json` directly. The compiler maps every graph
node to one empty table, every graph edge to one pair of ordinary equality
columns, and the whole graph to one flat inner-join query. One projected text
payload per relation carries its generated width through every intermediate.
Virtual pages are derived from rows times projected width using one fixed
8192-byte costing unit. The query has no filters, indexes, ordering
requirements, or explicit join parentheses, so it
does not add unrelated access paths or preselect a join order.

The generated artifacts have distinct roles:

- `graph.md` is the generated diagram and edge summary for quick reading;
- `graph.json` is the logical source of node rows, edge behavior, and final
  selectivities;
- `schema.sql` and `query.sql` express the graph as PostgreSQL relations and
  equality predicates;
- `base_stats.sql` supplies relation sizes and uniform join-column statistics;
- `cardinality_metadata.sql` lets the benchmark-only `exactcard` provider map
  PostgreSQL relation subsets back to the graph formula.

The tables stay empty because this is a plan-only benchmark. PostgreSQL still
uses its own physical operators and cost model; `exactcard` only ensures that
the compared algorithms see the graph's construction-independent subset row
counts.

The generation rules and seeds are fixed before algorithm comparison, and
cases are not selected or regenerated based on which algorithm wins. This is a
controlled synthetic workload, not a replacement for separate results on real
workloads such as JOB.
