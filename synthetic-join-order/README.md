# Synthetic join-order workload model

This directory defines the deterministic workload model used by the plan-only
synthetic join-order benchmark. A minimal spec expands into a logical Join
graph, construction-independent subset cardinalities, and the schema, query,
statistics, and metadata consumed during PostgreSQL planning.

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
boundary. For graph and cardinality generation, follow:

1. `generate_graph()` in `graph.py` expands the three-field spec;
2. `subset_cardinality()` in `cardinality.py` defines the logical oracle;
3. `compile_graph()` in `compile.py` produces the generated files.

`validate_graph()` and the offline tests enforce the same contract; they are
useful for implementation review, but are not additional workload semantics.

## What the workload models

The workload follows the broad scalability-test method used by Neumann and
Radke in
[*Adaptive Optimization of Very Large Join Queries*](../references/lindp/2018-adaptive-optimization-very-large-join-queries.pdf):
generate increasingly large random tree-shaped join graphs, assign relation
cardinalities and mostly PK/FK-style edge behavior, and compare the join trees
found by different algorithms.

Their algorithms consume an abstract query graph directly. This benchmark
first builds the same kind of logical input, then converts it into inputs that
PostgreSQL understands:

```text
small spec
    -> tree-shaped join graph with node rows and edge behavior
    -> graph.md reading view, graph.json, and subset-cardinality formula
    -> PostgreSQL schema, flat query, statistics, and provider metadata
```

The paper does not fully specify its graph generator, distributions, or seeds.
The concrete choices below are therefore reproducible benchmark choices guided
by the paper's workload shape and by cardinality behavior seen in JOB.

This workload controls logical Join cardinalities instead of asking
PostgreSQL's native estimator to derive them. The benchmark is intended to
compare join-order search with the same cardinality input, not to test
cardinality estimation. Native equality estimation cannot directly represent
cross-table key overlap and can make a large random tree contract to repeated
one-row clamps or grow until a common root dominates every plan. The generated
edge model and root-balance rule limit those accidental extremes, while
PostgreSQL statistics continue to drive physical operator costing.

## Input

One case needs only three values:

```json
{
  "n": 5,
  "graph_seed": 7,
  "cardinality_seed": 1002
}
```

- `n` is the number of relations.
- `graph_seed` determines the shape of the tree.
- `cardinality_seed` independently determines node row counts and edge
  cardinality behavior.

The seeds randomly generate the tree shape, base rows, edge family, initial key
overlap, and endpoint key multiplicities. Integer endpoint key NDVs, any
root-balance adjustment, effective key overlap, selectivities, and Join output
rows are calculated from those generated values.

The checked-in `golden_n5` case uses this spec and provides the examples below.

## 1. Build a tree-shaped join graph

The workload intentionally uses a simple tree: it is connected, acyclic, has
`n - 1` edges, and has at most one edge between any pair of relations. It does
not model cycles, multiple predicates between the same relations, hyperedges,
or non-inner joins. This keeps the PostgreSQL realization small while
following the paper's random-tree test shape.

Topology is generated with a
[Prüfer sequence](https://en.wikipedia.org/wiki/Pr%C3%BCfer_sequence). Prüfer was
chosen because it can produce every labeled tree—that is, every tree whose
relations keep distinct identities such as `R0` and `R1`. This includes chains,
stars, and irregular branching trees, and always produces a valid tree without
graph repair or retries. Uniform sequences are uniform over trees with these
relation identities, not over shapes after the identities are ignored.

For `golden_n5`, `graph_seed = 7` produces this chain:

```text
R0 -- R2 -- R4 -- R1 -- R3
```

The graph defines which equality predicates exist. It does not prescribe the
order in which PostgreSQL must join the relations.

## 2. Assign relation rows

Each node receives a `base_rows` value sampled log-uniformly from
`[1000, 1000000]`. Log-uniform sampling makes different orders of magnitude
appear regularly rather than concentrating relations near the upper bound.

```text
R0 = 631707    R1 = 13873    R2 = 1491
R3 = 748299    R4 = 34829
```

The paper says that relation sizes vary but does not publish an exact
range. This three-decade range gives the workload meaningful size variation
without relying on very small or unboundedly large inputs. Very small inputs
make contracting intermediates repeatedly fall below one row and get clamped;
very large intermediate and root rows can dominate PostgreSQL cost and hide
join-order differences. The lower bound reduces accidental one-row clamps, and
the root-balance rule below controls overall growth.

## 3. Assign edge cardinality behavior

An equality Join depends on both repetition within each input and overlap
between the two inputs:

```text
key multiplicity
    average number of rows per distinct key in one input

key overlap
    fraction of distinct keys that can match across the two inputs
```

For example:

```text
left rows = 10,000, left NDV = 1,000
    -> 10 rows per key

right rows = 6,000, right NDV = 2,000
    -> 3 rows per key

key overlap = 0.5
    -> 500 of the smaller key set's 1,000 values match
```

Let `D_left` and `D_right` be the endpoint key NDVs and
`q = key_overlap_fraction`. Assuming uniform frequency:

```text
matching keys      = q × min(D_left, D_right)
left rows per key  = left_rows / D_left
right rows per key = right_rows / D_right

join rows
    = matching keys × left rows per key × right rows per key
    = left_rows × right_rows × q / max(D_left, D_right)

selectivity = q / max(D_left, D_right)
```

The matching-key count is effective and need not be an integer. Join output
rows and the displayed output factor are derived rather than independently
generated.

Separating endpoint NDVs from overlap is also necessary for the PostgreSQL
realization:

```text
left/right key NDV
    -> each endpoint's pg_stats.n_distinct and representative MCV frequency
    -> PostgreSQL equality and Hash Join bucket costing

q / max(left NDV, right NDV)
    -> logical edge selectivity
    -> exactcard subset cardinalities and Join output rows
```

Ordinary per-column `pg_stats` can describe each endpoint's NDV, but it cannot
describe cross-table overlap `q`. Native equality estimation approximately
uses `1 / max(D_left, D_right)`, implicitly treating the smaller key domain as
fully covered. The benchmark-only `exactcard` provider adds `q` for logical row
counts while PostgreSQL continues to use each endpoint NDV for physical
costing.

### `one_to_many`

One endpoint has one row per distinct key, while the other repeats keys. The
generator assigns the unique-key role to the smaller input:

```text
smaller endpoint multiplicity = 1
larger endpoint multiplicity > 1
join output factor relative to the repeated side = q
```

For a 10,000-row key side, a 100,000-row repeated side, and `q = 0.2`,
the Join has 20,000 rows. Full overlap keeps the repeated input's row count;
partial overlap keeps only the matching fraction.

This corresponds roughly to the paper's PK/FK-style family, but the name
describes only the generated key multiplicities. The SQL uses ordinary
equality columns rather than declaring PK/FK constraints.

### `many_to_many`

Both endpoints repeat keys, so neither endpoint is unique. Each endpoint
independently varies its rows per key, while `q` controls how much of the
smaller key set matches. Repetition can expand the Join and partial overlap can
contract it; their combination determines the result. This corresponds
roughly to the paper's less common FK/FK-style family.

The paper describes mostly PK/FK edges and a few FK/FK edges, without exact
probabilities. This benchmark uses the following approximation:

```text
80% one_to_many
    smaller input has one row per key
    larger input repeats keys with independently varied multiplicity
    20% full key overlap
    80% overlap sampled log-uniformly from [0.1, 1]

20% many_to_many
    both endpoints repeat keys with independently varied multiplicity
    key overlap sampled log-uniformly from [0.03, 1]
```

Endpoint multiplicity varies by up to `8×` beyond the minimum needed to keep
both key domains no larger than the smaller relation. This keeps the domains
comparable enough to Join while allowing the two endpoint NDVs to differ.

The 80/20 family split approximates the paper's qualitative edge mix. The
numeric ranges are rough overlap and repetition scales informed by equality
joins seen in
[JOB](../references/job/2015-how-good-are-query-optimizers-really.pdf) and its
[follow-up analysis](../references/job/2018-query-optimization-through-the-looking-glass.pdf).
They model similar orders of magnitude; they do not reproduce individual JOB
queries. Root balance may reduce every edge's sampled overlap by one common
scale; the stored `key_overlap_fraction` is the effective value after that
adjustment.

## 4. Prevent the common root from dominating

Edge effects multiply as more relations are joined. Without a limit, a very
large final result can dominate every plan's `Total Cost`, making different
join orders look nearly identical even when their intermediate results differ.

Root balance therefore limits geometric average growth per added relation to
`1.20`. When a graph exceeds that limit, the generator applies one common
downward scale to every edge's sampled overlap and stores the resulting
effective `key_overlap_fraction`. Key NDVs and repetition stay unchanged,
relative edge strength is preserved, and the case is not regenerated.
The `1.20` scale was chosen to roughly match the root-to-input scale seen in
high-join JOB tests.

## 5. Define cardinality independently of join order

For any relation subset `S`, the logical cardinality is:

```text
rows(S)
  = product(base_rows for relations in S)
    * product(selectivity for edges fully inside S)
```

The formula uses the complete subset, so `{R0,R2,R4}` has the same row count
whether PostgreSQL constructs it as `(R0 join R2) join R4` or
`R0 join (R2 join R4)`. This gives every join-order algorithm the same
cardinality input.

## 6. Convert the graph for PostgreSQL

PostgreSQL cannot plan `graph.json` directly. The compiler maps every graph
node to one empty table, every graph edge to one pair of ordinary equality
columns, and the whole graph to one flat inner-join query. The query has no
filters, indexes, ordering requirements, or explicit join parentheses, so it
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
