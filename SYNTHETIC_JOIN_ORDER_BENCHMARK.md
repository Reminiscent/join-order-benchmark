# Synthetic Join-Order Benchmark Design

## How to read this document

This document is the measurement contract, not an implementation walkthrough.
A PostgreSQL optimizer reader can follow it in three passes:

1. Sections 1–4 explain the question, generated workload, cardinality oracle,
   and SQL realization. No Python or PostgreSQL patch code is required.
2. Sections 5–7 explain what is controlled, how a plan is qualified, and how
   Total Cost Q is interpreted.
3. Sections 8–9 define durable evidence, claim limits, and reproduction
   commands.

Read code only to review an implementation boundary. The
[workload README](synthetic-join-order/README.md) gives the model with the
`golden_n5` example; the
[implementation map](bench/synthetic_join_order/README.md) points to the
relevant Python entry points; the [patch guide](patches/README.md) explains how
to review exactcard without reading its email patch linearly.

## 1. Question and limits

The benchmark asks one question:

> Given the same query, subset cardinalities, PostgreSQL cost model, and
> physical-planning settings, which join-order search algorithm finds the
> lowest-cost plan?

PostgreSQL root `Total Cost` is the only quality objective. Queries are planned
with `EXPLAIN`; they are never executed. Cardinality-shape statistics are
diagnostics, not ranking metrics.

The benchmark's claim boundary is summarized in Section 8.

The generation distributions and root-balance limit are fixed by the benchmark
code. Changing them changes the workload and must be reported with a new
benchmark revision.

## 2. Evidence chain

```text
spec(n, graph_seed, cardinality_seed)
    -> deterministic random tree and cardinality graph
    -> graph.md for inspection; graph.json as the logical authority
    -> empty tables, restored statistics, provider metadata, and query.sql
    -> formula exactcard supplies subset row estimates
    -> each algorithm plans under the same PostgreSQL settings
    -> compare root Total Cost
```

`graph_seed` controls topology. `cardinality_seed` independently controls base
rows, edge types, initial key overlap, and endpoint key multiplicities.
The generator checks deterministic regeneration, connectivity, acyclicity, and
absence of duplicate or self edges.

Topology uses a uniformly sampled Prüfer sequence of length `n-2`. Every such
sequence identifies one labeled tree, so the generator can produce chains,
stars, and irregular branching trees while always returning a connected,
acyclic graph with exactly `n-1` edges. Uniform sequences are uniform over
labeled trees, not over unlabeled shapes. Prüfer was chosen because this
contract is direct, deterministic, and needs no rejection or graph repair; it
is a benchmark implementation choice rather than a disclosed part of the
LinDP generator.

The evaluation lanes are:

```text
threshold:  every n from 12 through 20
large:      n = 20,30,...,100 (20 is shared)
DP:         n <= 20
instances:  100 per size
seeds:      graph 200..299, cardinality 1200..1299
```

All algorithms use the same instances. No instance is removed because of its
winner, cost spread, or clamp behavior.

## 3. Cardinality generation

### Base rows

Base rows are rounded log-uniform values in `[10^3,10^6]`:

```text
u ~ uniform [0,1)
base_rows = round(10^3 * (10^6 / 10^3)^u)
```

They represent effective post-filter inputs; the benchmark emits no explicit
base-table filters. The three-decade range preserves relative scale variation
while avoiding very small relations that would make contracting intermediates
repeatedly clamp to one row.

### Edge cardinality model

The model separates each endpoint's key repetition from cross-input key
overlap:

```text
left/right key NDV (D_left, D_right)
    distinct Join keys modeled in each endpoint column

key_overlap_fraction (q)
    effective fraction of the smaller key set that matches the other endpoint
```

Under the uniform key model:

```text
matching keys = q × min(D_left, D_right)

join rows
    = matching keys
      × (left_rows / D_left)
      × (right_rows / D_right)
    = left_rows × right_rows × q / max(D_left, D_right)

selectivity = q / max(D_left, D_right)
```

The matching-key count is effective and need not be an integer. No independent
Join output factor is generated.

Ordinary per-column `pg_stats` can describe each endpoint NDV, but cannot
describe the cross-table overlap `q`. The realization therefore uses the
values as follows:

```text
left/right key NDV
    -> the corresponding endpoint's n_distinct and representative MCV frequency
    -> native equality and Hash Join bucket costing

q / max(D_left, D_right)
    -> logical edge selectivity
    -> exactcard subset cardinalities and Join output rows
```

For `one_to_many`, the smaller endpoint has multiplicity one and the larger
endpoint repeats keys. Full overlap preserves the repeated endpoint's rows;
partial overlap keeps fraction `q`. For `many_to_many`, both endpoints repeat
keys with independently varied multiplicity. Repetition can expand the Join
while partial overlap can contract it.

The generated SQL uses ordinary equality columns rather than constraints,
indexes, or explicit filters. The family names therefore describe generated
key multiplicity, not declared SQL relationships. `one_to_many` roughly
corresponds to the paper's PK/FK-style family, and `many_to_many` to its
FK/FK-style family.

Each of the tree's `n-1` edges independently draws:

```text
80% one_to_many
    smaller endpoint multiplicity = 1
    larger endpoint multiplicity > 1 and varies independently
    20% initial key overlap = 1
    80% initial key overlap ~ log-uniform [0.1,1]

20% many_to_many
    both endpoint multiplicities > 1 and vary independently
    initial key overlap ~ log-uniform [0.03,1]
```

Endpoint multiplicity varies by up to `8×` beyond the minimum needed to keep
both endpoint key domains within the smaller relation's row count. Integer
endpoint NDVs are derived from those sampled multiplicities.

The edge-family mix and numeric ranges are benchmark-owned approximations
informed by effective matches and equality joins observed in JOB. They are not
parameters specified by the JOB papers, and this benchmark does not claim to
reproduce JOB or the paper's generator.

### Root balance

Independent edge effects multiply. At large n, an enormous common root cost
can dominate all order-sensitive intermediate costs, making algorithms
numerically indistinguishable. The generator corrects this query-level failure
mode after the complete initial graph is generated.

All calculations use log space. Conceptually:

```text
typical_base = geometric_mean(base_rows)
initial_selectivity(edge)
    = initial_key_overlap(edge)
      / max(left_key_ndv(edge), right_key_ndv(edge))
raw_root     = product(base_rows) * product(initial_selectivities)
raw_average_growth = (raw_root / typical_base) ^ (1 / (n-1))

G = 1.20
edge_scale = min(1, G / raw_average_growth)
key_overlap_fraction(edge) = initial_key_overlap(edge) * edge_scale
selectivity(edge)
    = key_overlap_fraction(edge)
      / max(left_key_ndv(edge), right_key_ndv(edge))
```

One scale is applied to every edge. It only reduces effective key overlap,
preserves all relative selectivity ratios, and never rejects or resamples an
instance. The graph stores the left/right key NDVs, final effective
`key_overlap_fraction`, final selectivity, and one query-level `edge_scale`;
initial values are not part of the consumer contract.

The rule does not impose a fixed root row count. It limits geometric average
growth per added relation to `1.20`, so before PostgreSQL row clamping:

```text
final_root <= typical_base * 1.20 ^ (n - 1)
```

The `1.20` limit was chosen to roughly match the root-to-input scale seen in
high-join JOB tests. Its purpose is to prevent a huge common root from masking
the intermediate-cost differences caused by join order.

## 4. Subset oracle and SQL realization

For any subset `S`, the logical row estimate is:

```text
log_rows(S)
    = sum(log(base_rows(r)) for r in S)
      + sum(log(selectivity(e)) for edges inside S)
```

Rows are clamped only when exposed to PostgreSQL. A disconnected subset follows
the same formula and therefore represents the Cartesian product of its
components. Two connected inputs in a tree have one crossing edge, but an
algorithm may first form disconnected inputs and later join them across
multiple edges. The provider derives every joinrel from its complete canonical
subset, independent of either construction path.

Each graph node compiles to an empty table. Every incident edge gets a distinct
`bigint` column, and every graph edge emits exactly one equality predicate.
There are no rows, indexes, constraints, explicit join parentheses, or shared
columns that could add access paths or inferred edges.

The compiler restores base `reltuples` and uses a fixed virtual I/O model of
200 rows per page:

```text
relpages = max(1, ceil(base_rows / 200))
```

This is deliberately not an estimate of PostgreSQL heap tuple layout. Keeping
the virtual row width constant prevents graph degree from implicitly changing
base-table I/O cost. The compiler also restores these column statistics:

```text
null_frac = 0
avg_width = 8
n_distinct = D
one representative MCV with frequency 1 / D
```

Formula exactcard replaces each joinrel's row estimate with the subset oracle.
The final selectivity is the logical cardinality authority. Each endpoint key
NDV remains the corresponding column-statistics input used by native equality
and Hash Join costing.

## 5. Controlled PostgreSQL comparison

Every plan begins with `RESET ALL`. The benchmark then applies the session
settings and provider semantic required for a bounded, deterministic comparison:

```text
join_collapse_limit = 10000
from_collapse_limit = 10000
max_parallel_workers_per_gather = 0
jit = off
enable_mergejoin = off
exactcard.mode = formula
formula path-cost fuzz factor = 1.0
```

Hash Join and Nested Loop retain PostgreSQL defaults. Merge Join is disabled
because exactcard does not calculate candidate rows before remaining join
filters are applied. This keeps the provider small and its costs consistent.
While formula mode is active, the benchmark-only provider also changes
`add_path()` dominance from PostgreSQL's normal `1.01` fuzz factor to exact
`1.0` dominance. This preserves sub-1% cost differences and strengthens the DP
anchor, at the cost of potentially retaining more paths. The value is fixed
provider behavior rather than another GUC and is recorded explicitly in
`run.json`. Off mode retains PostgreSQL's standard `1.01`.

The schema has no indexes and the query has no ordering requirement. Other
planner, cost, memory, and GEQO settings use the server defaults.
The added join-search algorithms default to disabled and are enabled only by
their explicit entries in `config/variants.toml`.

The current fork dispatches join search through `geqo_threshold`: below it,
PostgreSQL uses standard DP; at or above it, an enabled extension search takes
precedence, followed by GEQO. DP uses `geqo_threshold=100` and is scheduled only
through n=20. GEQO and extension variants use a low threshold; extension
activation remains in `config/variants.toml`.

When `--variants` is omitted, both runners select entries marked
`baseline = true`. A size runs only when at least two selected algorithms are
eligible; skipped sizes are recorded, and an entirely ineligible request fails
before filesystem or database mutation. The checked-in variants assume
`geqo=on` and disabled extension searches by default.

The formula provider computes each joinrel directly from its canonical subset,
so rows do not depend on the input split that first constructs it. It also
supplies the output-sensitive tuple counts used by Hash Join. Nested Loop
continues to charge for every candidate input pair because that is the work it
performs before applying the join quals. Native statistics derived from `D`
continue to drive Hash bucket estimates. The compared value is PostgreSQL's
resulting root `Total Cost`, not a separate cost model.

For a multi-crossing Hash Join, formula mode requires every graph edge crossing
the two inputs to appear as a Hash clause. This keeps Hash output-tuple costing
equal to the complete subset oracle while still allowing LinDP to form
disconnected interval subproblems. Single-crossing candidates retain the more
detailed bucket/MCV trace checks.

## 6. Qualification

Qualification has two deliberately different tiers.

### Golden trace

A fixed small graph uses exhaustive offline checks and PostgreSQL trace audit:

- deterministic graph-to-SQL recompilation;
- exact connected-subset formula and recurrence checks;
- restored base rows/pages and column statistics;
- every traced join record agrees with its subset oracle;
- every traced single-crossing Hash candidate has the expected inputs, rows,
  selectivity, and bucket/MCV values;
- multi-crossing Hash joins contain exactly the graph edges between their
  inputs, and output-sensitive tuple costing uses the complete subset
  cardinality;
- root rows agree with the full-subset oracle.

This proves the provider/compiler integration in a bounded case. It is not run
exhaustively at n=100.

### Matrix summary

Every successful matrix plan uses bounded summary evidence and checks:

- the provider is active for the expected instance;
- all base rows/pages and restored statistics match the graph;
- provider join activity is nonzero;
- provider and selected-plan root rows match the full-subset oracle;
- the selected plan is a complete binary join tree over every base relation.

The matrix does not trace every visited joinrel. Qualification errors fail
closed; timeout and aggregation behavior are defined below.

## 7. Metrics and DP boundary

For each successful algorithm `A`:

```text
best_cost = minimum successful Total Cost for the instance
Q(A)      = Total Cost(A) / best_cost
```

The report places every algorithm run for the instance in the same comparison
and shows p50, p95, and max Q by relation count. Max is the worst observed fixed
sample, not a population bound. Shared root and base work remains in Total Cost
and can dilute order-sensitive differences.

If one algorithm times out, `quality.csv` retains the successful algorithms and
their Q values relative to the lowest successful cost for that instance; the
timed-out algorithm has no Q. Headline p50/p95/max tables exclude that instance
for every algorithm scheduled at the same relation count, so each displayed
column uses the same instance population. The report shows that common-success
sample count for every relation count. The coverage section reports timeouts
separately rather than inventing a cost for a missing plan.

At n<=20, PostgreSQL DP is the exhaustive join-order anchor, not an independent
oracle over every physical path. PostgreSQL still prunes a Pareto set within
each joinrel before `set_cheapest()` chooses a path. Formula rows remove one
important source of inconsistency by making a joinrel's cardinality depend only
on its subset rather than the input pair that first created it.

With subset-invariant formula rows, a heuristic should not be materially
cheaper than DP. The runner records such anomalies without discarding them or
failing the run. Locate one by its `n` and seeds in `quality.csv`, regenerate it,
and investigate it before making a claim relative to DP. The runner never
filters instances by winner or cost spread.

## 8. Artifacts and claim boundary

The matrix runner produces only the durable inputs needed for review and
diagnosis:

```text
run.json                  seeds, algorithms, benchmark revision,
                          declared PostgreSQL source revision, live server
                          version, formula path-cost fuzz factor, non-default
                          settings, requested/executed/skipped sizes, timeout,
                          and status
quality.csv               per-instance status, Total Cost, best variant, and Q
workload_diagnostics.csv  compact edge-scale, root-clamp, and selected-plan
                          intermediate-clamp checks; generator details are
                          regenerable from the benchmark revision and seeds
```

The renderer derives the reader-facing `report.md` from `run.json` and
`quality.csv`. Matrix outputs remain local audit inputs; clamp diagnostics stay
in `workload_diagnostics.csv`. Intermediate summary CSVs are not persisted.

`run.json` records the benchmark `HEAD`, declared PostgreSQL source revision,
and live server version. These are concise provenance identifiers, not proof of
the connected server binary or a snapshot of every local/default setting.

The resulting claims are limited to PostgreSQL estimated plan quality on the
qualified random-tree inner-join population under the recorded run
configuration. They do not establish:

- execution-time improvement;
- optimality above n=20;
- behavior for indexes, ordering requirements, outer joins, cyclic graphs, or
  correlated statistics;
- expected GEQO quality across multiple GEQO seeds;
- replacement of real-workload evidence such as JOB or nearby sensitivity
  profiles.

The random-tree, increasing-relation-count stress-test follows Neumann and
Radke, *Adaptive Optimization of Very Large Join Queries* (SIGMOD 2018). This
benchmark does not reproduce their generator. Workload motivation and
cardinality shapes are informed by Leis et al., *How Good Are Query Optimizers,
Really?* (VLDB 2015) and *Query Optimization Through the Looking Glass, and
What We Found Running the Join Order Benchmark* (VLDB Journal 2018). The exact
distributions and root-balance rule remain benchmark-owned choices, with the
provenance and calibration boundary described above.

## 9. Reproduce and inspect

These commands are operational examples; the preceding sections define the
protocol.

Apply the benchmark-only PostgreSQL provider after the evaluated Join Order
patch series:

```bash
git -C /path/to/postgres am \
  /path/to/join_order_benchmark/patches/exactcard-formula.patch
```

Verify the checked-in five-relation case without PostgreSQL:

```bash
python3 tools/generate_synthetic_join_order.py \
  --spec synthetic-join-order/specs/golden_n5.json \
  --output synthetic-join-order/cases/golden_n5 \
  --verify --qualify
```

On a clean benchmark database, run the exhaustive PostgreSQL trace:

```bash
python3 tools/qualify_synthetic_join_order_pg.py \
  --case synthetic-join-order/cases/golden_n5 \
  --db sjo_golden --bootstrap-metadata --audit trace
```

Run a matrix and render its reader-facing report:

```bash
python3 tools/run_synthetic_join_order_matrix.py \
  --db sjo_matrix --bootstrap-metadata \
  --postgres-source /path/to/postgres \
  --sizes 12,13,14,15,16,17,18,19,20 \
  --instances 100 --variants dp,geqo,goo_combined \
  --output outputs/synthetic-main

python3 tools/render_synthetic_join_order_report.py \
  --input outputs/synthetic-main
```

Each database must be clean and dedicated to this benchmark because
`--bootstrap-metadata` creates the shared `bench` schema. Variant names come
from `config/variants.toml`; select at least two algorithms eligible at each
requested size.
