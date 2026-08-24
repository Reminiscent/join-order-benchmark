# Golden five-relation case

This checked-in case is generated from `../../specs/golden_n5.json`. It is a
small explanation case and a deterministic generation/compilation regression
fixture.

Its Prüfer sequence produces:

```text
R0 -- R2 -- R4 -- R1 -- R3
```

The independent anchor stream selects `R0`, so generation directs the tree
away from `R0`. Direction controls cardinality generation only; `query.sql`
remains a flat inner Join.

Start with `graph.md`. It shows base rows, the anchor, edge directions, logical
growth roles, endpoint NDVs, overlap, and selectivity. `graph.json` is the
machine-readable authority. The SQL and statistics files carry that model into
PostgreSQL.

For example, `R0 -> R2` is expanding:

```text
growth_factor        = 24.7686
child_rows            = 197296
selectivity           = growth_factor / child_rows
left/right NDV        = 1751 / 1751
key_overlap_fraction  = 0.219821
```

The endpoint NDVs imply repeated keys and partial key-domain overlap. Their
realization preserves both identities:

```text
selectivity = overlap / max(endpoint NDVs)
selectivity = growth / child rows
```

Thus adding `R2` to a connected intermediate containing `R0` multiplies rows
by about `24.7686`. This query's root growth is about `0.333341`, so the
complete query has about one third of the random anchor's rows. Cumulative
contraction and expansion remain separately bounded by 100 without reciprocal
edge pairs or post-generation scaling.

The five generated payload widths are `49`, `17`, `160`, `21`, and `244`
bytes. `query.sql` projects them, so PostgreSQL's root Plan Width is their
sum, `491`; base and intermediate widths vary with the relations they contain.

The generated artifacts in this directory should not be edited individually.
`tests.test_synthetic_join_order_offline` verifies exact recompilation.
