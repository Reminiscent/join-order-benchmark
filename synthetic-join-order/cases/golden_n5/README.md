# Golden five-relation case

This is the checked-in example generated from
`../../specs/golden_n5.json`. It is both a small explanation case and a
regression fixture for deterministic graph generation and compilation.

Its Prüfer sequence `[2, 4, 1]` produces:

```text
R0 -- R2 -- R4 -- R1 -- R3
```

## How to inspect this case

Start with `graph.md`. It shows the complete topology, node row counts, edge
types, and output factors in one place. Then open `graph.json` when exact
machine-readable values are needed. Finally, compare `query.sql` and
`schema.sql` to see how each node and edge becomes an unparenthesized
PostgreSQL inner Join.

`base_stats.sql` and `cardinality_metadata.sql` carry the same model into
PostgreSQL statistics and subset-cardinality metadata. They are useful for
integration review, but are not required to understand the case.

## One edge in detail

For example, the `R0 -- R2` edge in `graph.json` has:

```text
type                  = one_to_many
left_key_ndv          = 928
right_key_ndv         = 1491
key_overlap_fraction  = 0.0262
selectivity           = key_overlap_fraction / max(left_key_ndv, right_key_ndv)
```

`R2` is the smaller key-like endpoint, with one row for each of its 1,491
keys. `R0` models 928 distinct keys across 631,707 rows, or about 681 rows per
key. The graph-wide root-balance scale reduces this edge's effective overlap
to about 2.62%. This one-to-many Join therefore produces about 2.62% as many
rows as its larger input. `selectivity` is the final value used by the
subset-cardinality formula.

The generated artifacts in this directory should not be edited individually.
`tests.test_synthetic_join_order_offline` verifies that they match a fresh
compilation. The parent [README](../../README.md) explains the generation model
behind this result.
