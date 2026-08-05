# Benchmark-only PostgreSQL patch

`exactcard-formula.patch` adds the PostgreSQL-side cardinality provider required
by the synthetic join-order benchmark. It is an email patch for the evaluated
PostgreSQL fork, not a proposed upstream feature.

## What to understand

The workload compiler writes two metadata tables. In formula mode the patch
uses them to keep four PostgreSQL inputs consistent:

```text
base relation rows/pages
    -> subset-invariant joinrel rows
    -> Hash Join output tuple counts
    -> structured qualification audit
```

Native PostgreSQL costing and physical operators remain in use. The patch
replaces generated cardinality inputs; it does not implement another cost
model or Join Order algorithm.

## How to review it

Do not read the 1,600-line patch from top to bottom. Review these parts:

1. the email preface and the file header of `exactcard.c` for scope;
2. `exactcard_set_baserel_size()`, `exactcard_set_joinrel_rows()`, and
   `exactcard_get_hashjoin_rows()` for the three planner integration points;
3. `exactcard_last_audit()` for the evidence exposed to qualification;
4. `src/test/regress/sql/exactcard.sql` for the externally checked behavior.

The GUC/catalog/build-file edits are wiring. Read them only when reviewing
PostgreSQL integration details.

## Apply and validate

The expected PostgreSQL base is recorded in the patch preface. From a clean
checkout at that revision:

```bash
git am /path/to/join_order_benchmark/patches/exactcard-formula.patch
git diff --check HEAD^
```

The patch is tied to the evaluated Join Order series and intentionally has no
compatibility layer for other PostgreSQL revisions.
