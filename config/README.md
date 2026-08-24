# Benchmark Configuration

`variants.toml` defines all algorithm variants used by the runner.  A variant
can represent a join-order algorithm or the same algorithm with different
parameters.  Edit this file to add algorithms or parameter settings, then pick
the desired subset with `--variants`.

This is the checked-in runtime registry, not a sample file.  Both the regular
benchmark runner and the synthetic join-order matrix runner load selected entries from it.

Variants marked with `baseline = true` are used when `--variants` is omitted.
They are also the reviewer-table references when they are part of a run.
The checked-in file marks `dp` and `geqo` as baselines.

`benchmark_settings.toml` is the shared run-settings file.  Each top-level entry
is a session GUC applied to every variant, before variant-specific GUCs.  Use it
for run-protocol settings that should stay identical across algorithm
comparisons.

Default shared settings:

```toml
statement_timeout = 600000
join_collapse_limit = 100
max_parallel_workers_per_gather = 0
work_mem = "1GB"
effective_cache_size = "8GB"
```

Use configured variants with:

```bash
python3 bench/bench.py list variants
python3 bench/bench.py run main --variants dp,geqo,goo_cost
```

Variant entries use these fields:

- `name`
  Stable variant id used by CLI arguments.
- `label`
  Human-readable label used in reviewer tables.
- `baseline`
  Optional boolean.  Baseline variants are the default run selection and the
  ratio references in reviewer tables.
- `session_gucs`
  Variant-specific session-level PostgreSQL parameters.  Every listed GUC for
  a selected variant must exist on the target PostgreSQL server.  Variant
  entries are trusted benchmark configuration and are applied after common
  settings, so an intentional duplicate overrides the common value.  The run
  metadata records the effective configuration inputs for review and replay.

The checked-in variants assume PostgreSQL's default `geqo=on` and that extension
join searches are disabled by default. They contain only the settings normally
required to select each algorithm. If a server changes those defaults, add the
corresponding explicit overrides to the affected variant entries before
running it.

Put a setting in `benchmark_settings.toml` only when it should apply to every
variant in the run.
