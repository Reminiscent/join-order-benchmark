# Benchmark Configuration

`variants.toml` defines all algorithm variants used by the runner.  A variant
can represent a join-order algorithm or the same algorithm with different
parameters.  Edit this file to add algorithms or parameter settings, then pick
the desired subset with `--variants`.

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
  a selected variant must exist on the target PostgreSQL server.

Put a setting in `benchmark_settings.toml` only when it should apply to every
variant in the run.
