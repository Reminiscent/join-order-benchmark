# Config

This directory contains built-in benchmark scenario definitions.

## `scenarios.toml`

`scenarios.toml` defines the built-in benchmark scopes: `main`, `extended`, and
`full`.

Think of it as a small manifest:

- `[scenario.main]` starts the definition of a scenario named `main`.
- Scalar fields under that table define the run protocol for that scenario.
- Every `[[scenario.main.dataset]]` block appends one dataset entry to `main`.
- Dataset entries may optionally restrict which variants they apply to or which
  query join sizes are selected.

Minimal shape:

```toml
[scenario.main]
description = "Primary algorithm validation path on complete JOB and JOB-Complex."
default_variants = ["dp", "geqo"]
reps = 3
statement_timeout_ms = 600000
stabilize = "vacuum_freeze_analyze"
variant_order_mode = "rotate"
session_gucs = { join_collapse_limit = 100, max_parallel_workers_per_gather = 0 }

[[scenario.main.dataset]]
name = "job"

[[scenario.main.dataset]]
name = "job_complex"
```

Supported fields per scenario:

- `description`
  Human-readable scenario description shown by `bench.py list scenarios`.
- `default_variants`
  Variant names selected when `bench.py run` does not receive `--variants`.
  The built-in scenarios keep this to the portable `dp` and `geqo` baselines;
  pass `--variants` for patch-specific algorithms.  The names must exist in the
  active variants file.
- `reps`
  Number of measured repetitions per query and variant.
- `statement_timeout_ms`
  Per-statement timeout applied by the runner.
- `stabilize`
  Database stabilization mode before the run.  `vacuum_freeze_analyze` means
  the runner executes `VACUUM FREEZE ANALYZE` on prepared benchmark tables
  before measurement so visibility-map and statistics state are refreshed.
- `variant_order_mode`
  Variant execution order.  Use `rotate` for benchmark runs so each variant
  appears in different positions across query groups.
- `session_gucs`
  Scenario-level session settings applied before variant-level settings.

Supported fields per dataset entry:

- `name`
  Dataset id.  It must be one of `python3 bench/bench.py list datasets`.
- `max_join`
  Optional maximum query join size for this entry.
- `variants`
  Optional allow-list.  The dataset entry runs only for selected variants that
  are also present in this list.
- `exclude_variants`
  Optional deny-list.  The dataset entry runs for all selected variants except
  these.  Do not set both `variants` and `exclude_variants` in the same entry.

The duplicated `gpuqo_clique_small` entry is intentional:

```toml
[[scenario.extended.dataset]]
name = "gpuqo_clique_small"
exclude_variants = ["dp"]

[[scenario.extended.dataset]]
name = "gpuqo_clique_small"
variants = ["dp"]
max_join = 12
```

This means selected non-`dp` variants run the complete clique workload, while `dp`
runs only clique queries with `join_size <= 12`.  That guard keeps the dynamic
programming baseline tractable on the dense clique workload.

## Variants

Variant definitions live outside `config/` because they are expected to be copied
or replaced by users evaluating their own algorithms.

The default example file is [../examples/variants.toml](../examples/variants.toml).
Pass a custom file with:

```bash
python3 bench/bench.py run main --variants-file path/to/variants.toml
```

Supported fields per `[[variant]]` entry:

- `name`
  Stable variant id used by scenarios and CLI arguments.
- `label`
  Human-readable label.
- `session_gucs`
  Required session-level parameters applied for that variant.
- `optional_session_gucs`
  Session-level parameters applied only when the current PostgreSQL build exposes
  that GUC.  Use this for portable baseline cleanup such as explicitly turning a
  custom patch GUC off on patched builds without breaking stock PostgreSQL runs.
