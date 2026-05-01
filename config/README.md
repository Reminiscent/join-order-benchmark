# Config

This directory contains built-in benchmark scenario definitions.

## `scenarios.toml`

`scenarios.toml` defines the built-in benchmark scopes: `main`, `extended`, and
`full`.

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
  the runner executes `VACUUM (FREEZE, ANALYZE)` on prepared benchmark tables
  before measurement so visibility-map and statistics state are refreshed.
- `variant_order_mode`
  Variant execution order.  Use `rotate` for benchmark runs so each variant
  appears in different positions across query groups.
- `session_gucs`
  Scenario-level session settings applied before variant-level settings.
- `[[scenario.<name>.dataset]]`
  Dataset entries selected by the scenario.  Entries may include `max_join`, a
  per-entry `variants` subset, or `exclude_variants`.  Use `exclude_variants`
  for "all selected variants except these"; for example,
  `gpuqo_clique_small` runs non-`dp` variants fully while `dp` is limited to
  `join_size <= 12`.

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
