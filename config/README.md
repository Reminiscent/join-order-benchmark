# Config

This directory contains the benchmark configuration files.

## Files

`variants.toml`

- defines stable variant names such as `dp`, `geqo`, `hybrid_search`
- maps each variant to the session-level parameters used by the runner

Supported fields per variant:

- `name`
  Stable variant id used by scenarios and CLI arguments.
- `label`
  Human-readable label.
- `session_gucs`
  Session-level parameters applied for that variant.
- `optional_session_gucs`
  Session-level parameters applied only when the current PostgreSQL build exposes that GUC.
  Use this for portable baseline cleanup such as explicitly turning a custom patch GUC off on patched builds
  without breaking stock PostgreSQL runs.

If the implementation under test changes parameter names or adds new parameters, update the corresponding variant entry here.

`scenarios.toml`

- defines built-in benchmark scopes such as `smoke`, `main`, `full`, and `custom`
- selects datasets, default variants, and protocol settings
