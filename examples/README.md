# Examples

The portable `dp` and `geqo` baseline variants are built into `bench.py`.
`variants.toml` is an example extra variant file.  Extra variants can represent
additional algorithms or alternate parameter settings, such as GOO or hybrid
search settings.

The built-in scenarios default to the portable baselines.  The CLI automatically
loads `examples/variants.toml` when this file exists, so pass `--variants-file`
only when using a different TOML file.  To change the default extra variants,
edit `examples/variants.toml` directly.

Use the default extra variants with:

```bash
python3 bench/bench.py list variants
python3 bench/bench.py run main --variants dp,geqo,goo_cost
```

Each `[[variant]]` entry supports:

- `name`
  Stable variant id used by CLI arguments.
- `label`
  Human-readable label used in reviewer tables.
- `session_gucs`
  Required session-level PostgreSQL parameters for the variant.  Every listed
  GUC must exist on the target PostgreSQL server.

Patched builds should keep new algorithm switches disabled by default.  Enable
or tune them explicitly in `session_gucs` for the variants that need them.
