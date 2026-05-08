# Examples

The portable `dp` and `geqo` baseline variants are built into `bench.py`.
`variants.toml` is an example extra variant file.  Extra variants can represent
additional algorithms or alternate parameter settings, such as GOO or hybrid
search settings.

The built-in scenarios default to the portable baselines.  The CLI loads
`examples/variants.toml` when this file exists.  To change the default extra
variants, edit `examples/variants.toml` directly.

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
Do not put algorithm-specific switches in `benchmark_settings.toml`.
