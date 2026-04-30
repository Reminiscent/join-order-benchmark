# Examples

`variants.toml` is the default variant file used by `bench.py`.  It keeps the
portable `dp` and `geqo` baselines separate from other experiment-specific
variants such as GOO or hybrid search settings.

The built-in scenarios default to the portable baselines.  Pass `--variants`
when a submitted benchmark compares additional algorithms.

Use a custom variant file with:

```bash
python3 bench/bench.py list variants --variants-file path/to/variants.toml
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,geqo,my_algo
```
