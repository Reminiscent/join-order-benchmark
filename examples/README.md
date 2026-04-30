# Examples

`variants.toml` is the default variant file used by `bench.py`.  It documents
the algorithm parameters used by this repository's experiments and can be copied
as a starting point for a custom variant set.

Use a custom variant file with:

```bash
python3 bench/bench.py list variants --variants-file path/to/variants.toml
python3 bench/bench.py run main --variants-file path/to/variants.toml --variants dp,my_algo
```
