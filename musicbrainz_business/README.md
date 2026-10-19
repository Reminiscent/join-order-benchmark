# MusicBrainz Business

Business-oriented wide joins over the public MusicBrainz catalog.

## Summary

- Queries: 44
- Join width: 12 to 51 relation instances
- Distinct tables per query: 12 to 42
- Schema: MusicBrainz Server revision `cff977f0ba8f06d5fa594e7590f6a134b1a5a22c`
- Data: official Full export `20260905-002519`

MusicBrainz supplies the schema, relationship semantics, and data. The SQL was
written for this benchmark from those semantics; it is not a production query
log. Each SQL file starts with its business question and inclusion rule.

## Files

- `queries/`: benchmark SQL.
- `tables.txt`: the 81 tables required by the queries.
- `source.lock.json`: schema source revision and checksums.
- `real_data.lock.json`: data URLs, licenses, sizes, and checksums.
- `download_schema.py`, `download.py`, `load.py`: data setup helpers.

## Setup

Download the pinned schema files and Full archives:

```bash
python3 musicbrainz_business/download_schema.py --output data/musicbrainz/source
python3 musicbrainz_business/download.py mbdump.tar.bz2 --output-dir data/musicbrainz/full
python3 musicbrainz_business/download.py mbdump-derived.tar.bz2 --output-dir data/musicbrainz/full
```

Load the 81 required tables into an empty database:

```bash
python3 musicbrainz_business/load.py \
  --data-mode official_full \
  --archive data/musicbrainz/full/mbdump.tar.bz2 \
  --archive data/musicbrainz/full/mbdump-derived.tar.bz2 \
  --source-dir data/musicbrainz/source \
  --database musicbrainz_bench \
  --output data/musicbrainz/load.json
```

Run one query directly:

```bash
psql -d musicbrainz_bench \
  -f musicbrainz_business/queries/label_publication_lineage_record.sql
```

Or run the registered benchmark scenario:

```bash
python3 bench/bench.py run musicbrainz \
  --variants geqo,goo_combined,goo_combined_lindp \
  --reuse-stats
```

The archives and loaded database are external artifacts and are not stored in
Git.
