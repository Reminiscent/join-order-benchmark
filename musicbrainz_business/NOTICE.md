# Upstream attribution

This workload uses table, column, primary-key, and foreign-key facts extracted
from MusicBrainz Server at commit
`cff977f0ba8f06d5fa594e7590f6a134b1a5a22c`.

MusicBrainz Server is maintained by MetaBrainz and its contributors and is
licensed under GNU GPL version 2 or later.  The original source and license are
available at <https://github.com/metabrainz/musicbrainz-server>.  Exact source
paths and checksums are recorded in `source.lock.json`.

The queries are authored for this benchmark from public MusicBrainz entity,
relationship, and foreign-key semantics. They are not MusicBrainz production
queries or a captured production query log.

Official data archives remain outside Git. The pinned core catalog archive is
published under CC0 1.0; the pinned derived annotations, ratings, tags, and
search-derived archive is published under CC BY-NC-SA 3.0. Exact filenames,
sizes, checksums, roles, and source URLs are recorded in
`real_data.lock.json`.
