-- Query: place_event_series_external_provenance_record
-- Question: Which venue events have an explicit performer, part-of series membership, and typed external endpoints for place, event, and performer?
-- Inclusion rule: Place membership is direct, artist-event is restricted to live-performance roles, event-series means part-of, and each endpoint remains an independently typed reference.
SELECT count(DISTINCT (v001."id", v008."id"))
FROM
  musicbrainz."place" AS v001,
  musicbrainz."place_type" AS v002,
  musicbrainz."area" AS v003,
  musicbrainz."area_type" AS v004,
  musicbrainz."l_event_place" AS v005,
  musicbrainz."link" AS v006,
  musicbrainz."link_type" AS v007,
  musicbrainz."event" AS v008,
  musicbrainz."event_type" AS v009,
  musicbrainz."l_artist_event" AS v010,
  musicbrainz."artist" AS v011,
  musicbrainz."artist_type" AS v012,
  musicbrainz."link" AS v013,
  musicbrainz."link_type" AS v014,
  musicbrainz."l_event_series" AS v015,
  musicbrainz."link" AS v016,
  musicbrainz."link_type" AS v017,
  musicbrainz."series" AS v018,
  musicbrainz."series_type" AS v019,
  musicbrainz."l_place_url" AS v020,
  musicbrainz."link" AS v021,
  musicbrainz."link_type" AS v022,
  musicbrainz."url" AS v023,
  musicbrainz."l_event_url" AS v024,
  musicbrainz."link" AS v025,
  musicbrainz."link_type" AS v026,
  musicbrainz."url" AS v027,
  musicbrainz."l_artist_url" AS v028,
  musicbrainz."link" AS v029,
  musicbrainz."link_type" AS v030,
  musicbrainz."url" AS v031
WHERE
  v001."type" = v002."id"
  AND v001."area" = v003."id"
  AND v003."type" = v004."id"
  AND v005."entity1" = v001."id"
  AND v005."link" = v006."id"
  AND v006."link_type" = v007."id"
  AND v005."entity0" = v008."id"
  AND v008."type" = v009."id"
  AND v010."entity1" = v008."id"
  AND v010."entity0" = v011."id"
  AND v011."type" = v012."id"
  AND v010."link" = v013."id"
  AND v013."link_type" = v014."id"
  AND v015."entity0" = v008."id"
  AND v015."link" = v016."id"
  AND v016."link_type" = v017."id"
  AND v015."entity1" = v018."id"
  AND v018."type" = v019."id"
  AND v020."entity0" = v001."id"
  AND v020."link" = v021."id"
  AND v021."link_type" = v022."id"
  AND v020."entity1" = v023."id"
  AND v024."entity0" = v008."id"
  AND v024."link" = v025."id"
  AND v025."link_type" = v026."id"
  AND v024."entity1" = v027."id"
  AND v028."entity0" = v011."id"
  AND v028."link" = v029."id"
  AND v029."link_type" = v030."id"
  AND v028."entity1" = v031."id"
  AND v014."gid" IN ('936c7c95-3156-3889-a062-8a0cd57f8946'::uuid, '492a850e-97eb-306a-a85e-4b6d98527796'::uuid, '292df906-98a6-307e-86e8-df01a579a321'::uuid, '92873f0d-12a7-4fb3-9eac-ff06c38c6a60'::uuid, '9b2d5b96-b4d9-4bce-b056-c369ced25e81'::uuid, '2c5c92da-259c-42fc-ac1f-53d9dda2d6d0'::uuid)
  AND v017."gid" = '707d947d-9563-328a-9a7d-0c5b9c3a9791'::uuid;
