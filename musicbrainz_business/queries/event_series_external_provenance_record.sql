-- Query: event_series_external_provenance_record
-- Question: Which typed live events have a venue, explicit performer, part-of series membership, and a typed external event endpoint?
-- Inclusion rule: The event-series relationship must mean part-of, and the artist-event relationship must be an explicit performance role. The external endpoint type is evidence, not an official-status claim.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."event" AS v001,
  musicbrainz."event_type" AS v002,
  musicbrainz."l_event_place" AS v003,
  musicbrainz."place" AS v004,
  musicbrainz."place_type" AS v005,
  musicbrainz."area" AS v006,
  musicbrainz."l_artist_event" AS v007,
  musicbrainz."link" AS v008,
  musicbrainz."link_type" AS v009,
  musicbrainz."artist" AS v010,
  musicbrainz."artist_type" AS v011,
  musicbrainz."artist_credit_name" AS v012,
  musicbrainz."artist_credit" AS v013,
  musicbrainz."l_event_series" AS v014,
  musicbrainz."link" AS v015,
  musicbrainz."link_type" AS v016,
  musicbrainz."series" AS v017,
  musicbrainz."series_type" AS v018,
  musicbrainz."l_event_url" AS v019,
  musicbrainz."link" AS v020,
  musicbrainz."link_type" AS v021,
  musicbrainz."url" AS v022
WHERE
  v001."type" = v002."id"
  AND v003."entity0" = v001."id"
  AND v003."entity1" = v004."id"
  AND v004."type" = v005."id"
  AND v004."area" = v006."id"
  AND v007."entity1" = v001."id"
  AND v007."entity0" = v010."id"
  AND v007."link" = v008."id"
  AND v008."link_type" = v009."id"
  AND v010."type" = v011."id"
  AND v012."artist" = v010."id"
  AND v012."artist_credit" = v013."id"
  AND v014."entity0" = v001."id"
  AND v014."link" = v015."id"
  AND v015."link_type" = v016."id"
  AND v014."entity1" = v017."id"
  AND v017."type" = v018."id"
  AND v019."entity0" = v001."id"
  AND v019."link" = v020."id"
  AND v020."link_type" = v021."id"
  AND v019."entity1" = v022."id"
  AND v009."gid" IN ('936c7c95-3156-3889-a062-8a0cd57f8946'::uuid, '492a850e-97eb-306a-a85e-4b6d98527796'::uuid, '292df906-98a6-307e-86e8-df01a579a321'::uuid, '92873f0d-12a7-4fb3-9eac-ff06c38c6a60'::uuid, '9b2d5b96-b4d9-4bce-b056-c369ced25e81'::uuid, '2c5c92da-259c-42fc-ac1f-53d9dda2d6d0'::uuid)
  AND v016."gid" = '707d947d-9563-328a-9a7d-0c5b9c3a9791'::uuid;
