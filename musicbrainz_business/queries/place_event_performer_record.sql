-- Query: place_event_performer_record
-- Question: Which typed venues in typed areas can be traced through a classified event-place relationship to a typed event and typed performer?
-- Inclusion rule: The output is a venue provenance record. Event and performer relationships are required; place annotations, tags, redirects, and other relationship channels are not. The artist-event relationship must use an explicit performance role; non-performance contributors are excluded.
SELECT count(DISTINCT v001."id")
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
  musicbrainz."link_type" AS v014
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
  AND v014."gid" IN ('936c7c95-3156-3889-a062-8a0cd57f8946'::uuid, '492a850e-97eb-306a-a85e-4b6d98527796'::uuid, '292df906-98a6-307e-86e8-df01a579a321'::uuid, '92873f0d-12a7-4fb3-9eac-ff06c38c6a60'::uuid, '9b2d5b96-b4d9-4bce-b056-c369ced25e81'::uuid, '2c5c92da-259c-42fc-ac1f-53d9dda2d6d0'::uuid);
