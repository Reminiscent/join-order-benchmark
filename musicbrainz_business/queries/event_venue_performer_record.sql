-- Query: event_venue_performer_record
-- Question: Which typed events can be published with a typed venue, containing area, typed performer, performer relationship definition, and a catalog credit using that performer?
-- Inclusion rule: The venue and performer are both required parts of this export record. Recording, annotation, and tag evidence are not required because they are separate optional event facets. The artist-event relationship must use an explicit performance role; non-performance contributors are excluded.
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
  musicbrainz."artist_credit" AS v013
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
  AND v009."gid" IN ('936c7c95-3156-3889-a062-8a0cd57f8946'::uuid, '492a850e-97eb-306a-a85e-4b6d98527796'::uuid, '292df906-98a6-307e-86e8-df01a579a321'::uuid, '92873f0d-12a7-4fb3-9eac-ff06c38c6a60'::uuid, '9b2d5b96-b4d9-4bce-b056-c369ced25e81'::uuid, '2c5c92da-259c-42fc-ac1f-53d9dda2d6d0'::uuid);
