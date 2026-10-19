-- Query: area_live_event_coverage_record
-- Question: Which typed country areas with ISO 3166-1 codes contain a typed venue that hosts a typed event with a typed performer?
-- Inclusion rule: This is a country-level live-event coverage record. ISO 3166-2 and ISO 3166-3 are alternative area classes and are not simultaneous requirements. The artist-event relationship must use an explicit performance role; non-performance contributors are excluded. Area coverage uses direct place.area membership, not transitive containment.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."area" AS v001,
  musicbrainz."area_type" AS v002,
  musicbrainz."country_area" AS v003,
  musicbrainz."iso_3166_1" AS v004,
  musicbrainz."place" AS v005,
  musicbrainz."place_type" AS v006,
  musicbrainz."l_event_place" AS v007,
  musicbrainz."link" AS v008,
  musicbrainz."link_type" AS v009,
  musicbrainz."event" AS v010,
  musicbrainz."event_type" AS v011,
  musicbrainz."l_artist_event" AS v012,
  musicbrainz."artist" AS v013,
  musicbrainz."artist_type" AS v014,
  musicbrainz."link" AS v015,
  musicbrainz."link_type" AS v016
WHERE
  v001."type" = v002."id"
  AND v003."area" = v001."id"
  AND v004."area" = v001."id"
  AND v005."area" = v001."id"
  AND v005."type" = v006."id"
  AND v007."entity1" = v005."id"
  AND v007."link" = v008."id"
  AND v008."link_type" = v009."id"
  AND v007."entity0" = v010."id"
  AND v010."type" = v011."id"
  AND v012."entity1" = v010."id"
  AND v012."entity0" = v013."id"
  AND v013."type" = v014."id"
  AND v012."link" = v015."id"
  AND v015."link_type" = v016."id"
  AND v016."gid" IN ('936c7c95-3156-3889-a062-8a0cd57f8946'::uuid, '492a850e-97eb-306a-a85e-4b6d98527796'::uuid, '292df906-98a6-307e-86e8-df01a579a321'::uuid, '92873f0d-12a7-4fb3-9eac-ff06c38c6a60'::uuid, '9b2d5b96-b4d9-4bce-b056-c369ced25e81'::uuid, '2c5c92da-259c-42fc-ac1f-53d9dda2d6d0'::uuid);
