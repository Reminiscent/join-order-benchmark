-- Query: place_recording_identifier_provenance_audit
-- Question: Which typed, area-associated places are explicit recorded-at sources for an ISRC recording with performance-linked ISWC work lineage?
-- Inclusion rule: The place-recording relationship is restricted to recorded at; mixing, mastering, production, and other location roles are excluded.
SELECT count(DISTINCT (v014."id", v001."id"))
FROM
  musicbrainz."recording" AS v001,
  musicbrainz."isrc" AS v002,
  musicbrainz."artist_credit" AS v003,
  musicbrainz."artist_credit_name" AS v004,
  musicbrainz."artist" AS v005,
  musicbrainz."l_recording_work" AS v006,
  musicbrainz."link" AS v007,
  musicbrainz."link_type" AS v008,
  musicbrainz."work" AS v009,
  musicbrainz."work_type" AS v010,
  musicbrainz."iswc" AS v011,
  musicbrainz."work_language" AS v012,
  musicbrainz."language" AS v013,
  musicbrainz."place" AS v014,
  musicbrainz."place_type" AS v015,
  musicbrainz."area" AS v016,
  musicbrainz."l_place_recording" AS v017,
  musicbrainz."link" AS v018,
  musicbrainz."link_type" AS v019
WHERE
  v002."recording" = v001."id"
  AND v001."artist_credit" = v003."id"
  AND v004."artist_credit" = v003."id"
  AND v004."artist" = v005."id"
  AND v006."entity0" = v001."id"
  AND v006."link" = v007."id"
  AND v007."link_type" = v008."id"
  AND v006."entity1" = v009."id"
  AND v009."type" = v010."id"
  AND v011."work" = v009."id"
  AND v012."work" = v009."id"
  AND v012."language" = v013."id"
  AND v014."type" = v015."id"
  AND v014."area" = v016."id"
  AND v017."entity0" = v014."id"
  AND v017."entity1" = v001."id"
  AND v017."link" = v018."id"
  AND v018."link_type" = v019."id"
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v019."gid" IN ('ad462279-14b0-4180-9b58-571d0eef7c51'::uuid);
