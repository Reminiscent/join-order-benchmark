-- Query: recording_multiple_isrc_identifier_audit
-- Question: Which credited recordings carry more than one ISRC while retaining performance-linked ISWC work lineage?
-- Inclusion rule: Different ISRC rows are treated as an identifier pair requiring review; multiplicity can be legitimate and is not classified as a conflict by this query.
SELECT count(DISTINCT (v001."id", v002."id", v014."id"))
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
  musicbrainz."isrc" AS v014
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
  AND v014."recording" = v001."id"
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v002."id" < v014."id";
