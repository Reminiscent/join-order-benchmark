-- Query: work_multilingual_recording_lineage_audit
-- Question: Which ISWC works declare at least two distinct languages and have performance-linked credited recording evidence?
-- Inclusion rule: Language multiplicity is retained as multilingual catalog metadata and is not treated as a conflict.
SELECT count(DISTINCT (v001."id", v004."id", v014."id"))
FROM
  musicbrainz."work" AS v001,
  musicbrainz."work_type" AS v002,
  musicbrainz."work_language" AS v003,
  musicbrainz."language" AS v004,
  musicbrainz."iswc" AS v005,
  musicbrainz."l_recording_work" AS v006,
  musicbrainz."link" AS v007,
  musicbrainz."link_type" AS v008,
  musicbrainz."recording" AS v009,
  musicbrainz."artist_credit" AS v010,
  musicbrainz."artist_credit_name" AS v011,
  musicbrainz."artist" AS v012,
  musicbrainz."work_language" AS v013,
  musicbrainz."language" AS v014
WHERE
  v001."type" = v002."id"
  AND v003."work" = v001."id"
  AND v003."language" = v004."id"
  AND v005."work" = v001."id"
  AND v006."entity1" = v001."id"
  AND v006."entity0" = v009."id"
  AND v006."link" = v007."id"
  AND v007."link_type" = v008."id"
  AND v009."artist_credit" = v010."id"
  AND v011."artist_credit" = v010."id"
  AND v011."artist" = v012."id"
  AND v013."work" = v001."id"
  AND v013."language" = v014."id"
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v004."id" < v014."id";
