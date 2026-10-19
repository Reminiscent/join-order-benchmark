-- Query: recording_multiple_work_attribution_audit
-- Question: Which ISRC recordings have two distinct performance-linked, ISWC-identified works?
-- Inclusion rule: The query identifies multi-work attribution for review, such as medleys or composite recordings; it does not label multiplicity as erroneous.
SELECT count(DISTINCT (v001."id", v009."id", v017."id"))
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
  musicbrainz."l_recording_work" AS v014,
  musicbrainz."link" AS v015,
  musicbrainz."link_type" AS v016,
  musicbrainz."work" AS v017,
  musicbrainz."iswc" AS v018
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
  AND v014."entity0" = v001."id"
  AND v014."link" = v015."id"
  AND v015."link_type" = v016."id"
  AND v014."entity1" = v017."id"
  AND v018."work" = v017."id"
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v016."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v009."id" < v017."id";
