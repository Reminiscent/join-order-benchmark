-- Query: series_recording_membership_record
-- Question: Which typed and ordered series contain an ISRC-identified recording with credited artists and an ISWC-identified underlying work?
-- Inclusion rule: This query selects the recording-member channel only and follows the member's rights lineage. Artist and release membership are alternative queries. Only the part-of relationship channel is included.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."series" AS v001,
  musicbrainz."series_type" AS v002,
  musicbrainz."series_ordering_type" AS v003,
  musicbrainz."l_recording_series" AS v004,
  musicbrainz."link" AS v005,
  musicbrainz."link_type" AS v006,
  musicbrainz."recording" AS v007,
  musicbrainz."isrc" AS v008,
  musicbrainz."artist_credit" AS v009,
  musicbrainz."artist_credit_name" AS v010,
  musicbrainz."artist" AS v011,
  musicbrainz."l_recording_work" AS v012,
  musicbrainz."work" AS v013,
  musicbrainz."iswc" AS v014,
  musicbrainz."link" AS v015,
  musicbrainz."link_type" AS v016
WHERE
  v001."type" = v002."id"
  AND v001."ordering_type" = v003."id"
  AND v004."entity1" = v001."id"
  AND v004."link" = v005."id"
  AND v005."link_type" = v006."id"
  AND v004."entity0" = v007."id"
  AND v008."recording" = v007."id"
  AND v007."artist_credit" = v009."id"
  AND v010."artist_credit" = v009."id"
  AND v010."artist" = v011."id"
  AND v012."entity0" = v007."id"
  AND v012."entity1" = v013."id"
  AND v014."work" = v013."id"
  AND v012."link" = v015."id"
  AND v015."link_type" = v016."id"
  AND v006."gid" IN ('ea6f0698-6782-30d6-b16d-293081b66774'::uuid)
  AND v016."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid);
