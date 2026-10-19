-- Query: series_release_membership_record
-- Question: Which typed and ordered series contain a release whose group, credit, medium format, track, and recording lineage can all be exported?
-- Inclusion rule: This query selects the release-member channel only. Artist-, recording-, and release-member channels are alternative series workflows, not simultaneous requirements.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."series" AS v001,
  musicbrainz."series_type" AS v002,
  musicbrainz."series_ordering_type" AS v003,
  musicbrainz."l_release_series" AS v004,
  musicbrainz."link" AS v005,
  musicbrainz."link_type" AS v006,
  musicbrainz."release" AS v007,
  musicbrainz."release_group" AS v008,
  musicbrainz."artist_credit" AS v009,
  musicbrainz."artist_credit_name" AS v010,
  musicbrainz."artist" AS v011,
  musicbrainz."medium" AS v012,
  musicbrainz."medium_format" AS v013,
  musicbrainz."track" AS v014,
  musicbrainz."recording" AS v015
WHERE
  v001."type" = v002."id"
  AND v001."ordering_type" = v003."id"
  AND v004."entity1" = v001."id"
  AND v004."link" = v005."id"
  AND v005."link_type" = v006."id"
  AND v004."entity0" = v007."id"
  AND v007."release_group" = v008."id"
  AND v007."artist_credit" = v009."id"
  AND v010."artist_credit" = v009."id"
  AND v010."artist" = v011."id"
  AND v012."release" = v007."id"
  AND v012."format" = v013."id"
  AND v014."medium" = v012."id"
  AND v014."recording" = v015."id";
