-- Query: series_release_external_identifier_coverage
-- Question: Which series member releases have typed external endpoints for the series, release, and a catalog recording?
-- Inclusion rule: The series membership remains the core business fact; external endpoints are typed discoverability metadata and need not use the same provider.
SELECT count(DISTINCT (v001."id", v007."id"))
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
  musicbrainz."recording" AS v015,
  musicbrainz."l_series_url" AS v016,
  musicbrainz."link" AS v017,
  musicbrainz."link_type" AS v018,
  musicbrainz."url" AS v019,
  musicbrainz."l_release_url" AS v020,
  musicbrainz."link" AS v021,
  musicbrainz."link_type" AS v022,
  musicbrainz."url" AS v023,
  musicbrainz."l_recording_url" AS v024,
  musicbrainz."link" AS v025,
  musicbrainz."link_type" AS v026,
  musicbrainz."url" AS v027
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
  AND v014."recording" = v015."id"
  AND v016."entity0" = v001."id"
  AND v016."link" = v017."id"
  AND v017."link_type" = v018."id"
  AND v016."entity1" = v019."id"
  AND v020."entity0" = v007."id"
  AND v020."link" = v021."id"
  AND v021."link_type" = v022."id"
  AND v020."entity1" = v023."id"
  AND v024."entity0" = v015."id"
  AND v024."link" = v025."id"
  AND v025."link_type" = v026."id"
  AND v024."entity1" = v027."id";
