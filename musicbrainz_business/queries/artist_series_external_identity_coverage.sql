-- Query: artist_series_external_identity_coverage
-- Question: Which part-of artist-series memberships have typed external endpoints for both the artist and the series?
-- Inclusion rule: External relationship types are retained as evidence and may represent official, social, database, or media endpoints; they are not treated as identity proof by themselves.
SELECT count(DISTINCT (v001."id", v007."id"))
FROM
  musicbrainz."series" AS v001,
  musicbrainz."series_type" AS v002,
  musicbrainz."series_ordering_type" AS v003,
  musicbrainz."l_artist_series" AS v004,
  musicbrainz."link" AS v005,
  musicbrainz."link_type" AS v006,
  musicbrainz."artist" AS v007,
  musicbrainz."artist_type" AS v008,
  musicbrainz."artist_credit_name" AS v009,
  musicbrainz."artist_credit" AS v010,
  musicbrainz."artist_alias" AS v011,
  musicbrainz."artist_isni" AS v012,
  musicbrainz."l_artist_url" AS v013,
  musicbrainz."link" AS v014,
  musicbrainz."link_type" AS v015,
  musicbrainz."url" AS v016,
  musicbrainz."l_series_url" AS v017,
  musicbrainz."link" AS v018,
  musicbrainz."link_type" AS v019,
  musicbrainz."url" AS v020
WHERE
  v001."type" = v002."id"
  AND v001."ordering_type" = v003."id"
  AND v004."entity1" = v001."id"
  AND v004."link" = v005."id"
  AND v005."link_type" = v006."id"
  AND v004."entity0" = v007."id"
  AND v007."type" = v008."id"
  AND v009."artist" = v007."id"
  AND v009."artist_credit" = v010."id"
  AND v011."artist" = v007."id"
  AND v012."artist" = v007."id"
  AND v013."entity0" = v007."id"
  AND v013."link" = v014."id"
  AND v014."link_type" = v015."id"
  AND v013."entity1" = v016."id"
  AND v017."entity0" = v001."id"
  AND v017."link" = v018."id"
  AND v018."link_type" = v019."id"
  AND v017."entity1" = v020."id"
  AND v006."gid" IN ('d1a845d1-8c03-3191-9454-e4e8d37fa5e0'::uuid);
