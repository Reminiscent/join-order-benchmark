-- Query: series_artist_membership_record
-- Question: Which typed and ordered series contain a typed, aliased and ISNI-identified artist that is used in a catalog credit?
-- Inclusion rule: This query selects the artist-member channel only and exports authority-backed member identity. Recording and release membership are alternative queries. Only the part-of relationship channel is included.
SELECT count(DISTINCT v001."id")
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
  musicbrainz."artist_isni" AS v012
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
  AND v006."gid" IN ('d1a845d1-8c03-3191-9454-e4e8d37fa5e0'::uuid);
