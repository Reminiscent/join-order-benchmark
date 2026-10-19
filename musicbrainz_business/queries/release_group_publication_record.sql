-- Query: release_group_publication_record
-- Question: Which typed release groups have a country-coded release with credited artists, a formatted medium, and an ISRC-identified recording track?
-- Inclusion rule: Every relation is part of the release-group classification, concrete publication, release credit, country, medium, track, or recording identifier output. Secondary types and unrelated enrichment are optional and excluded.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."release_group" AS v001,
  musicbrainz."release_group_primary_type" AS v002,
  musicbrainz."release" AS v003,
  musicbrainz."release_country" AS v004,
  musicbrainz."country_area" AS v005,
  musicbrainz."area" AS v006,
  musicbrainz."iso_3166_1" AS v007,
  musicbrainz."artist_credit" AS v008,
  musicbrainz."artist_credit_name" AS v009,
  musicbrainz."artist" AS v010,
  musicbrainz."medium" AS v011,
  musicbrainz."medium_format" AS v012,
  musicbrainz."track" AS v013,
  musicbrainz."recording" AS v014,
  musicbrainz."isrc" AS v015
WHERE
  v001."type" = v002."id"
  AND v003."release_group" = v001."id"
  AND v004."release" = v003."id"
  AND v004."country" = v005."area"
  AND v005."area" = v006."id"
  AND v007."area" = v006."id"
  AND v003."artist_credit" = v008."id"
  AND v009."artist_credit" = v008."id"
  AND v009."artist" = v010."id"
  AND v011."release" = v003."id"
  AND v011."format" = v012."id"
  AND v013."medium" = v011."id"
  AND v013."recording" = v014."id"
  AND v015."recording" = v014."id";
