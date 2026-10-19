-- Query: genre_instrument_invention_catalog_lineage_record
-- Question: Which genres are explicitly associated with an instrument whose inventor is credited on an ISRC recording?
-- Inclusion rule: The genre-instrument relationship means associated instrument and the artist-instrument relationship means invented. Recording credit is catalog evidence; the query does not claim instrument performance on that recording.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."genre" AS v001,
  musicbrainz."l_genre_instrument" AS v002,
  musicbrainz."link" AS v003,
  musicbrainz."link_type" AS v004,
  musicbrainz."instrument" AS v005,
  musicbrainz."instrument_type" AS v006,
  musicbrainz."l_artist_instrument" AS v007,
  musicbrainz."artist" AS v008,
  musicbrainz."artist_type" AS v009,
  musicbrainz."artist_credit_name" AS v010,
  musicbrainz."artist_credit" AS v011,
  musicbrainz."recording" AS v012,
  musicbrainz."isrc" AS v013,
  musicbrainz."link" AS v014,
  musicbrainz."link_type" AS v015
WHERE
  v002."entity0" = v001."id"
  AND v002."link" = v003."id"
  AND v003."link_type" = v004."id"
  AND v002."entity1" = v005."id"
  AND v005."type" = v006."id"
  AND v007."entity1" = v005."id"
  AND v007."entity0" = v008."id"
  AND v008."type" = v009."id"
  AND v010."artist" = v008."id"
  AND v010."artist_credit" = v011."id"
  AND v012."artist_credit" = v011."id"
  AND v013."recording" = v012."id"
  AND v007."link" = v014."id"
  AND v014."link_type" = v015."id"
  AND v004."gid" IN ('0b4d32c8-bdba-4842-a6b5-35b2ca2f4f11'::uuid)
  AND v015."gid" IN ('87bfa63d-c91f-4bf2-9051-5103f7d181dd'::uuid);
