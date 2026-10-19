-- Query: artist_catalog_authority_lineage_record
-- Question: Which typed, area-associated artists have both IPI and ISNI authority identifiers and occur in a label-registered release path containing an ISRC recording?
-- Inclusion rule: The release credit, authority identifiers, and recording identifier are independent catalog facts. Release-label registration is publication metadata, not artist affiliation or ownership.
SELECT count(DISTINCT v007."id")
FROM
  musicbrainz."label" AS v001,
  musicbrainz."release_label" AS v002,
  musicbrainz."release" AS v003,
  musicbrainz."release_group" AS v004,
  musicbrainz."artist_credit" AS v005,
  musicbrainz."artist_credit_name" AS v006,
  musicbrainz."artist" AS v007,
  musicbrainz."medium" AS v008,
  musicbrainz."medium_format" AS v009,
  musicbrainz."track" AS v010,
  musicbrainz."recording" AS v011,
  musicbrainz."artist_type" AS v012,
  musicbrainz."area" AS v013,
  musicbrainz."artist_ipi" AS v014,
  musicbrainz."artist_isni" AS v015,
  musicbrainz."isrc" AS v016
WHERE
  v002."label" = v001."id"
  AND v002."release" = v003."id"
  AND v003."release_group" = v004."id"
  AND v003."artist_credit" = v005."id"
  AND v006."artist_credit" = v005."id"
  AND v006."artist" = v007."id"
  AND v008."release" = v003."id"
  AND v008."format" = v009."id"
  AND v010."medium" = v008."id"
  AND v010."recording" = v011."id"
  AND v007."type" = v012."id"
  AND v007."area" = v013."id"
  AND v014."artist" = v007."id"
  AND v015."artist" = v007."id"
  AND v016."recording" = v011."id";
