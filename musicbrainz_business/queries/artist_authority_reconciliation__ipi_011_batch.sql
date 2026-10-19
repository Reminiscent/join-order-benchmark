-- Query: artist_authority_reconciliation__ipi_011_batch
-- Question: Which credited artists in the IPI 011 allocation batch can be reconciled across typed identity, IPI, ISNI, aliases, annotations, and community tags?
-- Inclusion rule: Return an artist only when every authority and editorial fact named in the output is present. The IPI allocation range is the batch parameter, not a planted row identifier. The mandatory editorial fields make this a strict authority-review cohort, not general artist coverage.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."artist" AS v001,
  musicbrainz."artist_type" AS v002,
  musicbrainz."area" AS v003,
  musicbrainz."gender" AS v004,
  musicbrainz."artist_credit_name" AS v005,
  musicbrainz."artist_credit" AS v006,
  musicbrainz."artist_alias" AS v007,
  musicbrainz."artist_ipi" AS v008,
  musicbrainz."artist_isni" AS v009,
  musicbrainz."artist_annotation" AS v010,
  musicbrainz."annotation" AS v011,
  musicbrainz."artist_tag" AS v012,
  musicbrainz."tag" AS v013
WHERE
  v001."type" = v002."id"
  AND v001."area" = v003."id"
  AND v001."gender" = v004."id"
  AND v005."artist" = v001."id"
  AND v005."artist_credit" = v006."id"
  AND v007."artist" = v001."id"
  AND v008."artist" = v001."id"
  AND v009."artist" = v001."id"
  AND v010."artist" = v001."id"
  AND v010."annotation" = v011."id"
  AND v012."artist" = v001."id"
  AND v012."tag" = v013."id"
  AND v008."ipi" >= '01100000000' AND v008."ipi" < '01200000000';
