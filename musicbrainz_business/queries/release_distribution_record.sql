-- Query: release_distribution_record
-- Question: Which barcode-bearing physical releases have a dated country publication, label, language and script, packaging and status, and at least one formatted medium with a recording track?
-- Inclusion rule: Every joined fact is a field of a distribution-ready release record. Unknown-country rows and undated publication facts are a different workflow and are not ANDed with known-country publication.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."release" AS v001,
  musicbrainz."release_group" AS v002,
  musicbrainz."artist_credit" AS v003,
  musicbrainz."artist_credit_name" AS v004,
  musicbrainz."artist" AS v005,
  musicbrainz."release_status" AS v006,
  musicbrainz."release_packaging" AS v007,
  musicbrainz."language" AS v008,
  musicbrainz."script" AS v009,
  musicbrainz."release_country" AS v010,
  musicbrainz."country_area" AS v011,
  musicbrainz."area" AS v012,
  musicbrainz."iso_3166_1" AS v013,
  musicbrainz."release_label" AS v014,
  musicbrainz."label" AS v015,
  musicbrainz."medium" AS v016,
  musicbrainz."medium_format" AS v017,
  musicbrainz."track" AS v018,
  musicbrainz."recording" AS v019
WHERE
  v001."release_group" = v002."id"
  AND v001."artist_credit" = v003."id"
  AND v004."artist_credit" = v003."id"
  AND v004."artist" = v005."id"
  AND v001."status" = v006."id"
  AND v001."packaging" = v007."id"
  AND v001."language" = v008."id"
  AND v001."script" = v009."id"
  AND v010."release" = v001."id"
  AND v010."country" = v011."area"
  AND v011."area" = v012."id"
  AND v013."area" = v012."id"
  AND v014."release" = v001."id"
  AND v014."label" = v015."id"
  AND v016."release" = v001."id"
  AND v016."format" = v017."id"
  AND v018."medium" = v016."id"
  AND v018."recording" = v019."id"
  AND v001."barcode" IS NOT NULL
  AND v010."date_year" IS NOT NULL;
