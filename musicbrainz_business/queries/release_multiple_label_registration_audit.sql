-- Query: release_multiple_label_registration_audit
-- Question: Which distribution-ready releases are registered to at least two distinct labels?
-- Inclusion rule: The pair represents two release_label catalog registrations. It does not infer joint ownership or the legal role of either label.
SELECT count(DISTINCT (v001."id", v015."id", v021."id"))
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
  musicbrainz."recording" AS v019,
  musicbrainz."release_label" AS v020,
  musicbrainz."label" AS v021
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
  AND v020."release" = v001."id"
  AND v020."label" = v021."id"
  AND v001."barcode" IS NOT NULL
  AND v010."date_year" IS NOT NULL
  AND v015."id" < v021."id";
