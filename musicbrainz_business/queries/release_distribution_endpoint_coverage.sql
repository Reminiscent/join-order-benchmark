-- Query: release_distribution_endpoint_coverage
-- Question: Which complete distribution records have typed external endpoints for the release, registered label, and a catalog recording?
-- Inclusion rule: This is a fully populated release cohort with independent typed endpoints. Endpoint presence indicates discoverability, not availability in every territory.
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
  musicbrainz."recording" AS v019,
  musicbrainz."l_release_url" AS v020,
  musicbrainz."link" AS v021,
  musicbrainz."link_type" AS v022,
  musicbrainz."url" AS v023,
  musicbrainz."l_label_url" AS v024,
  musicbrainz."link" AS v025,
  musicbrainz."link_type" AS v026,
  musicbrainz."url" AS v027,
  musicbrainz."l_recording_url" AS v028,
  musicbrainz."link" AS v029,
  musicbrainz."link_type" AS v030,
  musicbrainz."url" AS v031
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
  AND v020."entity0" = v001."id"
  AND v020."link" = v021."id"
  AND v021."link_type" = v022."id"
  AND v020."entity1" = v023."id"
  AND v024."entity0" = v015."id"
  AND v024."link" = v025."id"
  AND v025."link_type" = v026."id"
  AND v024."entity1" = v027."id"
  AND v028."entity0" = v019."id"
  AND v028."link" = v029."id"
  AND v029."link_type" = v030."id"
  AND v028."entity1" = v031."id"
  AND v001."barcode" IS NOT NULL
  AND v010."date_year" IS NOT NULL;
