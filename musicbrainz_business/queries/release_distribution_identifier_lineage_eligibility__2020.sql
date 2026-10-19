-- Query: release_distribution_identifier_lineage_eligibility__2020
-- Question: Which 2020 barcode releases have a fully populated distribution path, label registration area, release and recording credits, ISRC, and performance-linked ISWC work metadata?
-- Inclusion rule: This is a strict complete-field eligibility cohort. Nullable packaging, language, script, label area, classification and identifier fields are intentionally mandatory; it is not a general release export or legal-rights record.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."release" AS v001,
  musicbrainz."release_group" AS v002,
  musicbrainz."release_group_primary_type" AS v003,
  musicbrainz."artist_credit" AS v004,
  musicbrainz."artist_credit_name" AS v005,
  musicbrainz."artist" AS v006,
  musicbrainz."artist_type" AS v007,
  musicbrainz."release_status" AS v008,
  musicbrainz."release_packaging" AS v009,
  musicbrainz."language" AS v010,
  musicbrainz."script" AS v011,
  musicbrainz."release_country" AS v012,
  musicbrainz."country_area" AS v013,
  musicbrainz."area" AS v014,
  musicbrainz."iso_3166_1" AS v015,
  musicbrainz."release_label" AS v016,
  musicbrainz."label" AS v017,
  musicbrainz."label_type" AS v018,
  musicbrainz."area" AS v019,
  musicbrainz."medium" AS v020,
  musicbrainz."medium_format" AS v021,
  musicbrainz."track" AS v022,
  musicbrainz."recording" AS v023,
  musicbrainz."isrc" AS v024,
  musicbrainz."artist_credit" AS v025,
  musicbrainz."artist_credit_name" AS v026,
  musicbrainz."artist" AS v027,
  musicbrainz."l_recording_work" AS v028,
  musicbrainz."link" AS v029,
  musicbrainz."link_type" AS v030,
  musicbrainz."work" AS v031,
  musicbrainz."work_type" AS v032,
  musicbrainz."iswc" AS v033,
  musicbrainz."work_language" AS v034,
  musicbrainz."language" AS v035
WHERE
  v001."release_group" = v002."id"
  AND v002."type" = v003."id"
  AND v001."artist_credit" = v004."id"
  AND v005."artist_credit" = v004."id"
  AND v005."artist" = v006."id"
  AND v006."type" = v007."id"
  AND v001."status" = v008."id"
  AND v001."packaging" = v009."id"
  AND v001."language" = v010."id"
  AND v001."script" = v011."id"
  AND v012."release" = v001."id"
  AND v012."country" = v013."area"
  AND v013."area" = v014."id"
  AND v015."area" = v014."id"
  AND v016."release" = v001."id"
  AND v016."label" = v017."id"
  AND v017."type" = v018."id"
  AND v017."area" = v019."id"
  AND v020."release" = v001."id"
  AND v020."format" = v021."id"
  AND v022."medium" = v020."id"
  AND v022."recording" = v023."id"
  AND v024."recording" = v023."id"
  AND v023."artist_credit" = v025."id"
  AND v026."artist_credit" = v025."id"
  AND v026."artist" = v027."id"
  AND v028."entity0" = v023."id"
  AND v028."link" = v029."id"
  AND v029."link_type" = v030."id"
  AND v028."entity1" = v031."id"
  AND v031."type" = v032."id"
  AND v033."work" = v031."id"
  AND v034."work" = v031."id"
  AND v034."language" = v035."id"
  AND v001."barcode" IS NOT NULL
  AND v012."date_year" = 2020
  AND v030."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid);
