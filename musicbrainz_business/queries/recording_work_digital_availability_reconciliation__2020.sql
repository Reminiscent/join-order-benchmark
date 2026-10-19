-- Query: recording_work_digital_availability_reconciliation__2020
-- Question: Which reconciled 2020 recording/work identifier paths also have typed external endpoints for the recording and its release?
-- Inclusion rule: Credits are presentation and attribution roles, label is release-label metadata, and country is publication context. The result is an existential qualifying-recording cohort, not settlement ownership. Endpoint relationships are typed catalog references and do not prove universal availability.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."recording" AS v001,
  musicbrainz."isrc" AS v002,
  musicbrainz."artist_credit" AS v003,
  musicbrainz."artist_credit_name" AS v004,
  musicbrainz."artist" AS v005,
  musicbrainz."artist_type" AS v006,
  musicbrainz."l_recording_work" AS v007,
  musicbrainz."link" AS v008,
  musicbrainz."link_type" AS v009,
  musicbrainz."work" AS v010,
  musicbrainz."work_type" AS v011,
  musicbrainz."iswc" AS v012,
  musicbrainz."work_language" AS v013,
  musicbrainz."language" AS v014,
  musicbrainz."track" AS v015,
  musicbrainz."artist_credit" AS v016,
  musicbrainz."artist_credit_name" AS v017,
  musicbrainz."artist" AS v018,
  musicbrainz."medium" AS v019,
  musicbrainz."medium_format" AS v020,
  musicbrainz."release" AS v021,
  musicbrainz."release_group" AS v022,
  musicbrainz."release_group_primary_type" AS v023,
  musicbrainz."release_status" AS v024,
  musicbrainz."artist_credit" AS v025,
  musicbrainz."artist_credit_name" AS v026,
  musicbrainz."artist" AS v027,
  musicbrainz."release_country" AS v028,
  musicbrainz."country_area" AS v029,
  musicbrainz."area" AS v030,
  musicbrainz."iso_3166_1" AS v031,
  musicbrainz."release_label" AS v032,
  musicbrainz."label" AS v033,
  musicbrainz."label_type" AS v034,
  musicbrainz."l_recording_url" AS v035,
  musicbrainz."link" AS v036,
  musicbrainz."link_type" AS v037,
  musicbrainz."url" AS v038,
  musicbrainz."l_release_url" AS v039,
  musicbrainz."link" AS v040,
  musicbrainz."link_type" AS v041,
  musicbrainz."url" AS v042
WHERE
  v002."recording" = v001."id"
  AND v001."artist_credit" = v003."id"
  AND v004."artist_credit" = v003."id"
  AND v004."artist" = v005."id"
  AND v005."type" = v006."id"
  AND v007."entity0" = v001."id"
  AND v007."link" = v008."id"
  AND v008."link_type" = v009."id"
  AND v007."entity1" = v010."id"
  AND v010."type" = v011."id"
  AND v012."work" = v010."id"
  AND v013."work" = v010."id"
  AND v013."language" = v014."id"
  AND v015."recording" = v001."id"
  AND v015."artist_credit" = v016."id"
  AND v017."artist_credit" = v016."id"
  AND v017."artist" = v018."id"
  AND v015."medium" = v019."id"
  AND v019."format" = v020."id"
  AND v019."release" = v021."id"
  AND v021."release_group" = v022."id"
  AND v022."type" = v023."id"
  AND v021."status" = v024."id"
  AND v021."artist_credit" = v025."id"
  AND v026."artist_credit" = v025."id"
  AND v026."artist" = v027."id"
  AND v028."release" = v021."id"
  AND v028."country" = v029."area"
  AND v029."area" = v030."id"
  AND v031."area" = v030."id"
  AND v032."release" = v021."id"
  AND v032."label" = v033."id"
  AND v033."type" = v034."id"
  AND v035."entity0" = v001."id"
  AND v035."link" = v036."id"
  AND v036."link_type" = v037."id"
  AND v035."entity1" = v038."id"
  AND v039."entity0" = v021."id"
  AND v039."link" = v040."id"
  AND v040."link_type" = v041."id"
  AND v039."entity1" = v042."id"
  AND v028."date_year" = 2020
  AND v009."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid);
