-- Query: release_track_multi_endpoint_lineage__2020
-- Question: Which eligible 2020 release-track occurrences also have typed external endpoints for both release and recording?
-- Inclusion rule: This is an existential metadata-eligibility cohort, not a rights-ownership record. Each credit level is an independent presentation or attribution role. Release and recording endpoints are independent typed discoverability metadata.
SELECT count(DISTINCT (v017."id", v011."id"))
FROM
  musicbrainz."recording" AS v001,
  musicbrainz."isrc" AS v002,
  musicbrainz."artist_credit" AS v003,
  musicbrainz."artist_credit_name" AS v004,
  musicbrainz."artist" AS v005,
  musicbrainz."l_recording_work" AS v006,
  musicbrainz."link" AS v007,
  musicbrainz."link_type" AS v008,
  musicbrainz."work" AS v009,
  musicbrainz."iswc" AS v010,
  musicbrainz."track" AS v011,
  musicbrainz."artist_credit" AS v012,
  musicbrainz."artist_credit_name" AS v013,
  musicbrainz."artist" AS v014,
  musicbrainz."medium" AS v015,
  musicbrainz."medium_format" AS v016,
  musicbrainz."release" AS v017,
  musicbrainz."release_group" AS v018,
  musicbrainz."release_status" AS v019,
  musicbrainz."artist_credit" AS v020,
  musicbrainz."artist_credit_name" AS v021,
  musicbrainz."artist" AS v022,
  musicbrainz."release_country" AS v023,
  musicbrainz."country_area" AS v024,
  musicbrainz."area" AS v025,
  musicbrainz."iso_3166_1" AS v026,
  musicbrainz."release_label" AS v027,
  musicbrainz."label" AS v028,
  musicbrainz."l_release_url" AS v029,
  musicbrainz."link" AS v030,
  musicbrainz."link_type" AS v031,
  musicbrainz."url" AS v032,
  musicbrainz."l_recording_url" AS v033,
  musicbrainz."link" AS v034,
  musicbrainz."link_type" AS v035,
  musicbrainz."url" AS v036
WHERE
  v002."recording" = v001."id"
  AND v001."artist_credit" = v003."id"
  AND v004."artist_credit" = v003."id"
  AND v004."artist" = v005."id"
  AND v006."entity0" = v001."id"
  AND v006."link" = v007."id"
  AND v007."link_type" = v008."id"
  AND v006."entity1" = v009."id"
  AND v010."work" = v009."id"
  AND v011."recording" = v001."id"
  AND v011."artist_credit" = v012."id"
  AND v013."artist_credit" = v012."id"
  AND v013."artist" = v014."id"
  AND v011."medium" = v015."id"
  AND v015."format" = v016."id"
  AND v015."release" = v017."id"
  AND v017."release_group" = v018."id"
  AND v017."status" = v019."id"
  AND v017."artist_credit" = v020."id"
  AND v021."artist_credit" = v020."id"
  AND v021."artist" = v022."id"
  AND v023."release" = v017."id"
  AND v023."country" = v024."area"
  AND v024."area" = v025."id"
  AND v026."area" = v025."id"
  AND v027."release" = v017."id"
  AND v027."label" = v028."id"
  AND v029."entity0" = v017."id"
  AND v029."link" = v030."id"
  AND v030."link_type" = v031."id"
  AND v029."entity1" = v032."id"
  AND v033."entity0" = v001."id"
  AND v033."link" = v034."id"
  AND v034."link_type" = v035."id"
  AND v033."entity1" = v036."id"
  AND v023."date_year" = 2020
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid);
