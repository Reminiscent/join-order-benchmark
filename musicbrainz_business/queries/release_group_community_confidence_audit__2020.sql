-- Query: release_group_community_confidence_audit__2020
-- Question: Which complete-path 2020 release groups have aggregate community-rating records for the group, at least one credited artist, recording, and work?
-- Inclusion rule: This is existential complete-path coverage, not proof that every release or track is complete. Community tags must have a positive aggregate vote count. Endpoint coverage is existential for the qualifying path and does not require one provider across entity levels. Every entity-level rating_count must be positive. The query does not assert that ratings came from independent users or are statistically independent. Ratings are community review signals, not ownership or factual-certainty claims.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."release_group" AS v001,
  musicbrainz."release_group_primary_type" AS v002,
  musicbrainz."release_group_secondary_type_join" AS v003,
  musicbrainz."release_group_secondary_type" AS v004,
  musicbrainz."artist_credit" AS v005,
  musicbrainz."artist_credit_name" AS v006,
  musicbrainz."artist" AS v007,
  musicbrainz."artist_type" AS v008,
  musicbrainz."release_group_tag" AS v009,
  musicbrainz."tag" AS v010,
  musicbrainz."release" AS v011,
  musicbrainz."release_status" AS v012,
  musicbrainz."release_packaging" AS v013,
  musicbrainz."language" AS v014,
  musicbrainz."script" AS v015,
  musicbrainz."release_country" AS v016,
  musicbrainz."country_area" AS v017,
  musicbrainz."area" AS v018,
  musicbrainz."iso_3166_1" AS v019,
  musicbrainz."release_label" AS v020,
  musicbrainz."label" AS v021,
  musicbrainz."label_type" AS v022,
  musicbrainz."medium" AS v023,
  musicbrainz."medium_format" AS v024,
  musicbrainz."track" AS v025,
  musicbrainz."recording" AS v026,
  musicbrainz."isrc" AS v027,
  musicbrainz."l_recording_work" AS v028,
  musicbrainz."link" AS v029,
  musicbrainz."link_type" AS v030,
  musicbrainz."work" AS v031,
  musicbrainz."work_type" AS v032,
  musicbrainz."iswc" AS v033,
  musicbrainz."work_language" AS v034,
  musicbrainz."language" AS v035,
  musicbrainz."l_release_group_url" AS v036,
  musicbrainz."link" AS v037,
  musicbrainz."link_type" AS v038,
  musicbrainz."url" AS v039,
  musicbrainz."l_release_url" AS v040,
  musicbrainz."link" AS v041,
  musicbrainz."link_type" AS v042,
  musicbrainz."url" AS v043,
  musicbrainz."l_recording_url" AS v044,
  musicbrainz."link" AS v045,
  musicbrainz."link_type" AS v046,
  musicbrainz."url" AS v047,
  musicbrainz."release_group_meta" AS v048,
  musicbrainz."artist_meta" AS v049,
  musicbrainz."recording_meta" AS v050,
  musicbrainz."work_meta" AS v051
WHERE
  v001."type" = v002."id"
  AND v003."release_group" = v001."id"
  AND v003."secondary_type" = v004."id"
  AND v001."artist_credit" = v005."id"
  AND v006."artist_credit" = v005."id"
  AND v006."artist" = v007."id"
  AND v007."type" = v008."id"
  AND v009."release_group" = v001."id"
  AND v009."tag" = v010."id"
  AND v011."release_group" = v001."id"
  AND v011."status" = v012."id"
  AND v011."packaging" = v013."id"
  AND v011."language" = v014."id"
  AND v011."script" = v015."id"
  AND v016."release" = v011."id"
  AND v016."country" = v017."area"
  AND v017."area" = v018."id"
  AND v019."area" = v018."id"
  AND v020."release" = v011."id"
  AND v020."label" = v021."id"
  AND v021."type" = v022."id"
  AND v023."release" = v011."id"
  AND v023."format" = v024."id"
  AND v025."medium" = v023."id"
  AND v025."recording" = v026."id"
  AND v027."recording" = v026."id"
  AND v028."entity0" = v026."id"
  AND v028."link" = v029."id"
  AND v029."link_type" = v030."id"
  AND v028."entity1" = v031."id"
  AND v031."type" = v032."id"
  AND v033."work" = v031."id"
  AND v034."work" = v031."id"
  AND v034."language" = v035."id"
  AND v036."entity0" = v001."id"
  AND v036."link" = v037."id"
  AND v037."link_type" = v038."id"
  AND v036."entity1" = v039."id"
  AND v040."entity0" = v011."id"
  AND v040."link" = v041."id"
  AND v041."link_type" = v042."id"
  AND v040."entity1" = v043."id"
  AND v044."entity0" = v026."id"
  AND v044."link" = v045."id"
  AND v045."link_type" = v046."id"
  AND v044."entity1" = v047."id"
  AND v048."id" = v001."id"
  AND v049."id" = v007."id"
  AND v050."id" = v026."id"
  AND v051."id" = v031."id"
  AND v011."barcode" IS NOT NULL
  AND v016."date_year" = 2020
  AND v009."count" > 0
  AND v030."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v048."rating_count" > 0
  AND v049."rating_count" > 0
  AND v050."rating_count" > 0
  AND v051."rating_count" > 0;
