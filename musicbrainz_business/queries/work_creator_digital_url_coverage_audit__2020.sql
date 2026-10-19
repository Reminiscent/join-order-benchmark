-- Query: work_creator_digital_url_coverage_audit__2020
-- Question: Which Japanese-language songs embodied by reconciled 2020 recording/release paths have an explicitly classified composer, writer, or lyricist attribution and a typed work URL relationship?
-- Inclusion rule: Credits are presentation and attribution roles, label is release-label metadata, and country is publication context. The result is an existential qualifying-recording cohort, not settlement ownership. Endpoint relationships are typed catalog references and do not prove universal availability. Creator evidence is limited to the published composer, writer, and lyricist relationship types, and the credited creator must have an artist classification. The operational cohort is limited to works classified as Song with Japanese as an explicitly recorded work language. A typed work URL relationship is discoverability evidence and is not necessarily an identity-provider endpoint or proof of authorship, ownership, or official status.
SELECT count(DISTINCT v010."id")
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
  musicbrainz."url" AS v042,
  musicbrainz."l_artist_work" AS v043,
  musicbrainz."link" AS v044,
  musicbrainz."link_type" AS v045,
  musicbrainz."artist" AS v046,
  musicbrainz."artist_type" AS v047,
  musicbrainz."l_url_work" AS v048,
  musicbrainz."link" AS v049,
  musicbrainz."link_type" AS v050,
  musicbrainz."url" AS v051
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
  AND v043."entity1" = v010."id"
  AND v043."link" = v044."id"
  AND v044."link_type" = v045."id"
  AND v043."entity0" = v046."id"
  AND v046."type" = v047."id"
  AND v048."entity1" = v010."id"
  AND v048."link" = v049."id"
  AND v049."link_type" = v050."id"
  AND v048."entity0" = v051."id"
  AND v028."date_year" = 2020
  AND v009."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v011."name" = 'Song'
  AND v014."iso_code_3" = 'jpn'
  AND v045."gid" IN ('d59d99ea-23d4-4a80-b066-edca32ee158f'::uuid, 'a255bca1-b157-4518-9108-7b147dc3fc68'::uuid, '3e48faba-ec01-47fd-8e89-30e81161661c'::uuid);
