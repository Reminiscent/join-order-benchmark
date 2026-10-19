-- Query: work_creator_recording_lineage_record
-- Question: Which ISWC works with performance-linked credited recording evidence have an explicit composer, writer, or lyricist attribution?
-- Inclusion rule: The creator role comes from an explicit typed MusicBrainz relationship. It is attribution metadata, not a statement of legal ownership or contribution share.
SELECT count(DISTINCT (v001."id", v016."id"))
FROM
  musicbrainz."work" AS v001,
  musicbrainz."work_type" AS v002,
  musicbrainz."work_language" AS v003,
  musicbrainz."language" AS v004,
  musicbrainz."iswc" AS v005,
  musicbrainz."l_recording_work" AS v006,
  musicbrainz."link" AS v007,
  musicbrainz."link_type" AS v008,
  musicbrainz."recording" AS v009,
  musicbrainz."artist_credit" AS v010,
  musicbrainz."artist_credit_name" AS v011,
  musicbrainz."artist" AS v012,
  musicbrainz."l_artist_work" AS v013,
  musicbrainz."link" AS v014,
  musicbrainz."link_type" AS v015,
  musicbrainz."artist" AS v016
WHERE
  v001."type" = v002."id"
  AND v003."work" = v001."id"
  AND v003."language" = v004."id"
  AND v005."work" = v001."id"
  AND v006."entity1" = v001."id"
  AND v006."entity0" = v009."id"
  AND v006."link" = v007."id"
  AND v007."link_type" = v008."id"
  AND v009."artist_credit" = v010."id"
  AND v011."artist_credit" = v010."id"
  AND v011."artist" = v012."id"
  AND v013."entity1" = v001."id"
  AND v013."link" = v014."id"
  AND v014."link_type" = v015."id"
  AND v013."entity0" = v016."id"
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v015."gid" IN ('d59d99ea-23d4-4a80-b066-edca32ee158f'::uuid, 'a255bca1-b157-4518-9108-7b147dc3fc68'::uuid, '3e48faba-ec01-47fd-8e89-30e81161661c'::uuid);
