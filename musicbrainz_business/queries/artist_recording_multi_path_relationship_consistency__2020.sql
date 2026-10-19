-- Query: artist_recording_multi_path_relationship_consistency__2020
-- Question: Which artist-recording pairs in the 2020 release catalog are present in recording credit, track credit, and any explicit typed artist-recording relationship?
-- Inclusion rule: The three catalog paths must converge on the same artist and recording. The explicit relationship type is evidence output and is not assumed to mean performance or ownership.
SELECT count(DISTINCT (v005."id", v001."id"))
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
  musicbrainz."medium" AS v014,
  musicbrainz."medium_format" AS v015,
  musicbrainz."release" AS v016,
  musicbrainz."release_group" AS v017,
  musicbrainz."release_status" AS v018,
  musicbrainz."release_country" AS v019,
  musicbrainz."country_area" AS v020,
  musicbrainz."area" AS v021,
  musicbrainz."iso_3166_1" AS v022,
  musicbrainz."release_label" AS v023,
  musicbrainz."label" AS v024,
  musicbrainz."l_artist_recording" AS v025,
  musicbrainz."link" AS v026,
  musicbrainz."link_type" AS v027
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
  AND v011."medium" = v014."id"
  AND v014."format" = v015."id"
  AND v014."release" = v016."id"
  AND v016."release_group" = v017."id"
  AND v016."status" = v018."id"
  AND v019."release" = v016."id"
  AND v019."country" = v020."area"
  AND v020."area" = v021."id"
  AND v022."area" = v021."id"
  AND v023."release" = v016."id"
  AND v023."label" = v024."id"
  AND v013."artist" = v005."id"
  AND v025."entity0" = v005."id"
  AND v025."entity1" = v001."id"
  AND v025."link" = v026."id"
  AND v026."link_type" = v027."id"
  AND v019."date_year" = 2020
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid);
