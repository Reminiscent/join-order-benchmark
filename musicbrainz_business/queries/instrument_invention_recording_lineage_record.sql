-- Query: instrument_invention_recording_lineage_record
-- Question: Which typed instruments have an explicit inventor who is also credited on an ISRC recording of a performance-linked ISWC work?
-- Inclusion rule: MusicBrainz currently uses the artist-instrument channel for invented-by relationships, not performance credits. The same artist must occur in the recording credit, which is catalog evidence rather than proof that the inventor performed.
SELECT count(DISTINCT v001."id")
FROM
  musicbrainz."instrument" AS v001,
  musicbrainz."instrument_type" AS v002,
  musicbrainz."l_artist_instrument" AS v003,
  musicbrainz."link" AS v004,
  musicbrainz."link_type" AS v005,
  musicbrainz."artist" AS v006,
  musicbrainz."artist_type" AS v007,
  musicbrainz."artist_credit_name" AS v008,
  musicbrainz."artist_credit" AS v009,
  musicbrainz."recording" AS v010,
  musicbrainz."isrc" AS v011,
  musicbrainz."l_recording_work" AS v012,
  musicbrainz."work" AS v013,
  musicbrainz."iswc" AS v014,
  musicbrainz."link" AS v015,
  musicbrainz."link_type" AS v016
WHERE
  v001."type" = v002."id"
  AND v003."entity1" = v001."id"
  AND v003."link" = v004."id"
  AND v004."link_type" = v005."id"
  AND v003."entity0" = v006."id"
  AND v006."type" = v007."id"
  AND v008."artist" = v006."id"
  AND v008."artist_credit" = v009."id"
  AND v010."artist_credit" = v009."id"
  AND v011."recording" = v010."id"
  AND v012."entity0" = v010."id"
  AND v012."entity1" = v013."id"
  AND v014."work" = v013."id"
  AND v012."link" = v015."id"
  AND v015."link_type" = v016."id"
  AND v005."gid" IN ('87bfa63d-c91f-4bf2-9051-5103f7d181dd'::uuid)
  AND v016."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid);
