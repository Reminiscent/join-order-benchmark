-- Query: event_venue_recording_provenance_consistency
-- Question: Which typed events have both an explicit held-at venue and an explicit recorded-at ISRC recording with performance-linked ISWC work lineage?
-- Inclusion rule: Held-at and recorded-at are separate typed provenance facts. The query does not assert that the venue is a studio or that every event recording was captured there.
SELECT count(DISTINCT (v014."id", v019."id", v001."id"))
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
  musicbrainz."work_type" AS v010,
  musicbrainz."iswc" AS v011,
  musicbrainz."work_language" AS v012,
  musicbrainz."language" AS v013,
  musicbrainz."event" AS v014,
  musicbrainz."event_type" AS v015,
  musicbrainz."l_event_recording" AS v016,
  musicbrainz."link" AS v017,
  musicbrainz."link_type" AS v018,
  musicbrainz."place" AS v019,
  musicbrainz."place_type" AS v020,
  musicbrainz."area" AS v021,
  musicbrainz."l_event_place" AS v022,
  musicbrainz."link" AS v023,
  musicbrainz."link_type" AS v024
WHERE
  v002."recording" = v001."id"
  AND v001."artist_credit" = v003."id"
  AND v004."artist_credit" = v003."id"
  AND v004."artist" = v005."id"
  AND v006."entity0" = v001."id"
  AND v006."link" = v007."id"
  AND v007."link_type" = v008."id"
  AND v006."entity1" = v009."id"
  AND v009."type" = v010."id"
  AND v011."work" = v009."id"
  AND v012."work" = v009."id"
  AND v012."language" = v013."id"
  AND v014."type" = v015."id"
  AND v016."entity0" = v014."id"
  AND v016."entity1" = v001."id"
  AND v016."link" = v017."id"
  AND v017."link_type" = v018."id"
  AND v019."type" = v020."id"
  AND v019."area" = v021."id"
  AND v022."entity0" = v014."id"
  AND v022."entity1" = v019."id"
  AND v022."link" = v023."id"
  AND v023."link_type" = v024."id"
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v018."gid" IN ('b06e6732-2603-47d3-8a49-9f589b430483'::uuid)
  AND v024."gid" IN ('e2c6f697-07dc-38b1-be0b-83d740165532'::uuid);
