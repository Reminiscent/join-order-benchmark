-- Query: event_recording_identifier_provenance_audit
-- Question: Which typed events are explicitly recorded-at sources for an ISRC recording with performance-linked ISWC work lineage?
-- Inclusion rule: The event-recording relationship is restricted to recorded at. It establishes catalog provenance, not that every recording from the event is represented.
SELECT count(DISTINCT (v014."id", v001."id"))
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
  musicbrainz."link_type" AS v018
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
  AND v008."gid" IN ('a3005666-a872-32c3-ad06-98af558e99b0'::uuid)
  AND v018."gid" IN ('b06e6732-2603-47d3-8a49-9f589b430483'::uuid);
