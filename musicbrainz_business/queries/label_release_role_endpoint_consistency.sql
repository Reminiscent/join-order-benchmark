-- Query: label_release_role_endpoint_consistency
-- Question: Which registered label-release pairs also have an explicit typed label-release business relationship and typed external endpoints for both entities?
-- Inclusion rule: The release_label registration and explicit relationship must converge on the same label and release. The explicit role and endpoint types are evidence and are not collapsed into ownership.
SELECT count(DISTINCT (v001."id", v006."id"))
FROM
  musicbrainz."label" AS v001,
  musicbrainz."label_type" AS v002,
  musicbrainz."area" AS v003,
  musicbrainz."area_type" AS v004,
  musicbrainz."release_label" AS v005,
  musicbrainz."release" AS v006,
  musicbrainz."release_group" AS v007,
  musicbrainz."artist_credit" AS v008,
  musicbrainz."artist_credit_name" AS v009,
  musicbrainz."artist" AS v010,
  musicbrainz."medium" AS v011,
  musicbrainz."medium_format" AS v012,
  musicbrainz."track" AS v013,
  musicbrainz."recording" AS v014,
  musicbrainz."l_label_release" AS v015,
  musicbrainz."link" AS v016,
  musicbrainz."link_type" AS v017,
  musicbrainz."l_label_url" AS v018,
  musicbrainz."link" AS v019,
  musicbrainz."link_type" AS v020,
  musicbrainz."url" AS v021,
  musicbrainz."l_release_url" AS v022,
  musicbrainz."link" AS v023,
  musicbrainz."link_type" AS v024,
  musicbrainz."url" AS v025
WHERE
  v001."type" = v002."id"
  AND v001."area" = v003."id"
  AND v003."type" = v004."id"
  AND v005."label" = v001."id"
  AND v005."release" = v006."id"
  AND v006."release_group" = v007."id"
  AND v006."artist_credit" = v008."id"
  AND v009."artist_credit" = v008."id"
  AND v009."artist" = v010."id"
  AND v011."release" = v006."id"
  AND v011."format" = v012."id"
  AND v013."medium" = v011."id"
  AND v013."recording" = v014."id"
  AND v015."entity0" = v001."id"
  AND v015."entity1" = v006."id"
  AND v015."link" = v016."id"
  AND v016."link_type" = v017."id"
  AND v018."entity0" = v001."id"
  AND v018."link" = v019."id"
  AND v019."link_type" = v020."id"
  AND v018."entity1" = v021."id"
  AND v022."entity0" = v006."id"
  AND v022."link" = v023."id"
  AND v023."link_type" = v024."id"
  AND v022."entity1" = v025."id";
