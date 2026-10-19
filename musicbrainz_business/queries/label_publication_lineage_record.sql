-- Query: label_publication_lineage_record
-- Question: Which typed labels in typed areas can be traced through a release, release group, credited artist, formatted medium, track, and recording?
-- Inclusion rule: The output is a label publication lineage. Every relation is part of the label, release credit, or physical publication path; unrelated label relationships and editorial enrichment are excluded.
SELECT count(DISTINCT v001."id")
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
  musicbrainz."recording" AS v014
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
  AND v013."recording" = v014."id";
