SELECT
    "r0000"."payload" AS "payload_0000",
    "r0001"."payload" AS "payload_0001",
    "r0002"."payload" AS "payload_0002",
    "r0003"."payload" AS "payload_0003",
    "r0004"."payload" AS "payload_0004"
FROM
    "sjo_35c3f6ca885d"."r0000" AS "r0000",
    "sjo_35c3f6ca885d"."r0001" AS "r0001",
    "sjo_35c3f6ca885d"."r0002" AS "r0002",
    "sjo_35c3f6ca885d"."r0003" AS "r0003",
    "sjo_35c3f6ca885d"."r0004" AS "r0004"
WHERE
    "r0000"."j_e_0000_0002" = "r0002"."j_e_0000_0002"
  AND "r0001"."j_e_0001_0003" = "r0003"."j_e_0001_0003"
  AND "r0001"."j_e_0001_0004" = "r0004"."j_e_0001_0004"
  AND "r0002"."j_e_0002_0004" = "r0004"."j_e_0002_0004";
