INSERT INTO bench.join_order_relation
    (instance_id, canonical_id, relation, base_rows, base_pages)
VALUES
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 0, 'sjo_35c3f6ca885d.r0000'::pg_catalog.regclass, 44355::double precision, 309::bigint),
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 1, 'sjo_35c3f6ca885d.r0001'::pg_catalog.regclass, 13173::double precision, 54::bigint),
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 2, 'sjo_35c3f6ca885d.r0002'::pg_catalog.regclass, 197296::double precision, 4239::bigint),
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 3, 'sjo_35c3f6ca885d.r0003'::pg_catalog.regclass, 769710::double precision, 2725::bigint),
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 4, 'sjo_35c3f6ca885d.r0004'::pg_catalog.regclass, 4610::double precision, 147::bigint);

INSERT INTO bench.join_order_edge
    (instance_id, left_id, right_id, left_key_ndv, right_key_ndv, selectivity)
VALUES
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 0, 2, 3766::bigint, 3766::bigint, 5.837950535404487e-05::double precision),
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 1, 3, 13172::bigint, 666513::bigint, 2.7060481781099128e-07::double precision),
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 1, 4, 13172::bigint, 4609::bigint, 3.6446233632450152e-06::double precision),
    ('35c3f6ca885d75d10e848465c044f03d9130749ec58463f3cbe1c2562d24da29', 2, 4, 843::bigint, 843::bigint, 0.00062778175215374857::double precision);
