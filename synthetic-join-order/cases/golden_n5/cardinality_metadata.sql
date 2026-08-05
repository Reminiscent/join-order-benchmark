INSERT INTO bench.join_order_relation
    (instance_id, canonical_id, relation, base_rows, base_pages)
VALUES
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 0, 'sjo_34ea18846648.r0000'::pg_catalog.regclass, 631707::double precision, 3159::bigint),
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 1, 'sjo_34ea18846648.r0001'::pg_catalog.regclass, 13873::double precision, 70::bigint),
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 2, 'sjo_34ea18846648.r0002'::pg_catalog.regclass, 1491::double precision, 8::bigint),
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 3, 'sjo_34ea18846648.r0003'::pg_catalog.regclass, 748299::double precision, 3742::bigint),
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 4, 'sjo_34ea18846648.r0004'::pg_catalog.regclass, 34829::double precision, 175::bigint);

INSERT INTO bench.join_order_edge
    (instance_id, left_id, right_id, left_key_ndv, right_key_ndv, selectivity)
VALUES
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 0, 2, 928::bigint, 1491::bigint, 1.7592776337608644e-05::double precision),
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 1, 3, 3580::bigint, 2751::bigint, 3.9640072758019316e-05::double precision),
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 1, 4, 13873::bigint, 11857::bigint, 2.6640024185579303e-06::double precision),
    ('34ea18846648c641b9b908c0e6854bd9436f96b748f6c8081c953063f4d3886e', 2, 4, 1491::bigint, 225::bigint, 0.00016671613508214888::double precision);
