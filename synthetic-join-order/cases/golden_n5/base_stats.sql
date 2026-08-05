DO $synthetic_join_order$
BEGIN
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0000',
        'relpages', 3159::integer,
        'reltuples', 631707::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0000';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0000',
        'attname', 'j_e_0000_0002',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 928::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 928)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0000.j_e_0000_0002';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0001',
        'relpages', 70::integer,
        'reltuples', 13873::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0001';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0001',
        'attname', 'j_e_0001_0003',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 3580::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 3580)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0001.j_e_0001_0003';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0001',
        'attname', 'j_e_0001_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 13873::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 13873)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0001.j_e_0001_0004';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0002',
        'relpages', 8::integer,
        'reltuples', 1491::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0002';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0002',
        'attname', 'j_e_0000_0002',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 1491::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 1491)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0002.j_e_0000_0002';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0002',
        'attname', 'j_e_0002_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 1491::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 1491)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0002.j_e_0002_0004';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0003',
        'relpages', 3742::integer,
        'reltuples', 748299::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0003';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0003',
        'attname', 'j_e_0001_0003',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 2751::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 2751)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0003.j_e_0001_0003';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0004',
        'relpages', 175::integer,
        'reltuples', 34829::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0004';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0004',
        'attname', 'j_e_0001_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 11857::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 11857)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0004.j_e_0001_0004';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_34ea18846648',
        'relname', 'r0004',
        'attname', 'j_e_0002_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 225::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 225)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0004.j_e_0002_0004';
    END IF;
END
$synthetic_join_order$;
