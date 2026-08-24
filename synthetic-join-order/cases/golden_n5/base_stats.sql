DO $synthetic_join_order$
BEGIN
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0000',
        'relpages', 309::integer,
        'reltuples', 44355::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0000';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0000',
        'attname', 'payload',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 49::integer,
        'n_distinct', -1::real) THEN
        RAISE EXCEPTION 'failed to restore payload stats for r0000';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0000',
        'attname', 'j_e_0000_0002',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 3766::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 3766)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0000.j_e_0000_0002';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0001',
        'relpages', 54::integer,
        'reltuples', 13173::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0001';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0001',
        'attname', 'payload',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 17::integer,
        'n_distinct', -1::real) THEN
        RAISE EXCEPTION 'failed to restore payload stats for r0001';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0001',
        'attname', 'j_e_0001_0003',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 13172::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 13172)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0001.j_e_0001_0003';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0001',
        'attname', 'j_e_0001_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 13172::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 13172)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0001.j_e_0001_0004';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0002',
        'relpages', 4239::integer,
        'reltuples', 197296::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0002';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0002',
        'attname', 'payload',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 160::integer,
        'n_distinct', -1::real) THEN
        RAISE EXCEPTION 'failed to restore payload stats for r0002';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0002',
        'attname', 'j_e_0000_0002',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 3766::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 3766)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0002.j_e_0000_0002';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0002',
        'attname', 'j_e_0002_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 843::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 843)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0002.j_e_0002_0004';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0003',
        'relpages', 2725::integer,
        'reltuples', 769710::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0003';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0003',
        'attname', 'payload',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 21::integer,
        'n_distinct', -1::real) THEN
        RAISE EXCEPTION 'failed to restore payload stats for r0003';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0003',
        'attname', 'j_e_0001_0003',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 666513::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 666513)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0003.j_e_0001_0003';
    END IF;
    IF NOT pg_catalog.pg_restore_relation_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0004',
        'relpages', 147::integer,
        'reltuples', 4610::real) THEN
        RAISE EXCEPTION 'failed to restore relation stats for r0004';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0004',
        'attname', 'payload',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 244::integer,
        'n_distinct', -1::real) THEN
        RAISE EXCEPTION 'failed to restore payload stats for r0004';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0004',
        'attname', 'j_e_0001_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 4609::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 4609)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0004.j_e_0001_0004';
    END IF;
    IF NOT pg_catalog.pg_restore_attribute_stats(
        'schemaname', 'sjo_35c3f6ca885d',
        'relname', 'r0004',
        'attname', 'j_e_0002_0004',
        'inherited', false::boolean,
        'null_frac', 0::real,
        'avg_width', 8::integer,
        'n_distinct', 843::real,
        'most_common_vals', '{0}'::text,
        'most_common_freqs', ARRAY[(1.0 / 843)::real]::real[]) THEN
        RAISE EXCEPTION 'failed to restore attribute stats for r0004.j_e_0002_0004';
    END IF;
END
$synthetic_join_order$;
