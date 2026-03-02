\set ON_ERROR_STOP on

\if :{?csv_dir}
\cd :csv_dir
\else
\echo 'Missing csv_dir. Use -v csv_dir=/absolute/path/to/imdb_csv.'
SELECT 1/0;
\endif

\copy aka_name        FROM 'aka_name.csv'        WITH (FORMAT csv, ESCAPE E'\\')
\copy aka_title       FROM 'aka_title.csv'       WITH (FORMAT csv, ESCAPE E'\\')
\copy cast_info       FROM 'cast_info.csv'       WITH (FORMAT csv, ESCAPE E'\\')
\copy char_name       FROM 'char_name.csv'       WITH (FORMAT csv, ESCAPE E'\\')
\copy comp_cast_type  FROM 'comp_cast_type.csv'  WITH (FORMAT csv, ESCAPE E'\\')
\copy company_name    FROM 'company_name.csv'    WITH (FORMAT csv, ESCAPE E'\\')
\copy company_type    FROM 'company_type.csv'    WITH (FORMAT csv, ESCAPE E'\\')
\copy complete_cast   FROM 'complete_cast.csv'   WITH (FORMAT csv, ESCAPE E'\\')
\copy info_type       FROM 'info_type.csv'       WITH (FORMAT csv, ESCAPE E'\\')
\copy keyword         FROM 'keyword.csv'         WITH (FORMAT csv, ESCAPE E'\\')
\copy kind_type       FROM 'kind_type.csv'       WITH (FORMAT csv, ESCAPE E'\\')
\copy link_type       FROM 'link_type.csv'       WITH (FORMAT csv, ESCAPE E'\\')
\copy movie_companies FROM 'movie_companies.csv' WITH (FORMAT csv, ESCAPE E'\\')
\copy movie_info      FROM 'movie_info.csv'      WITH (FORMAT csv, ESCAPE E'\\')
\copy movie_info_idx  FROM 'movie_info_idx.csv'  WITH (FORMAT csv, ESCAPE E'\\')
\copy movie_keyword   FROM 'movie_keyword.csv'   WITH (FORMAT csv, ESCAPE E'\\')
\copy movie_link      FROM 'movie_link.csv'      WITH (FORMAT csv, ESCAPE E'\\')
\copy name            FROM 'name.csv'            WITH (FORMAT csv, ESCAPE E'\\')
\copy person_info     FROM 'person_info.csv'     WITH (FORMAT csv, ESCAPE E'\\')
\copy role_type       FROM 'role_type.csv'       WITH (FORMAT csv, ESCAPE E'\\')
\copy title           FROM 'title.csv'           WITH (FORMAT csv, ESCAPE E'\\')
