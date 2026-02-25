\set ON_ERROR_STOP on

\if :{?csv_dir}
\cd :csv_dir
\else
\echo 'Missing csv_dir. Use -v csv_dir=/absolute/path/to/imdb_csv.'
SELECT 1/0;
\endif

\copy aka_name        FROM 'aka_name.csv'        WITH (FORMAT csv)
\copy aka_title       FROM 'aka_title.csv'       WITH (FORMAT csv)
\copy cast_info       FROM 'cast_info.csv'       WITH (FORMAT csv)
\copy char_name       FROM 'char_name.csv'       WITH (FORMAT csv)
\copy comp_cast_type  FROM 'comp_cast_type.csv'  WITH (FORMAT csv)
\copy company_name    FROM 'company_name.csv'    WITH (FORMAT csv)
\copy company_type    FROM 'company_type.csv'    WITH (FORMAT csv)
\copy complete_cast   FROM 'complete_cast.csv'   WITH (FORMAT csv)
\copy info_type       FROM 'info_type.csv'       WITH (FORMAT csv)
\copy keyword         FROM 'keyword.csv'         WITH (FORMAT csv)
\copy kind_type       FROM 'kind_type.csv'       WITH (FORMAT csv)
\copy link_type       FROM 'link_type.csv'       WITH (FORMAT csv)
\copy movie_companies FROM 'movie_companies.csv' WITH (FORMAT csv)
\copy movie_info      FROM 'movie_info.csv'      WITH (FORMAT csv)
\copy movie_info_idx  FROM 'movie_info_idx.csv'  WITH (FORMAT csv)
\copy movie_keyword   FROM 'movie_keyword.csv'   WITH (FORMAT csv)
\copy movie_link      FROM 'movie_link.csv'      WITH (FORMAT csv)
\copy name            FROM 'name.csv'            WITH (FORMAT csv)
\copy person_info     FROM 'person_info.csv'     WITH (FORMAT csv)
\copy role_type       FROM 'role_type.csv'       WITH (FORMAT csv)
\copy title           FROM 'title.csv'           WITH (FORMAT csv)
