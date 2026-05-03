from __future__ import annotations

from typing import Optional

from bench_workloads import (
    PREPARE_MARKERS,
    dataset_prepare_scripts,
    resolve_prepare_dataset_runs,
)
from bench_common import ConnOpts, Scenario, die, psql_cmd, psql_file, psql_sql, run_cmd, sql_identifier, sql_literal


def database_exists(db: str, conn: Optional[ConnOpts] = None) -> bool:
    c = conn or ConnOpts()
    cmd = ["psql", "-X", "-q", "-At", *c.to_args(), "-d", "postgres"]
    sql = f"SELECT 1 FROM pg_database WHERE datname = {sql_literal(db)};\n"
    p = run_cmd(cmd, input_text=sql, check=False)
    if p.returncode != 0:
        die(f"failed to check whether database '{db}' exists")
    return (p.stdout or "").strip() == "1"


def table_exists(db: str, table_name: str, conn: Optional[ConnOpts] = None) -> bool:
    sql = f"SELECT to_regclass({sql_literal(table_name.lower())});\n"
    p = run_cmd(psql_cmd(db, conn) + ["-At"], input_text=sql, check=False)
    if p.returncode != 0:
        return False
    return bool((p.stdout or "").strip())


def drop_and_create_db(db: str, conn: Optional[ConnOpts] = None) -> None:
    ident = sql_identifier(db)
    script = "\n".join(
        [
            "SELECT pg_terminate_backend(pid)",
            "FROM pg_stat_activity",
            f"WHERE datname = {sql_literal(db)} AND pid <> pg_backend_pid();",
            f"DROP DATABASE IF EXISTS {ident};",
            f"CREATE DATABASE {ident};",
            "",
        ]
    )
    psql_sql("postgres", script, conn=conn, check=True)


def dataset_is_prepared(dataset: str, db: str, conn: Optional[ConnOpts] = None) -> bool:
    markers = PREPARE_MARKERS.get(dataset)
    if markers is None:
        return False
    return all(table_exists(db, marker, conn) for marker in markers)


def prepare_dataset(
    dataset: str,
    db: str,
    csv_dir: Optional[str],
    conn: Optional[ConnOpts] = None,
    *,
    force_recreate: bool = False,
) -> None:
    schema_sql, load_sql, index_sql, needs_csv_dir = dataset_prepare_scripts(dataset)
    if needs_csv_dir and not csv_dir:
        die(f"dataset '{dataset}' requires --csv-dir /absolute/path/to/imdb_csv")

    if database_exists(db, conn):
        if force_recreate:
            drop_and_create_db(db, conn)
        elif dataset_is_prepared(dataset, db, conn):
            print(f"[prepare] skip dataset={dataset} db={db} (already prepared)")
            return
        else:
            die(
                f"database '{db}' already exists but does not look prepared for dataset '{dataset}'. "
                "Use --force-recreate to rebuild it."
            )
    else:
        drop_and_create_db(db, conn)

    print(f"[prepare] dataset={dataset} db={db}")
    psql_file(db, schema_sql, conn=conn, check=True)
    psql_file(db, load_sql, conn=conn, vars=({"csv_dir": csv_dir} if csv_dir else None), check=True)
    if index_sql is not None:
        psql_file(db, index_sql, conn=conn, check=True)


def prepare_scenario(
    scenario: Scenario,
    *,
    csv_dir: Optional[str],
    conn: Optional[ConnOpts],
    force_recreate: bool,
) -> None:
    resolved = resolve_prepare_dataset_runs(scenario)

    prepared: set[str] = set()
    for entry in resolved:
        if entry.db in prepared:
            continue
        prepare_dataset(entry.dataset, entry.db, csv_dir, conn, force_recreate=force_recreate)
        prepared.add(entry.db)

    if prepared:
        print(f"[prepare] done dbs={','.join(sorted(prepared))}")
