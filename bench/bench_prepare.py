"""Database preparation helpers for benchmark scenarios.

This module recreates benchmark databases and runs the schema/load SQL scripts
required before ``bench_run.py`` can execute queries.
"""

from __future__ import annotations

from typing import Optional

from bench_config import dataset_prepare_scripts, resolve_prepare_dataset_runs
from bench_common import ConnOpts, Scenario, die, psql_file, psql_sql, sql_identifier, sql_literal


# Database lifecycle helpers.


def drop_and_create_db(db: str, conn: Optional[ConnOpts] = None) -> None:
    """Terminate existing sessions, drop the target database, and recreate it."""
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


# Dataset and scenario preparation.


def prepare_dataset(
    dataset: str,
    db: str,
    csv_dir: Optional[str],
    conn: Optional[ConnOpts] = None,
) -> None:
    """Recreate one dataset database and run its schema/load/index scripts.

    CSV-backed IMDB workloads require ``csv_dir``; self-contained SQL workloads
    ignore it.
    """
    schema_sql, load_sql, index_sql, needs_csv_dir = dataset_prepare_scripts(dataset)
    if needs_csv_dir and not csv_dir:
        die(f"dataset '{dataset}' requires --csv-dir /absolute/path/to/imdb_csv")

    print(f"[prepare] recreate dataset={dataset} db={db}")
    drop_and_create_db(db, conn)
    psql_file(db, schema_sql, conn=conn, check=True)
    load_vars = {"csv_dir": csv_dir} if csv_dir else None
    psql_file(db, load_sql, conn=conn, vars=load_vars, check=True)
    if index_sql is not None:
        psql_file(db, index_sql, conn=conn, check=True)


def prepare_scenario(
    scenario: Scenario,
    *,
    csv_dir: Optional[str],
    conn: Optional[ConnOpts],
) -> None:
    """Prepare every distinct database required by a scenario.

    Several IMDB datasets share one physical database, so duplicate database
    names are skipped after the first recreate/load pass.
    """
    resolved = resolve_prepare_dataset_runs(scenario)

    recreated_dbs: set[str] = set()
    for entry in resolved:
        if entry.db in recreated_dbs:
            continue
        prepare_dataset(entry.dataset, entry.db, csv_dir, conn)
        recreated_dbs.add(entry.db)

    if recreated_dbs:
        print(f"[prepare] done dbs={','.join(sorted(recreated_dbs))}")
