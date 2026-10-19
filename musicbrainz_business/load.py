#!/usr/bin/env python3
"""Load workload-relevant tables from pinned MusicBrainz dump archives."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parent


SCHEMA_SCRIPTS_BEFORE_IMPORT = (
    "Extensions.sql",
    "CreateCollations.sql",
    "CreateTypes.sql",
    "CreateTables.sql",
)
SCHEMA_SCRIPTS_AFTER_IMPORT = (
    "CreatePrimaryKeys.sql",
    "CreateSearchConfiguration.sql",
    "CreateFunctions.sql",
    "CreateIndexes.sql",
)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def selected_tables() -> set[str]:
    return set((ROOT / "tables.txt").read_text().splitlines())


def psql_command(args: argparse.Namespace) -> list[str]:
    command = ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1"]
    if args.host:
        command.extend(["-h", args.host])
    if args.port:
        command.extend(["-p", str(args.port)])
    if args.user:
        command.extend(["-U", args.user])
    command.extend(["-d", args.database])
    return command


def psql_environment() -> dict[str, str]:
    return {**os.environ, "PGOPTIONS": "-c search_path=musicbrainz,public"}


def run_sql(args: argparse.Namespace, sql: str) -> str:
    result = subprocess.run(
        psql_command(args) + ["-A", "-t"],
        input=sql,
        text=True,
        capture_output=True,
        env=psql_environment(),
    )
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout).strip())
    return result.stdout.strip()


def run_file(args: argparse.Namespace, path: Path) -> None:
    print(f"[load] applying {path.name}", flush=True)
    result = subprocess.run(
        psql_command(args) + ["-f", str(path)], env=psql_environment()
    )
    if result.returncode:
        raise RuntimeError(f"psql failed for {path}")


def verify_sources(source_dir: Path) -> None:
    lock = read_json(ROOT / "source.lock.json")
    for entry in lock["source_files"]:
        path = source_dir / entry["path"]
        if not path.is_file():
            raise RuntimeError(f"missing pinned schema source: {path}")
        actual = sha256_file(path)
        if actual != entry["sha256"]:
            raise RuntimeError(
                f"schema checksum mismatch for {path}: "
                f"expected {entry['sha256']}, got {actual}"
            )


def archive_inventory(archive: Path) -> tuple[set[str], dict[str, str]]:
    tables: set[str] = set()
    metadata: dict[str, str] = {}
    with tarfile.open(archive, mode="r:*") as source:
        for member in source.getmembers():
            if not member.isfile():
                continue
            name = PurePosixPath(member.name).name
            if name in {"SCHEMA_SEQUENCE", "TIMESTAMP", "REPLICATION_SEQUENCE"}:
                extracted = source.extractfile(member)
                assert extracted is not None
                metadata[name] = extracted.read().decode().strip()
            elif PurePosixPath(member.name).parent.name == "mbdump":
                tables.add(name)
    return tables, metadata


def copy_member(
    args: argparse.Namespace,
    archive: tarfile.TarFile,
    member: tarfile.TarInfo,
    table: str,
) -> int:
    command = psql_command(args)
    process = subprocess.Popen(command, stdin=subprocess.PIPE, env=psql_environment())
    assert process.stdin is not None
    process.stdin.write(
        f'SET search_path = musicbrainz, public;\nCOPY "{table}" FROM STDIN;\n'.encode()
    )
    extracted = archive.extractfile(member)
    assert extracted is not None
    rows = 0
    ends_with_newline = True
    while chunk := extracted.read(1024 * 1024):
        rows += chunk.count(b"\n")
        ends_with_newline = chunk.endswith(b"\n")
        process.stdin.write(chunk)
    if not ends_with_newline:
        process.stdin.write(b"\n")
    process.stdin.write(b"\\.\n")
    process.stdin.close()
    if process.wait():
        raise RuntimeError(f"COPY failed for {table}")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, type=Path, action="append")
    parser.add_argument(
        "--data-mode",
        choices=("official_sample", "official_full"),
        default="official_sample",
    )
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--database", required=True)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--storage-path", type=Path, default=Path.cwd())
    parser.add_argument("--reserve-free-gib", type=float, default=20.0)
    parser.add_argument(
        "--fallback-c-collation",
        action="store_true",
        help=(
            "create the MusicBrainz collation with libc locale C instead of ICU; "
            "only valid for experiments that do not depend on MusicBrainz text ordering"
        ),
    )
    args = parser.parse_args()

    verify_sources(args.source_dir)
    tables = selected_tables()
    if not tables:
        raise RuntimeError("selected workload scope contains no tables")
    lock = read_json(ROOT / "real_data.lock.json")
    dataset_lock = lock["datasets"][args.data_mode]
    expected = {entry["name"]: entry for entry in dataset_lock["files"]}
    archive_names = {path.name for path in args.archive}
    if archive_names != set(expected):
        raise RuntimeError(
            f"{args.data_mode} requires archives {sorted(expected)}, got {sorted(archive_names)}"
        )
    archive_reports = []
    for archive_path in args.archive:
        entry = expected[archive_path.name]
        if archive_path.stat().st_size != entry["bytes"]:
            raise RuntimeError(f"archive size mismatch: {archive_path}")
        digest = sha256_file(archive_path)
        if digest != entry["sha256"]:
            raise RuntimeError(f"archive checksum mismatch: {archive_path}")
        archive_reports.append(
            {
                "name": archive_path.name,
                "path": str(archive_path),
                "bytes": archive_path.stat().st_size,
                "sha256": digest,
                "license": entry["license"],
                "role": entry["role"],
            }
        )
    archive_metadata: dict[str, str] = {}
    if args.data_mode == "official_sample":
        archive_tables, archive_metadata = archive_inventory(args.archive[0])
        missing = sorted(tables - archive_tables)
        if missing:
            raise RuntimeError(
                f"sample is missing {len(missing)} selected tables: {missing}"
            )
    if run_sql(args, "SELECT to_regnamespace('musicbrainz') IS NULL;") != "t":
        raise RuntimeError("target database must not already contain schema musicbrainz")

    sql_dir = args.source_dir / "admin/sql"
    run_sql(args, "CREATE SCHEMA musicbrainz;")
    for name in SCHEMA_SCRIPTS_BEFORE_IMPORT:
        if name == "CreateCollations.sql" and args.fallback_c_collation:
            print("[sample] creating fallback libc C collation", flush=True)
            run_sql(
                args,
                "CREATE COLLATION musicbrainz "
                "(provider = libc, locale = 'C');",
            )
        else:
            run_file(args, sql_dir / name)

    imported = []
    imported_tables: set[str] = set()
    for archive_path in args.archive:
        mode = "r|xz" if archive_path.name.endswith(".xz") else "r|bz2"
        with tarfile.open(archive_path, mode=mode) as archive:
            for member in archive:
                name = PurePosixPath(member.name).name
                if member.isfile() and name in {
                    "SCHEMA_SEQUENCE", "TIMESTAMP", "REPLICATION_SEQUENCE"
                }:
                    extracted = archive.extractfile(member)
                    assert extracted is not None
                    archive_metadata[name] = extracted.read().decode().strip()
                    continue
                table = name
                if (
                    not member.isfile()
                    or table not in tables
                    or PurePosixPath(member.name).parent.name != "mbdump"
                ):
                    continue
                if table in imported_tables:
                    raise RuntimeError(f"table appears in more than one archive: {table}")
                rows = copy_member(args, archive, member, table)
                imported.append({"table": table, "rows": rows})
                imported_tables.add(table)
                print(
                    f"[{args.data_mode}] imported={len(imported)}/{len(tables)} "
                    f"table={table} rows={rows}",
                    flush=True,
                )
                if args.data_mode == "official_full":
                    free = shutil.disk_usage(args.storage_path).free
                    reserve = args.reserve_free_gib * 1024**3
                    if free < reserve:
                        raise RuntimeError(
                            f"free-space reserve breached after {table}: "
                            f"free={free / 1024**3:.2f} GiB reserve={args.reserve_free_gib:.2f} GiB"
                        )
    missing = sorted(tables - imported_tables)
    if missing:
        raise RuntimeError(
            f"{args.data_mode} archives are missing {len(missing)} selected tables: {missing}"
        )

    for name in SCHEMA_SCRIPTS_AFTER_IMPORT:
        run_file(args, sql_dir / name)
    quoted_tables = ", ".join(f'"{table}"' for table in sorted(tables))
    run_sql(args, f"ANALYZE {quoted_tables};")

    args.output.write_text(
        json.dumps({
            "data_mode": args.data_mode,
            "archive": str(args.archive[0]) if len(args.archive) == 1 else None,
            "archive_sha256": archive_reports[0]["sha256"] if len(args.archive) == 1 else None,
            "archives": archive_reports,
            "archive_metadata": archive_metadata,
            "source_revision": read_json(ROOT / "source.lock.json")["revision"],
            "selected_tables": len(tables),
            "total_rows": sum(row["rows"] for row in imported),
            "tables": sorted(imported, key=lambda row: row["table"]),
        }, indent=2, sort_keys=True) + "\n"
    )


if __name__ == "__main__":
    main()
