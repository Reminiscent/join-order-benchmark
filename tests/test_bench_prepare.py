from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

import bench_prepare


class BenchPrepareTests(unittest.TestCase):
    def test_prepare_dataset_recreates_before_loading(self) -> None:
        calls: list[tuple[str, str]] = []

        def record_drop_and_create(db: str, _conn: object = None) -> None:
            calls.append(("drop_create", db))

        def record_psql_file(db: str, path: Path, **_kwargs: object) -> None:
            calls.append(("psql_file", f"{db}:{path.name}"))

        with (
            patch.object(
                bench_prepare,
                "dataset_prepare_scripts",
                Mock(return_value=(Path("schema.sql"), Path("load.sql"), Path("index.sql"), False)),
            ),
            patch.object(bench_prepare, "drop_and_create_db", side_effect=record_drop_and_create),
            patch.object(bench_prepare, "psql_file", side_effect=record_psql_file),
        ):
            bench_prepare.prepare_dataset("dataset", "bench_db", csv_dir=None)

        self.assertEqual(
            calls,
            [
                ("drop_create", "bench_db"),
                ("psql_file", "bench_db:schema.sql"),
                ("psql_file", "bench_db:load.sql"),
                ("psql_file", "bench_db:index.sql"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
