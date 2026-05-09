from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bench"))

import bench_exec
from bench_common import ConnOpts


class StatisticsDumpTests(unittest.TestCase):
    def test_dump_statistics_uses_pg_dump_statistics_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "stats" / "bench_db.sql"
            run_cmd_mock = Mock()

            with patch.object(bench_exec, "run_cmd", run_cmd_mock):
                bench_exec.dump_statistics(
                    "bench_db",
                    path,
                    ConnOpts(host="localhost", port=5432, user="bench"),
                )

            self.assertTrue(path.parent.is_dir())
            run_cmd_mock.assert_called_once_with(
                [
                    "pg_dump",
                    "-h",
                    "localhost",
                    "-p",
                    "5432",
                    "-U",
                    "bench",
                    "-d",
                    "bench_db",
                    "--statistics-only",
                    "-f",
                    str(path),
                ],
                check=True,
            )


if __name__ == "__main__":
    unittest.main()
