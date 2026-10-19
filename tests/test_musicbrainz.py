from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKLOAD = ROOT / "musicbrainz_business"
FROM = re.compile(r"\bFROM\b(.*?)\bWHERE\b", re.IGNORECASE | re.DOTALL)
RELATION = re.compile(r'musicbrainz\."([^"]+)"\s+AS\s+(\w+)', re.IGNORECASE)


class MusicBrainzTests(unittest.TestCase):
    def test_queries_are_complete_and_business_described(self) -> None:
        queries = sorted((WORKLOAD / "queries").glob("*.sql"))
        self.assertEqual(len(queries), 44)

        widths = []
        physical_widths = []
        covered_tables = set()
        allowed_tables = set((WORKLOAD / "tables.txt").read_text().splitlines())
        for query in queries:
            sql = query.read_text()
            self.assertIn("-- Question:", sql, query.name)
            self.assertIn("-- Inclusion rule:", sql, query.name)
            self.assertIn("SELECT count(DISTINCT", sql, query.name)
            match = FROM.search(sql)
            self.assertIsNotNone(match, query.name)
            relations = RELATION.findall(match.group(1))
            self.assertEqual(len(relations), len({alias for _, alias in relations}), query.name)
            tables = {table for table, _ in relations}
            self.assertLessEqual(tables, allowed_tables, query.name)
            widths.append(len(relations))
            physical_widths.append(len(tables))
            covered_tables.update(tables)

        self.assertEqual((min(widths), max(widths)), (12, 51))
        self.assertEqual((min(physical_widths), max(physical_widths)), (12, 42))
        self.assertEqual(covered_tables, allowed_tables)

    def test_sources_are_pinned(self) -> None:
        source = json.loads((WORKLOAD / "source.lock.json").read_text())
        data = json.loads((WORKLOAD / "real_data.lock.json").read_text())
        self.assertEqual(
            source["revision"],
            "cff977f0ba8f06d5fa594e7590f6a134b1a5a22c",
        )
        self.assertEqual(
            data["datasets"]["official_full"]["snapshot"],
            "20260905-002519",
        )


if __name__ == "__main__":
    unittest.main()
