import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts.data_shards import assemble, pack


class DataShardsTest(unittest.TestCase):
    def test_daily_round_trip_and_stable_archives(self):
        rows = [
            {"id": 1, "creationTimestamp": 1735689600},
            {"id": 2, "creationTimestamp": 1738368000},
            {"id": 3, "creationTimestamp": 1738454400},
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            shards = root / "tasks"
            output = root / "assembled.json"
            source.write_text(json.dumps(rows))

            pack(source, shards, "creationTimestamp")
            names = sorted(path.name for path in shards.iterdir())
            self.assertEqual(names, ["2025-01-01.json.gz", "2025-02-01.json.gz", "2025-02-02.json.gz"])
            original = {path.name: path.read_bytes() for path in shards.iterdir()}
            pack(source, shards, "creationTimestamp")
            self.assertEqual(original, {path.name: path.read_bytes() for path in shards.iterdir()})

            rows.append({"id": 4, "creationTimestamp": 1738454401})
            source.write_text(json.dumps(rows))
            pack(source, shards, "creationTimestamp")
            self.assertEqual((shards / "2025-01-01.json.gz").read_bytes(), original["2025-01-01.json.gz"])

            assemble(shards, output)
            self.assertEqual(json.loads(output.read_text()), rows)
            with gzip.open(shards / "2025-02-02.json.gz", "rt") as shard:
                self.assertEqual(len(json.load(shard)), 2)

    def test_daily_shards_avoid_monthly_size_limit(self):
        rows = [
            {"id": 1, "creationTimestamp": 1738368000},
            {"id": 2, "creationTimestamp": 1738454400},
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            shards = root / "tasks"
            source.write_text(json.dumps(rows))
            shards.mkdir()
            old_shard = shards / "2025-02.json.gz"
            old_shard.write_bytes(gzip.compress(
                json.dumps(rows, separators=(",", ":")).encode(), mtime=0
            ))
            self.assertGreater(old_shard.stat().st_size, 70)

            with patch("scripts.data_shards.MAX_SHARD_BYTES", 70):
                pack(source, shards, "creationTimestamp")
            self.assertFalse(old_shard.exists())
            self.assertTrue(all(path.stat().st_size <= 70 for path in shards.iterdir()))
            assemble(shards, root / "assembled.json")
            self.assertEqual(json.loads((root / "assembled.json").read_text()), rows)

            original = {path.name: path.read_bytes() for path in shards.iterdir()}
            with patch("scripts.data_shards.MAX_SHARD_BYTES", 1):
                with self.assertRaisesRegex(ValueError, "exceeds 50 MiB"):
                    pack(source, shards, "creationTimestamp")
            self.assertEqual(original, {path.name: path.read_bytes() for path in shards.iterdir()})

    def test_recent_assembly_keeps_cutoff_day(self):
        now = datetime.now(timezone.utc)
        recent = {"id": 1, "creationTimestamp": int(now.timestamp())}
        cutoff = (now - timedelta(days=31)).replace(hour=0, minute=0, second=0, microsecond=0)
        boundary = {"id": 3, "creationTimestamp": int(cutoff.timestamp())}
        old = {"id": 2, "creationTimestamp": int((cutoff - timedelta(days=1)).timestamp())}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            source.write_text(json.dumps([old, boundary, recent]))
            pack(source, root / "tasks", "creationTimestamp")
            with patch("scripts.data_shards.datetime") as clock:
                clock.now.return_value = now
                assemble(root / "tasks", root / "recent.json", since_days=31)
            self.assertEqual(json.loads((root / "recent.json").read_text()), [boundary, recent])


if __name__ == "__main__":
    unittest.main()
