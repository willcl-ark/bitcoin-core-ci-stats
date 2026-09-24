import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.data_shards import assemble, pack


class DataShardsTest(unittest.TestCase):
    def test_monthly_round_trip_and_stable_archives(self):
        rows = [
            {"id": 1, "creationTimestamp": 1735689600},
            {"id": 2, "creationTimestamp": 1738368000},
            {"id": 3, "creationTimestamp": 1738368001},
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            shards = root / "tasks"
            output = root / "assembled.json"
            source.write_text(json.dumps(rows))

            pack(source, shards, "creationTimestamp")
            names = sorted(path.name for path in shards.iterdir())
            self.assertEqual(names, ["2025-01.json.gz", "2025-02.json.gz"])
            original = {path.name: path.read_bytes() for path in shards.iterdir()}
            pack(source, shards, "creationTimestamp")
            self.assertEqual(original, {path.name: path.read_bytes() for path in shards.iterdir()})

            rows.append({"id": 4, "creationTimestamp": 1738368002})
            source.write_text(json.dumps(rows))
            pack(source, shards, "creationTimestamp")
            self.assertEqual((shards / "2025-01.json.gz").read_bytes(), original["2025-01.json.gz"])

            assemble(shards, output)
            self.assertEqual(json.loads(output.read_text()), rows)
            with gzip.open(shards / "2025-02.json.gz", "rt") as shard:
                self.assertEqual(len(json.load(shard)), 3)

    def test_recent_assembly_keeps_cutoff_month(self):
        now = datetime.now(timezone.utc)
        recent = {"id": 1, "creationTimestamp": int(now.timestamp())}
        old = {"id": 2, "creationTimestamp": int((now - timedelta(days=90)).timestamp())}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            source.write_text(json.dumps([old, recent]))
            pack(source, root / "tasks", "creationTimestamp")
            assemble(root / "tasks", root / "recent.json", since_days=31)
            self.assertEqual(json.loads((root / "recent.json").read_text()), [recent])


if __name__ == "__main__":
    unittest.main()
