import gzip
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts.data_shards import assemble, pack


class DataShardsTest(unittest.TestCase):
    def test_interleaved_days_preserve_row_values_and_order(self):
        rows = [
            {"id": 2**64 - 1, "creationTimestamp": 1738454400,
             "payload": {"text": "雪\n\"quoted\" \\ 😀", "values": [None, True, 0.125]}},
            {"id": 2, "creationTimestamp": 1738368000},
            {"id": 3, "creationTimestamp": 1738454401},
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            source.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            pack(source, root / "shards", "creationTimestamp")
            assemble(root / "shards", root / "assembled.json")
            self.assertEqual(json.loads((root / "assembled.json").read_text()),
                             [rows[1], rows[0], rows[2]])

    def test_invalid_input_preserves_shards_and_cleans_temporary_files(self):
        import ijson

        row = {"id": 1, "creationTimestamp": 1738368000}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            shards = root / "shards"
            source.write_text(json.dumps([row]))
            pack(source, shards, "creationTimestamp")
            original = {path.name: path.read_bytes() for path in shards.iterdir()}
            for invalid in ["", "{}", "[" + json.dumps(row) + ",",
                            json.dumps([row]) + " garbage"]:
                with self.subTest(input=invalid):
                    source.write_text(invalid)
                    with self.assertRaises((ValueError, ijson.JSONError)):
                        pack(source, shards, "creationTimestamp")
                    self.assertEqual(original, {path.name: path.read_bytes()
                                                for path in shards.iterdir()})
                    self.assertEqual({path.name for path in root.iterdir()},
                                     {"tasks.json", "shards"})

    def test_empty_array_removes_old_shards(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            source.write_text('[{"creationTimestamp":1738368000}]')
            pack(source, root / "shards", "creationTimestamp")
            source.write_text("[]")
            pack(source, root / "shards", "creationTimestamp")
            self.assertEqual(list((root / "shards").iterdir()), [])

    @unittest.skipUnless(sys.platform == "linux", "requires Linux address-space limits")
    def test_pack_memory_does_not_grow_with_archive_size(self):
        row = json.dumps({"creationTimestamp": 1738368000, "payload": "x" * 2048},
                         separators=(",", ":")).encode()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            with source.open("wb") as output:
                output.write(b"[")
                for index in range(32768):
                    if index:
                        output.write(b",")
                    output.write(row)
                output.write(b"]")

            result = subprocess.run(
                [sys.executable, "-c", """
import resource
import sys
from pathlib import Path
from scripts.data_shards import pack
limit = 128 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
pack(Path(sys.argv[1]), Path(sys.argv[2]), "creationTimestamp")
""", str(source), str(root / "shards")],
                cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            with source.open("rb") as original:
                expected = hashlib.file_digest(original, "sha256").digest()
            with gzip.open(root / "shards" / "2025-02-01.json.gz", "rb") as packed:
                self.assertEqual(hashlib.file_digest(packed, "sha256").digest(), expected)

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
                with self.assertRaisesRegex(ValueError, "single row .* exceeds 1 compressed bytes"):
                    pack(source, shards, "creationTimestamp")
            self.assertEqual(original, {path.name: path.read_bytes() for path in shards.iterdir()})

    def test_oversized_day_is_split_without_losing_rows(self):
        rows = [
            {"id": index, "creationTimestamp": 1738368000, "payload": str(index) * 100}
            for index in range(32)
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tasks.json"
            shards = root / "tasks"
            source.write_text(json.dumps(rows))
            with patch("scripts.data_shards.MAX_SHARD_BYTES", 120):
                pack(source, shards, "creationTimestamp")
                files = sorted(shards.iterdir())
                self.assertGreater(len(files), 2)
                self.assertTrue(all(path.stat().st_size <= 120 for path in files))
                original = {path.name: path.read_bytes() for path in files}
                pack(source, shards, "creationTimestamp")
                self.assertEqual(original, {path.name: path.read_bytes() for path in shards.iterdir()})
            assemble(shards, root / "assembled.json")
            self.assertEqual(json.loads((root / "assembled.json").read_text()), rows)

            with patch("scripts.data_shards.datetime") as clock:
                clock.now.return_value = datetime(2025, 3, 4, tzinfo=timezone.utc)
                assemble(shards, root / "recent.json", since_days=31)
            self.assertEqual(json.loads((root / "recent.json").read_text()), rows)

            source.write_text(json.dumps(rows[:1]))
            pack(source, shards, "creationTimestamp")
            self.assertEqual([path.name for path in shards.iterdir()], ["2025-02-01.json.gz"])
            assemble(shards, root / "assembled.json")
            self.assertEqual(json.loads((root / "assembled.json").read_text()), rows[:1])

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
