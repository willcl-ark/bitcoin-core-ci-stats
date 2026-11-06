#!/usr/bin/env python3
"""Pack JSON rows into deterministic daily gzip files, or assemble them."""

import argparse
import gzip
import json
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import chain
from pathlib import Path
from typing import BinaryIO


MAX_SHARD_BYTES = 50 * 1024 * 1024


def write_shard(source: BinaryIO, count: int, path: Path) -> None:
    start = source.tell()
    with path.open("wb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as archive:
            archive.write(b"[")
            for index in range(count):
                if index:
                    archive.write(b",")
                archive.write(source.readline().removesuffix(b"\n"))
            archive.write(b"]")
    if path.stat().st_size <= MAX_SHARD_BYTES:
        return
    if count <= 1:
        raise ValueError(f"single row in {path.name} exceeds {MAX_SHARD_BYTES} compressed bytes")

    path.unlink()
    midpoint = count // 2
    # Binary suffixes preserve row order when assembly sorts the filenames.
    stem = path.name.removesuffix(".json.gz")
    source.seek(start)
    write_shard(source, midpoint, path.with_name(f"{stem}-0.json.gz"))
    write_shard(source, count - midpoint, path.with_name(f"{stem}-1.json.gz"))


def pack(source: Path, shard_dir: Path, timestamp_field: str) -> None:
    # Assembly needs no parser dependency.
    import ijson

    shard_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=shard_dir.parent) as temporary:
        work_dir = Path(temporary)
        output_dir = work_dir / "shards"
        output_dir.mkdir()
        counts = {}
        with source.open("rb") as incoming:
            # Keep full-width integer IDs while matching json.loads float values.
            events = (
                (prefix, event, float(value) if isinstance(value, Decimal) else value)
                for prefix, event, value in ijson.parse(incoming)
            )
            first = next(events)
            if first != ("", "start_array", None):
                raise ValueError("expected a JSON array of rows")
            for row in ijson.items(chain([first], events), "item"):
                day = datetime.fromtimestamp(row[timestamp_field], timezone.utc).strftime("%Y-%m-%d")
                # Spool interleaved days to disk instead of retaining all rows.
                with (work_dir / day).open("ab") as daily:
                    daily.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode())
                    daily.write(b"\n")
                counts[day] = counts.get(day, 0) + 1

        for day, count in counts.items():
            daily_path = work_dir / day
            with daily_path.open("rb") as daily:
                write_shard(daily, count, output_dir / f"{day}.json.gz")
            daily_path.unlink()

        if shard_dir.exists():
            shutil.rmtree(shard_dir)
        shutil.copytree(output_dir, shard_dir)


def assemble(shard_dir: Path, output: Path, since_days: int | None = None) -> None:
    cutoff_day = None
    if since_days is not None:
        cutoff_day = (datetime.now(timezone.utc) - timedelta(days=since_days)).strftime("%Y-%m-%d")

    shards = sorted(shard_dir.glob("*.json.gz"))
    if not shards:
        raise ValueError(f"no shards found in {shard_dir}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as assembled:
        assembled.write(b"[")
        first = True
        for shard in shards:
            if cutoff_day is not None and shard.name[:10] < cutoff_day:
                continue
            with gzip.open(shard, "rb") as archive:
                content = archive.read().strip()
            if content == b"[]":
                continue
            if not first:
                assembled.write(b",")
            assembled.write(content[1:-1])
            first = False
        assembled.write(b"]")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    pack_parser = commands.add_parser("pack")
    pack_parser.add_argument("source", type=Path)
    pack_parser.add_argument("shard_dir", type=Path)
    pack_parser.add_argument("timestamp_field")
    assemble_parser = commands.add_parser("assemble")
    assemble_parser.add_argument("shard_dir", type=Path)
    assemble_parser.add_argument("output", type=Path)
    assemble_parser.add_argument("--since-days", type=int)
    args = parser.parse_args()

    if args.command == "pack":
        pack(args.source, args.shard_dir, args.timestamp_field)
    else:
        assemble(args.shard_dir, args.output, args.since_days)


if __name__ == "__main__":
    main()
