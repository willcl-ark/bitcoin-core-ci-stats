#!/usr/bin/env python3
"""Pack JSON rows into deterministic monthly gzip files, or assemble them."""

import argparse
import gzip
import json
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


MAX_SHARD_BYTES = 50 * 1024 * 1024


def pack(source: Path, shard_dir: Path, timestamp_field: str) -> None:
    rows_by_month = {}
    for row in json.loads(source.read_text()):
        month = datetime.fromtimestamp(row[timestamp_field], timezone.utc).strftime("%Y-%m")
        rows_by_month.setdefault(month, []).append(row)

    shard_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=shard_dir.parent) as temporary:
        output_dir = Path(temporary)
        for month, rows in rows_by_month.items():
            path = output_dir / f"{month}.json.gz"
            with path.open("wb") as output:
                with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as archive:
                    archive.write(json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode())
            if path.stat().st_size > MAX_SHARD_BYTES:
                raise ValueError(f"{path.name} exceeds 50 MiB; use smaller time shards")

        if shard_dir.exists():
            shutil.rmtree(shard_dir)
        shutil.copytree(output_dir, shard_dir)


def assemble(shard_dir: Path, output: Path, since_days: int | None = None) -> None:
    cutoff_month = None
    if since_days is not None:
        cutoff_month = (datetime.now(timezone.utc) - timedelta(days=since_days)).strftime("%Y-%m")

    shards = sorted(shard_dir.glob("*.json.gz"))
    if not shards:
        raise ValueError(f"no shards found in {shard_dir}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as assembled:
        assembled.write(b"[")
        first = True
        for shard in shards:
            if cutoff_month is not None and shard.name[:7] < cutoff_month:
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
