#!/usr/bin/env python3
"""Fetch and checksum the pinned MusicBrainz schema sources."""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    lock = json.loads((ROOT / "source.lock.json").read_text())
    revision = lock["revision"]
    entries = [*lock["source_files"], lock["license"]]
    for entry in entries:
        destination = args.output / entry["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        url = (
            "https://raw.githubusercontent.com/metabrainz/musicbrainz-server/"
            f"{revision}/{entry['path']}"
        )
        with urllib.request.urlopen(url, timeout=60) as response:
            destination.write_bytes(response.read())
        actual = sha256_file(destination)
        if actual != entry["sha256"]:
            destination.unlink()
            raise SystemExit(f"checksum mismatch for {entry['path']}: {actual}")
        print(f"[source] verified {entry['path']} {actual}")


if __name__ == "__main__":
    main()
