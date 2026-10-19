#!/usr/bin/env python3
"""Download pinned MusicBrainz real-data artifacts with verified HTTP ranges."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import threading
import time
import urllib.request
from pathlib import Path

COPY_BUFFER = 1024 * 1024
ROOT = Path(__file__).resolve().parent


def find_entry(manifest: dict, name: str) -> tuple[str, dict]:
    matches = [
        (dataset["source"], entry)
        for dataset in manifest["datasets"].values()
        for entry in dataset["files"]
        if entry["name"] == name
    ]
    if not matches:
        raise KeyError(name)
    if len(matches) != 1:
        raise RuntimeError(f"ambiguous real-data artifact name: {name}")
    return matches[0]


def download_range(
    *,
    url: str,
    fd: int,
    start: int,
    end: int,
    progress: list[int],
    lock: threading.Lock,
) -> None:
    request = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
    with urllib.request.urlopen(request, timeout=120) as response:
        if response.status != 206:
            raise RuntimeError(f"server ignored byte range {start}-{end}: HTTP {response.status}")
        offset = start
        while offset <= end:
            chunk = response.read(min(COPY_BUFFER, end - offset + 1))
            if not chunk:
                break
            os.pwrite(fd, chunk, offset)
            offset += len(chunk)
            with lock:
                progress[0] += len(chunk)
        if offset != end + 1:
            raise RuntimeError(f"short range {start}-{end}: stopped at {offset}")


def verify_sha256(path: Path, expected: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * COPY_BUFFER), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected:
        raise RuntimeError(f"checksum mismatch for {path}: expected {expected}, got {actual}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--connections",
        type=int,
        default=1,
        help="parallel ranges; the official server may throttle values above one",
    )
    args = parser.parse_args()
    if args.connections < 1:
        parser.error("--connections must be positive")

    manifest = json.loads((ROOT / "real_data.lock.json").read_text())
    try:
        source, entry = find_entry(manifest, args.name)
    except KeyError:
        parser.error(f"unknown artifact: {args.name}")
    destination = args.output_dir / args.name
    partial = destination.with_suffix(destination.suffix + ".partial")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file():
        verify_sha256(destination, entry["sha256"])
        print(f"[dump] already verified path={destination}")
        return

    total = int(entry["bytes"])
    url = source + entry["name"]
    fd = os.open(partial, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
    os.ftruncate(fd, total)
    progress = [0]
    progress_lock = threading.Lock()
    chunk_size = (total + args.connections - 1) // args.connections
    started = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.connections) as executor:
            futures = []
            for index in range(args.connections):
                start = index * chunk_size
                end = min(total - 1, start + chunk_size - 1)
                if start >= total:
                    break
                futures.append(
                    executor.submit(
                        download_range,
                        url=url,
                        fd=fd,
                        start=start,
                        end=end,
                        progress=progress,
                        lock=progress_lock,
                    )
                )
            while any(not future.done() for future in futures):
                time.sleep(10)
                with progress_lock:
                    downloaded = progress[0]
                elapsed = max(time.monotonic() - started, 0.001)
                print(
                    f"[dump] name={entry['name']} downloaded={downloaded}/{total} "
                    f"percent={downloaded / total:.1%} speed_mib_s={downloaded / elapsed / 1048576:.2f}",
                    flush=True,
                )
            for future in futures:
                future.result()
    finally:
        os.close(fd)

    verify_sha256(partial, entry["sha256"])
    partial.replace(destination)
    print(f"[dump] verified path={destination} sha256={entry['sha256']}")


if __name__ == "__main__":
    main()
