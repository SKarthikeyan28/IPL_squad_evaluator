"""Fetch every configured raw source: ``python -m cricket_value.ingest [--verify]``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import httpx

from cricket_value.ingest.download import Downloader, load_sources
from cricket_value.ingest.manifest import Manifest

REPO = Path(__file__).resolve().parents[2]
USER_AGENT = "cricket-value/0.1 (portfolio data project; polite incremental fetches)"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=REPO / "data" / "raw")
    parser.add_argument("--sources", type=Path, default=REPO / "config" / "sources.toml")
    parser.add_argument("--verify", action="store_true", help="only re-hash stored raw files")
    args = parser.parse_args()

    manifest = Manifest(args.raw_dir / "manifest")
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=120) as client:
        downloader = Downloader(client=client, raw_dir=args.raw_dir, manifest=manifest)
        if not args.verify:
            for source in load_sources(args.sources):
                r = downloader.fetch(source)
                print(
                    f"{r.source:<22} {r.status.value:<13} "
                    f"new={r.new_files:<5} changed={r.changed_files:<5} {r.path or ''}"
                )
        bad = downloader.verify()

    print(f"snapshot_id={manifest.snapshot_id()}")
    if bad:
        print("raw files missing or altered:", *bad, sep="\n  ", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
