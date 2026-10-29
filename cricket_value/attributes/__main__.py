"""Fetch player attributes, then stage them: ``python -m cricket_value.attributes``.

Players are everyone in a silver match with an ESPNcricinfo key in the Cricsheet
register, so run ``make ingest`` and ``make silver`` first. ``--stage-only`` rebuilds
silver ``player_attributes`` from the stored snapshots without touching the network.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import httpx
import pandas as pd

from cricket_value.attributes.fetch import AttributeFetcher
from cricket_value.attributes.silver import build_player_attributes
from cricket_value.ingest.manifest import Manifest

REPO = Path(__file__).resolve().parents[2]
# Wikidata's policy asks for a User-Agent that identifies the client and how to reach it
USER_AGENT = "cricket-value/0.1 (https://github.com/SKarthikeyan28/IPL_squad_evaluator)"


def players_to_fetch(silver_dir: Path) -> list[str]:
    in_matches = set(
        pd.read_parquet(silver_dir / "match_players", columns=["player_id"])["player_id"]
    )
    register = pd.read_parquet(silver_dir / "players", columns=["player_id", "key_cricinfo"])
    linked = register[register["player_id"].isin(in_matches)]["key_cricinfo"].dropna()
    return sorted(set(linked))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=REPO / "data" / "raw")
    parser.add_argument("--silver-dir", type=Path, default=REPO / "data" / "silver")
    parser.add_argument("--stage-only", action="store_true", help="don't fetch, only stage")
    args = parser.parse_args()

    manifest = Manifest(args.raw_dir / "manifest")
    if not args.stage_only:
        ids = players_to_fetch(args.silver_dir)
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=120) as client:
            fetcher = AttributeFetcher(client=client, raw_dir=args.raw_dir, manifest=manifest)
            for r in fetcher.fetch(ids):
                print(f"{r.source:<18} {r.status.value:<10} {r.path or ''}")
        print(f"players asked about: {len(ids):,}")
    rows = build_player_attributes(args.raw_dir, manifest, args.silver_dir)
    print(f"player_attributes    {rows:>10,} rows")


if __name__ == "__main__":
    main()
