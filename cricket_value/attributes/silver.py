"""Stage player attributes (silver ``player_attributes``) from every stored raw snapshot.

One row per Cricsheet player per snapshot time. A snapshot time is any ``fetched_at`` of
any source, and each row combines the latest Wikidata, DBpedia and Wikipedia answers at
that time, so a source that didn't change still contributes. Wikipedia's infobox only
fills what the other two left empty, and ``wikipedia_fields`` records which fields it
filled; its country is kept as its own column for dbt's nationality chain. Building from *every*
snapshot, not just the latest, is what lets ``dim_player_history`` (SCD Type 2) be rebuilt
from raw files alone.

Values that disagree within one snapshot (two dates of birth, a pace and a spin style)
become null rather than a guess: the overrides CSV and the attributes audit deal with them.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from cricket_value.attributes.fetch import (
    DBPEDIA,
    DBPEDIA_RESOURCE,
    WIKIDATA,
    WIKIPEDIA,
    article_title,
)
from cricket_value.attributes.infobox import birth_date, infobox_fields
from cricket_value.attributes.styles import batting_hand, combine_bowling, normalise
from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.silver import _write_file

ATTRIBUTES = "player_attributes"
STR = pa.string()
PLAYER_ATTRIBUTES = pa.schema(
    [
        ("player_id", STR),
        ("key_cricinfo", STR),
        ("retrieved_at", pa.timestamp("us")),
        ("wikidata_id", STR),
        ("n_wikidata_items", pa.int32()),
        ("wikipedia_title", STR),
        ("date_of_birth", pa.date32()),
        ("sport_country", STR),  # "|"-joined when Wikidata lists several
        ("citizenship", STR),
        ("batting_hand", STR),
        ("bowling_style_raw", STR),
        ("bowling_kind", STR),
        ("bowling_arm", STR),
        ("is_keeper", pa.bool_()),
        ("wikipedia_country", STR),
        ("wikipedia_fields", STR),  # "|"-joined fields filled from Wikipedia's infobox
    ]
)


def _read(raw_dir: Path, path: str, sha256: str) -> dict[str, Any]:
    content = (raw_dir / path).read_bytes()
    actual = hashlib.sha256(content).hexdigest()
    if actual != sha256:
        raise ValueError(f"checksum mismatch for {path}: file {actual}")
    doc: dict[str, Any] = json.loads(content)
    return doc


def _load(raw_dir: Path, path: str, sha256: str) -> list[dict[str, Any]]:
    bindings = _read(raw_dir, path, sha256)["bindings"]
    return [{k: v["value"] for k, v in b.items()} for b in bindings]


def _wikipedia(raw_dir: Path, path: str, sha256: str) -> dict[str, dict[str, str]]:
    """Requested title -> infobox fields of the page it resolved to."""
    pages = _read(raw_dir, path, sha256)["pages"]
    return {p["requested"]: infobox_fields(p["wikitext"]) for p in pages if p["wikitext"]}


def _joined(values: set[str]) -> str | None:
    return "|".join(sorted(values)) or None


def _single(values: set[str]) -> str | None:
    return next(iter(values)) if len(values) == 1 else None


def _wikidata(rows: list[dict[str, Any]]) -> dict[str, dict[str, set[str]]]:
    """cricinfo id -> field -> set of values."""
    out: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for r in rows:
        fields = out[r["cid"]]
        fields["item"].add(r["item"].rsplit("/", 1)[-1])
        for key in ("sport_country", "citizenship"):
            if key in r:
                fields[key].add(r[key])
        if "dob" in r:
            fields["dob"].add(r["dob"][:10])
        if "article" in r:
            fields["title"].add(article_title(r["article"]))
    return out


def _dbpedia(rows: list[dict[str, Any]]) -> dict[str, dict[str, set[str]]]:
    """Wikipedia title -> infobox property -> set of values."""
    out: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for r in rows:
        out[r["s"].removeprefix(DBPEDIA_RESOURCE)][r["p"].rsplit("/", 1)[-1]].add(r["o"])
    return out


def _rows(
    wd: dict[str, dict[str, set[str]]],
    db: dict[str, dict[str, set[str]]],
    wp: dict[str, dict[str, str]],
    players: dict[str, str],
    retrieved_at: pd.Timestamp,
) -> list[dict[str, Any]]:
    rows = []
    for cid, f in wd.items():
        if cid not in players:
            continue
        title = _single(f["title"])
        info = db.get(title, {}) if title else {}
        box = wp.get(title, {}) if title else {}
        filled: list[str] = []
        bowling = info.get("bowling", set())
        if not bowling and "bowling" in box:
            bowling, filled = {box["bowling"]}, [*filled, "bowling"]
        style = combine_bowling(bowling)
        hand = batting_hand(info.get("batting", set()))
        if hand is None and "batting" in box:
            hand, filled = batting_hand([box["batting"]]), [*filled, "batting_hand"]
        dob_text = _single(f["dob"])
        dob = date.fromisoformat(dob_text) if dob_text else None
        if dob is None and "birth_date" in box:
            dob = birth_date(box["birth_date"])
            filled = [*filled, "date_of_birth"] if dob else filled
        roles = info.get("role", set()) | ({box["role"]} if "role" in box else set())
        rows.append(
            {
                "player_id": players[cid],
                "key_cricinfo": cid,
                "retrieved_at": retrieved_at,
                "wikidata_id": _single(f["item"]),
                "n_wikidata_items": len(f["item"]),
                "wikipedia_title": title,
                "date_of_birth": dob,
                "sport_country": _joined(f["sport_country"]),
                "citizenship": _joined(f["citizenship"]),
                "batting_hand": hand,
                "bowling_style_raw": _joined(bowling),
                "bowling_kind": style.kind,
                "bowling_arm": style.arm,
                "is_keeper": any("wicket" in normalise(r) for r in roles),
                "wikipedia_country": box.get("country"),
                "wikipedia_fields": _joined(set(filled)),
            }
        )
    return rows


def build_player_attributes(raw_dir: Path, manifest: Manifest, silver_dir: Path) -> int:
    """Write ``<silver_dir>/player_attributes/part-0.parquet``; returns the row count."""
    registry = pd.read_parquet(silver_dir / "players", columns=["player_id", "key_cricinfo"])
    registry = registry.dropna()
    players = dict(zip(registry["key_cricinfo"], registry["player_id"], strict=True))

    archives = manifest.archives()
    archives = archives[archives["source"].isin([WIKIDATA.name, DBPEDIA.name, WIKIPEDIA.name])]
    rows: list[dict[str, Any]] = []
    for fetched_at in sorted(archives["fetched_at"].unique()):
        upto = archives[archives["fetched_at"] <= fetched_at].sort_values("fetched_at")
        latest = upto.groupby("source").last()
        if WIKIDATA.name not in latest.index:
            continue
        wd_rec = latest.loc[WIKIDATA.name]
        wd = _wikidata(_load(raw_dir, wd_rec["path"], wd_rec["sha256"]))
        db: dict[str, dict[str, set[str]]] = {}
        if DBPEDIA.name in latest.index:
            db_rec = latest.loc[DBPEDIA.name]
            db = _dbpedia(_load(raw_dir, db_rec["path"], db_rec["sha256"]))
        wp: dict[str, dict[str, str]] = {}
        if WIKIPEDIA.name in latest.index:
            wp_rec = latest.loc[WIKIPEDIA.name]
            wp = _wikipedia(raw_dir, wp_rec["path"], wp_rec["sha256"])
        retrieved_at = pd.Timestamp(fetched_at).tz_localize(None)
        rows += _rows(wd, db, wp, players, retrieved_at)

    df = pd.DataFrame(rows, columns=PLAYER_ATTRIBUTES.names)
    df = df.sort_values(["player_id", "retrieved_at"], ignore_index=True)
    table = pa.Table.from_pandas(df, schema=PLAYER_ATTRIBUTES, preserve_index=False)
    _write_file(silver_dir / ATTRIBUTES / "part-0.parquet", table)
    return len(df)
