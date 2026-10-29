from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from cricket_value.attributes.fetch import DBPEDIA, Binding
from cricket_value.attributes.silver import build_player_attributes
from cricket_value.ingest.manifest import Manifest
from tests.attributes.test_fetch import BUMRAH, KOHLI, Endpoints, fetcher


def lit(value: str) -> dict[str, str]:
    return {"type": "literal", "value": value}


def dbp(title: str, prop: str, value: str) -> Binding:
    return {
        "s": {"type": "uri", "value": f"http://dbpedia.org/resource/{title}"},
        "p": {"type": "uri", "value": f"http://dbpedia.org/property/{prop}"},
        "o": lit(value),
    }


KOHLI_DB = [
    dbp("Virat_Kohli", "batting", "Right-handed"),
    dbp("Virat_Kohli", "bowling", "Right-arm medium"),
    dbp("Virat_Kohli", "role", "http://dbpedia.org/resource/Batting_order_(cricket)"),
]
BUMRAH_DB = [
    dbp("Jasprit_Bumrah", "batting", "Right-handed"),
    dbp("Jasprit_Bumrah", "bowling", "Right-arm fast"),
]


def write_register(silver: Path) -> None:
    players = pd.DataFrame(
        {
            "player_id": ["ba607b88", "4a8a2e3b", "nolinks1"],
            "key_cricinfo": ["253802", "625383", None],
        }
    )
    (silver / "players").mkdir(parents=True)
    players.to_parquet(silver / "players" / "part-0.parquet", index=False)


def build(tmp_path: Path) -> pd.DataFrame:
    silver = tmp_path / "silver"
    if not (silver / "players").exists():
        write_register(silver)
    raw = tmp_path / "raw"
    build_player_attributes(raw, Manifest(raw / "manifest"), silver)
    return pd.read_parquet(silver / "player_attributes").sort_values(["player_id", "retrieved_at"])


def test_one_row_per_linked_player_with_mapped_attributes(tmp_path: Path) -> None:
    kohli: Binding = {**KOHLI, "sport_country": lit("India"), "citizenship": lit("India")}
    fetcher(
        tmp_path, Endpoints([kohli, BUMRAH], KOHLI_DB + BUMRAH_DB), "2026-10-29T08:00:00"
    ).fetch(["253802", "625383"])

    df = build(tmp_path).set_index("player_id")

    assert list(df.index) == ["4a8a2e3b", "ba607b88"]  # the player with no cricinfo key is absent
    k = df.loc["ba607b88"]
    assert k["wikidata_id"] == "Q213854"
    assert k["wikipedia_title"] == "Virat_Kohli"
    assert k["date_of_birth"] == date(1988, 11, 5)
    assert (k["sport_country"], k["citizenship"]) == ("India", "India")
    assert (k["batting_hand"], k["bowling_kind"], k["bowling_arm"]) == ("right", "pace", "right")
    assert k["bowling_style_raw"] == "Right-arm medium"
    assert not k["is_keeper"]
    assert str(k["retrieved_at"]) == "2026-10-29 08:00:00"
    assert pd.isna(df.loc["4a8a2e3b", "date_of_birth"])


def test_each_snapshot_combines_the_latest_answer_of_each_source(tmp_path: Path) -> None:
    fetcher(tmp_path, Endpoints([KOHLI], KOHLI_DB), "2026-10-29T08:00:00").fetch(["253802"])
    moved: Binding = {**KOHLI, "sport_country": lit("India")}
    fetcher(tmp_path, Endpoints([moved], KOHLI_DB), "2026-11-05T08:00:00").fetch(["253802"])
    archives = Manifest(tmp_path / "raw" / "manifest").archives()
    assert archives["source"].value_counts()[DBPEDIA.name] == 1  # DBpedia unchanged

    df = build(tmp_path)

    assert len(df) == 2
    assert list(df["sport_country"].fillna("-")) == ["-", "India"]
    assert list(df["bowling_kind"]) == ["pace", "pace"]  # DBpedia's only snapshot, used twice


def test_conflicting_values_become_null_and_keepers_are_flagged(tmp_path: Path) -> None:
    twice_born: list[Binding] = [
        {**KOHLI, "dob": lit("1988-11-05T00:00:00Z")},
        {**KOHLI, "dob": lit("1989-11-05T00:00:00Z")},
    ]
    keeper = [dbp("Virat_Kohli", "role", "http://dbpedia.org/resource/Wicket-keeper-batter")]
    fetcher(tmp_path, Endpoints(twice_born, keeper), "2026-10-29T08:00:00").fetch(["253802"])

    (row,) = build(tmp_path).itertuples()

    assert pd.isna(row.date_of_birth)
    assert row.is_keeper


def test_no_snapshots_writes_an_empty_table(tmp_path: Path) -> None:
    df = build(tmp_path)

    assert df.empty
    assert {"player_id", "retrieved_at", "bowling_kind"} <= set(df.columns)


def test_an_altered_raw_snapshot_is_refused(tmp_path: Path) -> None:
    fetcher(tmp_path, Endpoints([KOHLI], KOHLI_DB), "2026-10-29T08:00:00").fetch(["253802"])
    path = tmp_path / "raw" / DBPEDIA.name / "20261029T080000Z.json"
    path.write_text(path.read_text().replace("medium", "fast"))

    with pytest.raises(ValueError, match="checksum"):
        build(tmp_path)


def test_wikipedia_fills_only_the_gaps_and_says_which(tmp_path: Path) -> None:
    # DBpedia has Kohli's batting but no bowling; Wikidata has two dates of birth and no
    # country. Wikipedia's infobox fills those three, and leaves the batting alone.
    twice_born: list[Binding] = [
        {**KOHLI, "dob": lit("1988-11-05T00:00:00Z")},
        {**KOHLI, "dob": lit("1989-11-05T00:00:00Z")},
    ]
    infobox = (
        "{{Infobox cricketer\n| country = India\n"
        "| birth_date = {{Birth date and age|1988|11|5|df=yes}}\n"
        "| batting = Left-handed\n| bowling = Right-arm [[Medium pace|medium]]\n}}"
    )
    endpoints = Endpoints(
        twice_born,
        [dbp("Virat_Kohli", "batting", "Right-handed")],
        wikipedia={"Virat Kohli": infobox},
    )
    fetcher(tmp_path, endpoints, "2026-10-29T08:00:00").fetch(["253802"])

    (row,) = build(tmp_path).itertuples()

    assert row.batting_hand == "right"  # DBpedia's, not Wikipedia's
    assert (row.bowling_kind, row.bowling_arm) == ("pace", "right")
    assert row.bowling_style_raw == "Right-arm medium"
    assert row.date_of_birth == date(1988, 11, 5)
    assert row.wikipedia_country == "India"
    assert row.wikipedia_fields == "bowling|date_of_birth"
