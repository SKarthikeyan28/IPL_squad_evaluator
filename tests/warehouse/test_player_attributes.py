"""Player attributes in the warehouse: overrides, nationality and SCD Type 2 history."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from tests.warehouse.conftest import build_warehouse, query

KOHLI, DHONI = "ba607b88", "4a8a2e3b"


def test_sources_and_overrides_fill_dim_player(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    players = query(db, "select * from dim_player").set_index("player_id")

    kohli, dhoni = players.loc[KOHLI], players.loc[DHONI]
    assert (kohli["batting_hand"], kohli["bowling_kind"], kohli["bowling_arm"]) == (
        "right",
        "pace",
        "right",
    )
    assert (kohli["nationality"], kohli["nationality_source"]) == (
        "India",
        "wikidata_sport_country",
    )
    assert str(kohli["date_of_birth"].date()) == "1988-11-05"
    # DBpedia has no infobox fields for Dhoni's page: every one comes from the overrides CSV
    assert (dhoni["batting_hand"], dhoni["bowling_kind"], dhoni["is_keeper"]) == (
        "right",
        "pace",
        True,
    )
    history = query(db, f"select * from dim_player_history where player_id = '{DHONI}'")
    assert history["override_fields"].item() == "batting_hand|bowling_arm|bowling_kind|is_keeper"
    # a player with no attribute source still has a row, with nulls
    assert players["batting_hand"].isna().sum() > 0


@pytest.fixture(scope="module")
def two_snapshots(warehouse: tuple[Path, Path], tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The fixture warehouse rebuilt after a second snapshot a week later, in which
    Kohli's bowling changed and Dhoni's attributes did not."""
    silver, _ = warehouse
    root = tmp_path_factory.mktemp("wh_history")
    shutil.copytree(silver, root / "silver")
    path = root / "silver" / "player_attributes" / "part-0.parquet"
    first = pd.read_parquet(path)
    second = first.copy()
    second["retrieved_at"] = second["retrieved_at"] + pd.Timedelta(days=7)
    kohli = second["player_id"] == KOHLI
    second.loc[kohli, ["bowling_style_raw", "bowling_kind"]] = ["Right-arm off break", "spin"]
    pd.concat([first, second]).to_parquet(path, index=False)
    return build_warehouse(root / "silver", root)


def test_a_change_opens_a_new_version_and_no_change_does_not(two_snapshots: Path) -> None:
    history = query(two_snapshots, "select * from dim_player_history order by valid_from")
    kohli = history[history["player_id"] == KOHLI]
    dhoni = history[history["player_id"] == DHONI]

    assert list(kohli["bowling_kind"]) == ["pace", "spin"]
    assert kohli["valid_to"].iloc[0] == kohli["valid_from"].iloc[1]
    assert list(kohli["is_current"]) == [False, True]
    assert len(dhoni) == 1  # an unchanged snapshot is folded into the version before it
    assert dhoni["valid_to"].isna().all()


def test_dim_player_shows_the_current_version(two_snapshots: Path) -> None:
    players = query(two_snapshots, "select * from dim_player").set_index("player_id")

    assert players.loc[KOHLI, "bowling_kind"] == "spin"
