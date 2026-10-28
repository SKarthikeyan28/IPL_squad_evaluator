from __future__ import annotations

from pathlib import Path

import pandas as pd

from cricket_value.reference.home import match_home_status
from cricket_value.reference.reference import load_reference
from cricket_value.warehouse.dbt import REPO
from tests.warehouse.conftest import query


def test_dbt_build_passes_every_test_and_contract(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse

    tables = set(query(db, "select table_name from information_schema.tables")["table_name"])

    assert {
        "fact_delivery",
        "fact_team_match",
        "dim_player",
        "dim_team",
        "dim_venue",
        "dim_season",
    } <= tables


def test_fact_delivery_has_one_row_per_silver_ball(warehouse: tuple[Path, Path]) -> None:
    silver, db = warehouse

    assert len(query(db, "select * from fact_delivery")) == len(
        pd.read_parquet(silver / "deliveries")
    )


def test_dbt_home_status_matches_the_python_rule(warehouse: tuple[Path, Path]) -> None:
    """The rule exists in SQL twice until Python callers read the warehouse: they must agree."""
    silver, db = warehouse
    matches = pd.read_parquet(silver / "matches")
    matches["season"] = matches["season"].astype("int64")

    python = match_home_status(matches, load_reference(REPO / "config" / "reference"))
    dbt = query(db, "select match_id, team, home_status from int_match_home_status")

    key = ["match_id", "team"]
    merged = python[[*key, "home_status"]].merge(dbt, on=key, suffixes=("_py", "_dbt"))
    assert len(merged) == len(python) == len(dbt) > 0
    assert (merged["home_status_py"] == merged["home_status_dbt"]).all()


def test_impact_player_era_and_phases(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse

    seasons = query(db, "select season from dim_season where impact_player_rule order by 1")
    phases = set(query(db, "select distinct phase from fact_delivery")["phase"])

    assert list(seasons["season"]) == [2023]  # the only fixture IPL season under the rule
    assert phases == {"powerplay", "middle", "death", "super_over"}
