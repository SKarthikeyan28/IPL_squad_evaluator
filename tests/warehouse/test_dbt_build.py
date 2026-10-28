from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.silver import build_registers, build_silver
from cricket_value.reference.home import match_home_status
from cricket_value.reference.reference import load_reference
from cricket_value.warehouse.dbt import REPO, run_dbt
from tests.rebuild.test_rebuild import stage_root

COMPETITIONS = {"cricsheet_ipl": "ipl", "cricsheet_t20i_male": "t20i"}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """Silver built from the 15 fixture matches, then `dbt build` into a fresh warehouse."""
    root = tmp_path_factory.mktemp("wh")
    paths = stage_root(root)
    manifest = Manifest(paths.raw / "manifest")
    build_silver(paths.raw, manifest, paths.silver, COMPETITIONS)
    build_registers(paths.raw, manifest, paths.silver)

    db = root / "warehouse.duckdb"
    result = run_dbt(["build"], silver_dir=paths.silver, warehouse=db, work_dir=root / "dbt")
    assert result.success, result.exception
    return paths.silver, db


def query(db: Path, sql: str) -> pd.DataFrame:
    # Same (default) settings as dbt's connection: DuckDB refuses a second connection to
    # one file in the same process with a different configuration, such as read_only
    with duckdb.connect(str(db)) as con:
        out: pd.DataFrame = con.sql(sql).df()
    return out


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
