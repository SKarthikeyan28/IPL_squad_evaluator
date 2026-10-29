from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from dbt.artifacts.schemas.run import RunExecutionResult

from cricket_value.attributes.silver import PLAYER_ATTRIBUTES, build_player_attributes
from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.silver import _write_file, build_registers, build_silver
from cricket_value.warehouse.dbt import run_dbt
from tests.rebuild.test_rebuild import stage_root

COMPETITIONS = {"cricsheet_ipl": "ipl", "cricsheet_t20i_male": "t20i"}

# 15 fixture matches can't meet the real data's sample-size floors for estimating priors
FIXTURE_VARS = {
    "rating_min_prior_balls": 1,
    "rating_min_prior_players": 2,
    "rating_min_role_balls": 10,
    # the 15-match fixture holds 2 of the override CSV's players
    "overrides_require_known_players": False,
}


def build_fixture_silver(root: Path) -> Path:
    paths = stage_root(root)
    manifest = Manifest(paths.raw / "manifest")
    build_silver(paths.raw, manifest, paths.silver, COMPETITIONS)
    build_registers(paths.raw, manifest, paths.silver)
    build_player_attributes(paths.raw, manifest, paths.silver)
    return paths.silver


def build_warehouse(silver: Path, root: Path, extra_vars: dict[str, object] | None = None) -> Path:
    db = root / "warehouse.duckdb"
    result = run_dbt(
        ["build", "--vars", json.dumps({**FIXTURE_VARS, **(extra_vars or {})})],
        silver_dir=silver,
        warehouse=db,
        work_dir=root / "dbt",
    )
    run = result.result
    failed = (
        [f"{r.node.name}: {r.message}" for r in run if str(r.status) in ("error", "fail")]
        if isinstance(run, RunExecutionResult)
        else []
    )
    assert result.success, result.exception or failed
    return db


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """Silver built from the 15 fixture matches, then `dbt build` into a fresh warehouse."""
    root = tmp_path_factory.mktemp("wh")
    silver = build_fixture_silver(root)
    return silver, build_warehouse(silver, root)


def query(db: Path, sql: str) -> pd.DataFrame:
    # Same (default) settings as dbt's connection: DuckDB refuses a second connection to
    # one file in the same process with a different configuration, such as read_only
    with duckdb.connect(str(db)) as con:
        out: pd.DataFrame = con.sql(sql).df()
    return out


def label_every_player_with_a_bowling_kind(silver: Path) -> None:
    """Structural test data, not results: the fixture has bowling styles for 2 players, so
    give everyone a kind (alternating pace and spin by id order) to fill the pace/spin
    series. Every ball then faces a bowler of known kind."""
    players = sorted(pd.read_parquet(silver / "match_players")["player_id"].unique())
    given = {
        "player_id": players,
        "retrieved_at": [datetime(2026, 10, 29, 8)] * len(players),
        "bowling_kind": ["pace" if i % 2 == 0 else "spin" for i in range(len(players))],
        "is_keeper": [False] * len(players),
    }
    table = pa.Table.from_pydict(
        {f.name: given.get(f.name, [None] * len(players)) for f in PLAYER_ATTRIBUTES},
        schema=PLAYER_ATTRIBUTES,
    )
    _write_file(silver / "player_attributes" / "part-0.parquet", table)


@pytest.fixture(scope="session")
def split_silver(warehouse: tuple[Path, Path], tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Silver of the fixture warehouse with every player labelled pace or spin."""
    root = tmp_path_factory.mktemp("split_silver")
    shutil.copytree(warehouse[0], root / "silver")
    label_every_player_with_a_bowling_kind(root / "silver")
    return root / "silver"


@pytest.fixture(scope="session")
def split_warehouse(split_silver: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_warehouse(split_silver, tmp_path_factory.mktemp("wh_split"))
