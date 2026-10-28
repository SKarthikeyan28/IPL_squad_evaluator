from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from dbt.artifacts.schemas.run import RunExecutionResult

from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.silver import build_registers, build_silver
from cricket_value.warehouse.dbt import run_dbt
from tests.rebuild.test_rebuild import stage_root

COMPETITIONS = {"cricsheet_ipl": "ipl", "cricsheet_t20i_male": "t20i"}

# 15 fixture matches can't meet the real data's sample-size floors for estimating priors
FIXTURE_VARS = {
    "rating_min_prior_balls": 1,
    "rating_min_prior_players": 2,
    "rating_min_role_balls": 10,
}


def build_fixture_silver(root: Path) -> Path:
    paths = stage_root(root)
    manifest = Manifest(paths.raw / "manifest")
    build_silver(paths.raw, manifest, paths.silver, COMPETITIONS)
    build_registers(paths.raw, manifest, paths.silver)
    return paths.silver


def build_warehouse(silver: Path, root: Path) -> Path:
    db = root / "warehouse.duckdb"
    result = run_dbt(
        ["build", "--vars", json.dumps(FIXTURE_VARS)],
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
