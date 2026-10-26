from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest

from cricket_value.orchestration.assets import SILVER_KEYS, match_index, silver_season
from cricket_value.orchestration.definitions import defs
from cricket_value.orchestration.partitions import season_partitions
from cricket_value.orchestration.resources import REPO_ROOT, ProjectPaths
from tests.ingest.test_silver import FIXTURES, fixture_members, stage_archive


def test_definitions_load() -> None:
    dg.Definitions.validate_loadable(defs)


def test_season_partitions_run_from_2005_to_this_year() -> None:
    keys = season_partitions.get_partition_keys()

    assert keys[0] == "2005"
    assert keys[-1] == str(dt.date.today().year)


def test_lineage_runs_raw_to_index_to_silver_to_report() -> None:
    graph = defs.resolve_asset_graph()
    matches = SILVER_KEYS["matches"]

    assert dg.AssetKey(["silver", "match_index"]) in graph.get(matches).parent_keys
    assert (
        dg.AssetKey(["raw", "cricsheet_ipl"])
        in graph.get(dg.AssetKey(["silver", "match_index"])).parent_keys
    )
    assert matches in graph.get(dg.AssetKey(["reports", "data_quality"])).parent_keys


@pytest.fixture
def paths(tmp_path: Path) -> ProjectPaths:
    raw = tmp_path / "data" / "raw"
    doc = json.loads((FIXTURES / "ipl" / "1082591.json").read_text())
    doc["info"]["toss"]["winner"] = "Somebody Else"  # breaks a contract: quarantined
    ipl = {**fixture_members("cricsheet_ipl"), "1082591.json": json.dumps(doc).encode()}
    stage_archive(raw, "cricsheet_ipl", ipl, "2026-10-26T07:00:00Z")
    stage_archive(
        raw, "cricsheet_t20i_male", fixture_members("cricsheet_t20i_male"), "2026-10-26T07:00:00Z"
    )
    return ProjectPaths(root=str(tmp_path), config_root=str(REPO_ROOT / "config"))


def checks(result: dg.ExecuteInProcessResult) -> dict[tuple[str, str], bool]:
    return {
        (e.asset_key.path[-1], e.check_name): e.passed for e in result.get_asset_check_evaluations()
    }


def test_one_season_partition_materialises_with_checks(paths: ProjectPaths) -> None:
    assert dg.materialize([match_index], resources={"paths": paths}).success

    result = dg.materialize([silver_season], partition_key="2017", resources={"paths": paths})

    assert result.success
    matches = pd.read_parquet(paths.silver_dir / "matches")
    assert set(matches["season"].astype(int)) == {2017}  # only this partition was built
    assert set(matches["match_id"]) == {"1082625", "1082648"}
    c = checks(result)
    assert c[("matches", "contracts")] is False  # 1082591 broke a rule and was quarantined
    assert c[("deliveries", "contracts")] is True
    assert c[("matches", "reconciliation")] is True


def test_reconciliation_check_fails_on_real_flags(paths: ProjectPaths) -> None:
    dg.materialize([match_index], resources={"paths": paths})

    result = dg.materialize([silver_season], partition_key="2024", resources={"paths": paths})

    assert result.success  # a WARN check never fails the run
    assert checks(result)[("matches", "reconciliation")] is False  # Serbia fielded 10


def test_a_partition_range_runs_as_one_run(paths: ProjectPaths) -> None:
    dg.materialize([match_index], resources={"paths": paths})

    result = dg.materialize(
        [silver_season],
        # the run tags a UI or CLI backfill sets for a partition range
        tags={
            "dagster/asset_partition_range_start": "2017",
            "dagster/asset_partition_range_end": "2019",
        },
        resources={"paths": paths},
    )

    assert result.success
    seasons = set(pd.read_parquet(paths.silver_dir / "matches")["season"].astype(int))
    assert seasons == {2017, 2018, 2019}
