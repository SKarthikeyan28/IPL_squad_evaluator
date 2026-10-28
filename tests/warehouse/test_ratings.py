"""Point-in-time player ratings (player_ratings_asof and the models behind it)."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.warehouse.conftest import build_warehouse, query

CUTOFF = date(2023, 3, 31)  # a fixture IPL match date with fixture matches either side


def falsify_matches_from(silver: Path, cutoff: date) -> None:
    """Rewrite silver so every match on or after ``cutoff`` has different outcomes."""
    matches = pd.read_parquet(silver / "matches")
    late = set(matches.loc[pd.to_datetime(matches["match_date"]).dt.date >= cutoff, "match_id"])
    for part in (silver / "deliveries").glob("*/*.parquet"):
        df = pd.read_parquet(part)
        hit = df["match_id"].isin(late)
        df.loc[hit, "runs_batter"] += 4
        df.loc[hit, "runs_total"] += 4
        df.to_parquet(part, index=False)
    for part in (silver / "wickets").glob("*/*.parquet"):
        df = pd.read_parquet(part)
        df[~df["match_id"].isin(late)].to_parquet(part, index=False)


@pytest.fixture(scope="module")
def falsified_warehouse(
    warehouse: tuple[Path, Path], tmp_path_factory: pytest.TempPathFactory
) -> Path:
    silver, _ = warehouse
    root = tmp_path_factory.mktemp("wh_falsified")
    shutil.copytree(silver, root / "silver")
    falsify_matches_from(root / "silver", CUTOFF)
    return build_warehouse(root / "silver", root)


def ratings(db: Path) -> pd.DataFrame:
    df = query(db, "select * from player_ratings_asof order by player_id, match_date")
    return df.reset_index(drop=True)


def test_no_rating_uses_a_match_on_or_after_its_date(
    warehouse: tuple[Path, Path], falsified_warehouse: Path
) -> None:
    """Changing every ball from CUTOFF onwards leaves every rating up to CUTOFF unchanged."""
    _, db = warehouse
    before, after = ratings(db), ratings(falsified_warehouse)
    early = before["match_date"].dt.date <= CUTOFF
    assert early.sum() > 0

    pd.testing.assert_frame_equal(
        before[early].reset_index(drop=True), after[early].reset_index(drop=True)
    )
    # and the falsification is visible later, so the comparison above isn't vacuous
    late = ~early
    rating_cols = [c for c in before.columns if c.startswith(("bat_", "bowl_"))]
    assert not np.allclose(
        before.loc[late, rating_cols].fillna(-1), after.loc[late, rating_cols].fillna(-1)
    )


def test_one_row_per_player_per_in_scope_match_date(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    expected = query(
        db,
        """
        select distinct mp.player_id, m.match_date
        from stg_silver__match_players mp
        join stg_silver__matches m using (match_id)
        where m.match_id in (
            select match_id from fact_delivery where competition = 'ipl' or is_full_member_t20i
        )
        """,
    )
    got = ratings(db)

    assert len(got) == len(expected) > 0
    assert not got.duplicated(["player_id", "match_date"]).any()


def test_a_player_without_history_gets_the_prior(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    long = query(db, "select * from int_player_ratings_long where prior_mean is not null")
    fresh = long[long["source_match_date"].isna()]
    assert len(fresh) > 0

    assert (fresh["balls"] == 0).all()
    assert np.allclose(fresh["rating"], fresh["prior_mean"])


def test_ratings_lie_between_the_raw_rate_and_the_prior(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    long = query(
        db, "select * from int_player_ratings_long where balls > 0 and prior_mean is not null"
    )
    assert len(long) > 0

    lo = np.minimum(long["raw_rate"], long["prior_mean"]) - 1e-12
    hi = np.maximum(long["raw_rate"], long["prior_mean"]) + 1e-12
    assert long["rating"].between(lo, hi).all()
    # with any evidence, the rating moves off the prior toward the player's own record
    moved = (long["rating"] - long["prior_mean"]).abs() > 0
    assert moved[long["raw_rate"] != long["prior_mean"]].all()
