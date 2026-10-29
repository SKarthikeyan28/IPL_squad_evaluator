"""Point-in-time player ratings (player_ratings_asof and the models behind it)."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from cricket_value.warehouse.dbt import REPO
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
def falsified_warehouse(split_silver: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("wh_falsified")
    shutil.copytree(split_silver, root / "silver")
    falsify_matches_from(root / "silver", CUTOFF)
    return build_warehouse(root / "silver", root)


def ratings(db: Path) -> pd.DataFrame:
    df = query(db, "select * from player_ratings_asof order by player_id, match_date")
    return df.reset_index(drop=True)


def test_no_rating_uses_a_match_on_or_after_its_date(
    split_warehouse: Path, falsified_warehouse: Path
) -> None:
    """Changing every ball from CUTOFF onwards leaves every rating up to CUTOFF unchanged,
    the pace and spin splits included."""
    before, after = ratings(split_warehouse), ratings(falsified_warehouse)
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


def test_the_wide_mart_has_pace_and_spin_batting_columns(split_warehouse: Path) -> None:
    columns = set(ratings(split_warehouse).columns)

    for phase in ("powerplay", "middle", "death"):
        for kind in ("pace", "spin"):
            assert f"bat_{phase}_vs_{kind}_runs_per_ball" in columns
            assert f"bat_{phase}_vs_{kind}_dismissal_rate" in columns
            assert f"bat_{phase}_vs_{kind}_balls" in columns
    assert not any(c.startswith("bowl_") and "_vs_" in c for c in columns)


def test_pace_and_spin_balls_partition_the_balls_faced(split_warehouse: Path) -> None:
    """With every bowler labelled, balls against pace plus balls against spin are the balls
    faced, for every batter, date, phase and stat."""
    parts = query(
        split_warehouse,
        """
        select player_id, match_date, phase, stat,
            sum(balls) filter (where vs = 'all') as faced,
            sum(balls) filter (where vs = 'pace') + sum(balls) filter (where vs = 'spin') as split
        from int_player_ratings_long
        where skill = 'bat'
        group by all
        """,
    )

    assert len(parts) > 0 and (parts["faced"] > 0).any()
    assert np.allclose(parts["faced"], parts["split"])


def test_balls_against_an_unlabelled_bowler_stay_out_of_the_split(
    warehouse: tuple[Path, Path],
) -> None:
    """The plain fixture labels 2 players, so most balls face a bowler of unknown kind: the
    split series hold fewer balls than the unsplit one, never more."""
    _, db = warehouse
    parts = query(
        db,
        """
        select sum(balls) filter (where vs = 'all') as faced,
            sum(balls) filter (where vs in ('pace', 'spin')) as split
        from int_player_ratings_long
        where skill = 'bat' and stat = 'runs_per_ball'
        group by player_id, match_date, phase
        """,
    )

    assert (parts["split"] <= parts["faced"] + 1e-9).all()
    assert (parts["split"] < parts["faced"] - 1e-9).any()


def test_role_counts_each_ball_once_however_many_splits_there_are(split_warehouse: Path) -> None:
    """bowl_share is bowled / (faced + bowled). Counting the pace and spin series as well
    as the unsplit one would triple the balls faced."""
    df = ratings(split_warehouse)
    faced = df[[f"bat_{p}_balls" for p in ("powerplay", "middle", "death")]].sum(axis=1)
    bowled = df[[f"bowl_{p}_balls" for p in ("powerplay", "middle", "death")]].sum(axis=1)
    known = (faced + bowled) > 0

    assert known.any()
    assert np.allclose(df.loc[known, "bowl_share"], (bowled / (faced + bowled))[known])


SPLIT_SQL = """
    with l as (select * from int_player_ratings_long),
    overall as (
        select player_id, match_date, phase, stat, rating as overall_rating,
            series_prior_mean as overall_mean
        from l where skill = 'bat' and vs = 'all'
    )
    select s.vs, s.balls, s.raw_rate, s.rating, s.prior_mean, s.prior_balls,
        s.series_prior_mean, s.series_prior_balls, o.overall_rating, o.overall_mean
    from l s
    join overall o using (player_id, match_date, phase, stat)
    where s.skill = 'bat' and s.vs in ('pace', 'spin') and s.prior_mean is not null
"""


def dbt_var(name: str) -> object:
    config = yaml.safe_load((REPO / "dbt" / "dbt_project.yml").read_text())
    return config["vars"][name]


def test_a_split_rating_shrinks_toward_the_players_own_overall_rating(
    split_warehouse: Path,
) -> None:
    """A batter's rating against spin is mostly his overall skill plus a small deviation, so
    its prior mean is his overall rating scaled by how the league scores against spin."""
    df = query(split_warehouse, SPLIT_SQL)
    multiplier = float(dbt_var("rating_split_prior_multiplier"))  # type: ignore[arg-type]

    assert len(df) > 0
    scaled = (
        df["overall_rating"]
        * df["series_prior_mean"]
        / df["overall_mean"].where(df["overall_mean"] > 0)
    )
    # a pooled average of 0 makes the scaling 0 / 0: the series prior is used instead
    expected_mean = scaled.fillna(df["series_prior_mean"])
    assert np.allclose(df["prior_mean"], expected_mean)
    assert np.allclose(df["prior_balls"], df["series_prior_balls"] * multiplier)
    lo = np.minimum(df["raw_rate"].fillna(df["prior_mean"]), df["prior_mean"]) - 1e-12
    hi = np.maximum(df["raw_rate"].fillna(df["prior_mean"]), df["prior_mean"]) + 1e-12
    assert df["rating"].between(lo, hi).all()


def test_a_split_with_no_evidence_is_the_overall_rating_scaled(split_warehouse: Path) -> None:
    df = query(split_warehouse, SPLIT_SQL)
    fresh = df[df["balls"] == 0]

    scaled = (
        fresh["overall_rating"]
        * fresh["series_prior_mean"]
        / fresh["overall_mean"].where(fresh["overall_mean"] > 0)
    )
    assert len(fresh) > 0
    assert np.allclose(fresh["rating"], scaled.fillna(fresh["series_prior_mean"]))


def test_the_role_prior_can_be_chosen_instead(
    split_silver: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """rating_split_shrink_to: role is the ablation DECISIONS.md reports against."""
    db = build_warehouse(
        split_silver,
        tmp_path_factory.mktemp("wh_role"),
        {"rating_split_shrink_to": "role"},
    )

    df = query(db, SPLIT_SQL)

    assert len(df) > 0
    assert np.allclose(df["prior_mean"], df["series_prior_mean"])
    assert np.allclose(df["prior_balls"], df["series_prior_balls"])


MISSING_SPLIT_PRIORS_SQL = """
    with years as (
        select distinct prior_year from int_rating_priors where vs = 'all' and role = 'any'
    ),
    evidence as (
        select y.prior_year, e.skill, e.phase, e.stat, e.vs, e.player_id
        from int_rating_evidence e
        cross join years y
        where e.skill = 'bat' and e.vs in ('pace', 'spin') and e.n > 0
          and e.match_date < make_date(y.prior_year, 1, 1)
    ),
    pooled_mean_exists as (
        select prior_year, skill, phase, stat, vs
        from evidence
        group by all
        having count(distinct player_id) >= 2
    )
    select m.*
    from pooled_mean_exists m
    join int_rating_priors a
        on a.vs = 'all' and a.role = 'any' and a.prior_year = m.prior_year
        and a.skill = m.skill and a.phase = m.phase and a.stat = m.stat
    left join int_rating_priors s
        on s.vs = m.vs and s.role = 'any' and s.prior_year = m.prior_year
        and s.skill = m.skill and s.phase = m.phase and s.stat = m.stat
    where s.prior_year is null
"""


def test_a_sparse_split_series_borrows_the_unsplit_prior_strength(
    split_silver: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Spin at the death is too rare for 20 batters to have 60 balls against it, so the
    series could not estimate its own prior strength and got no ratings at all. With a
    floor that the unsplit series meets and a split one does not, it must borrow."""
    db = build_warehouse(
        split_silver, tmp_path_factory.mktemp("wh_sparse"), {"rating_min_prior_balls": 8}
    )

    missing = query(db, MISSING_SPLIT_PRIORS_SQL)

    assert missing.empty, missing.head().to_string()
    sparse = query(db, "select count(*) as n from int_rating_priors where vs = 'spin'")
    unsplit = query(
        db, "select count(*) as n from int_rating_priors where vs = 'all' and skill = 'bat'"
    )
    assert sparse["n"].iloc[0] > 0 and unsplit["n"].iloc[0] > 0
