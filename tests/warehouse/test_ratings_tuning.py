"""The tuner's warehouse queries, on the fixture warehouse."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from cricket_value.ratings.tuning import (
    HISTORY_SQL,
    league_runs_distribution,
    load_rated_balls,
    match_totals,
    score_balls,
)
from tests.warehouse.conftest import query

SEASONS = range(2017, 2024)


def test_every_evaluation_ball_gets_both_players_ratings(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    balls = load_rated_balls(db, SEASONS)  # raises if any ball went unrated

    faced = query(
        db,
        "select count(*) as n from fact_delivery where competition = 'ipl' "
        "and not is_super_over and extras_wides = 0 and season between 2017 and 2023",
    )
    assert len(balls) == faced["n"].iloc[0] > 0
    assert balls[["bat_dismissal", "bat_runs", "bowl_wicket", "bowl_runs"]].notna().all().all()


def test_fixture_balls_score_to_finite_log_likelihoods(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    balls = load_rated_balls(db, SEASONS)
    league = league_runs_distribution(query(db, HISTORY_SQL.format(last=2023)), SEASONS)

    totals = match_totals(score_balls(balls, league))

    assert totals["balls"].sum() == len(balls)
    assert np.isfinite(totals[["ll_dismissal", "ll_runs"]].to_numpy()).all()
    assert (totals[["ll_dismissal", "ll_runs"]] < 0).all().all()


def test_locked_seasons_are_refused(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    with pytest.raises(ValueError, match="locked"):
        load_rated_balls(db, range(2020, 2026))
