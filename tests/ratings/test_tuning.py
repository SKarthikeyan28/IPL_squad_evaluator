from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from cricket_value.ratings.tuning import (
    GridPoint,
    bootstrap_choice,
    combine_means,
    combine_rates,
    edges_hit,
    fall_back_to_league,
    league_dismissal_rate,
    league_runs_distribution,
    match_totals,
    score_balls,
    score_league_only,
    summarise,
    tilt,
)


def test_an_average_bowler_leaves_the_batters_dismissal_rate_unchanged() -> None:
    p = combine_rates(np.array([0.05]), np.array([0.04]), np.array([0.04]))
    assert p == pytest.approx([0.05])


def test_dismissal_odds_multiply_by_the_bowlers_odds_ratio() -> None:
    # batter odds 1:1, bowler odds 0.25 against a league 1/9: 1 * 0.25 / (1/9) = 2.25
    p = combine_rates(np.array([0.5]), np.array([0.2]), np.array([0.1]))
    assert p == pytest.approx([2.25 / 3.25])


def test_runs_scale_by_the_bowlers_ratio_to_the_league() -> None:
    mu = combine_means(np.array([1.5, 1.2]), np.array([1.2, 1.0]), np.array([1.0, 1.0]))
    assert mu == pytest.approx([1.8, 1.2])


def test_tilt_moves_the_mean_and_keeps_the_shape_otherwise() -> None:
    # q = (0.5, 0.5) on {0, 1}; tilting by theta = ln 4 gives (1, 4) / 5 = (0.2, 0.8)
    q = np.array([[0.5, 0.5]])
    out = tilt(q, np.array([0.8]), support=np.array([0.0, 1.0]))
    assert out == pytest.approx(np.array([[0.2, 0.8]]))


def test_tilt_to_the_baselines_own_mean_is_the_baseline() -> None:
    q = np.array([[0.4, 0.4, 0.1, 0.0, 0.07, 0.0, 0.03]])
    mean = float(q[0] @ np.arange(7))
    out = tilt(q, np.array([mean]))
    assert out == pytest.approx(q)
    assert out.sum() == pytest.approx(1.0)


def test_tilt_matches_the_target_mean_on_the_lumpy_runs_support() -> None:
    q = np.array([[0.37, 0.39, 0.06, 0.003, 0.12, 0.001, 0.056]] * 3)
    q = q / q.sum(axis=1, keepdims=True)
    mu = np.array([0.6, 1.3, 2.4])
    out = tilt(q, mu)
    assert out @ np.arange(7) == pytest.approx(mu)


def test_league_distribution_uses_only_earlier_seasons() -> None:
    balls = pd.DataFrame(
        {
            "season": [2008, 2008, 2008, 2008, 2009, 2010],
            "phase": ["death"] * 6,
            "runs_batter": [0, 1, 6, 7, 4, 4],  # a 7 counts as 6
        }
    )
    q = league_runs_distribution(balls, seasons=[2009, 2010])

    assert q.loc[[(2009, "death")]].to_numpy()[0] == pytest.approx([0.25, 0.25, 0, 0, 0, 0, 0.5])
    assert q.loc[[(2010, "death")]].to_numpy()[0] == pytest.approx([0.2, 0.2, 0, 0, 0.2, 0, 0.4])


def test_league_dismissal_rate_uses_only_earlier_seasons() -> None:
    history = pd.DataFrame(
        {
            "season": [2008, 2008, 2008, 2008, 2009],
            "phase": ["middle"] * 5,
            "batter_dismissed": [True, False, False, False, True],
        }
    )
    rate = league_dismissal_rate(history, seasons=[2009, 2010])
    assert rate.loc[(2009, "middle")] == pytest.approx(0.25)
    assert rate.loc[(2010, "middle")] == pytest.approx(0.4)


def hand_balls() -> pd.DataFrame:
    """Two balls with ratings chosen so the expected scores can be worked out by hand."""
    return pd.DataFrame(
        {
            "match_id": ["m1", "m1"],
            "season": [2010, 2010],
            "phase": ["death", "death"],
            "runs_batter": [1, 0],
            "batter_dismissed": [False, True],
            "bat_dismissal": [0.5, 0.1],
            "bowl_wicket": [0.2, 0.2],
            "bowl_wicket_mean": [0.1, 0.2],
            "bat_runs": [0.8, 0.5],
            "bowl_runs": [1.0, 1.0],
            "bowl_runs_mean": [1.0, 2.0],
        }
    )


def test_score_balls_by_hand() -> None:
    # league: half dots, half singles, so runs only take the values 0 and 1 here
    league = pd.DataFrame(
        [[0.5, 0.5, 0, 0, 0, 0, 0]],
        index=pd.MultiIndex.from_tuples([(2010, "death")], names=["season", "phase"]),
        columns=range(7),
    )
    scored = score_balls(hand_balls(), league)

    # ball 1: not out with p = 2.25 / 3.25; one run with mean 0.8, so p(1) = 0.8
    # ball 2: average bowler, out with p = 0.1; mean 0.5 * 1/2 = 0.25, so p(0) = 0.75
    assert scored["ll_dismissal"].tolist() == pytest.approx([math.log(1 / 3.25), math.log(0.1)])
    assert scored["ll_runs"].tolist() == pytest.approx([math.log(0.8), math.log(0.75)])


def test_league_only_scores_each_ball_by_the_league_rates() -> None:
    league = pd.DataFrame(
        [[0.5, 0.25, 0, 0, 0.25, 0, 0]],
        index=pd.MultiIndex.from_tuples([(2010, "death")], names=["season", "phase"]),
        columns=range(7),
    )
    balls = hand_balls().assign(runs_batter=[4, 0], league_dismissal=0.2)
    scored = score_league_only(balls, league)
    assert scored["ll_runs"].tolist() == pytest.approx([math.log(0.25), math.log(0.5)])
    assert scored["ll_dismissal"].tolist() == pytest.approx([math.log(0.8), math.log(0.2)])


def test_unrated_balls_fall_back_to_the_league_and_are_counted() -> None:
    league = pd.DataFrame(
        [[0.5, 0.25, 0, 0, 0.25, 0, 0]],
        index=pd.MultiIndex.from_tuples([(2010, "death")], names=["season", "phase"]),
        columns=range(7),
    )
    balls = hand_balls().assign(league_dismissal=0.2)
    balls.loc[1, "bowl_runs"] = np.nan  # a year with no bowling prior: no rating

    filled = fall_back_to_league(balls, league)

    assert filled["rated"].tolist() == [True, False]
    assert filled.loc[0, "bat_dismissal"] == 0.5  # rated balls keep their ratings
    unrated = filled.loc[1]
    # the league's mean runs: 0.25 * 1 + 0.25 * 4
    assert (unrated.bat_dismissal, unrated.bat_runs) == pytest.approx((0.2, 1.25))
    assert unrated.bowl_wicket == unrated.bowl_wicket_mean
    assert unrated.bowl_runs == unrated.bowl_runs_mean


def test_match_totals_and_summary() -> None:
    scored = pd.DataFrame(
        {
            "match_id": ["a", "a", "b"],
            "rated": [True, False, True],
            "ll_dismissal": [-0.1, -0.2, -0.3],
            "ll_runs": [-1.0, -1.5, -2.0],
        }
    )
    totals = match_totals(scored)
    assert totals.loc[["a"]].to_numpy()[0] == pytest.approx([2, 1, -0.3, -2.5])

    grid = {GridPoint(365, 0.5): totals, GridPoint(730, 0.5): totals * [1, 1, 2, 1]}
    table = summarise(grid)
    first = table.iloc[0]
    # log loss per ball, lower is better: 0.6 / 3, 4.5 / 3, and their sum
    assert (first.half_life_days, first.balls, first.rated_share) == (365, 3, 2 / 3)
    assert [first.dismissal, first.runs, first.total] == pytest.approx([0.2, 1.5, 1.7])
    assert table["total"].tolist() == sorted(table["total"])


def test_bootstrap_choice_picks_a_clearly_better_point_every_time() -> None:
    rng = np.random.default_rng(0)
    base = pd.DataFrame(
        {"balls": 240, "rated": 240, "ll_dismissal": -50.0, "ll_runs": rng.normal(-300, 5, 50)},
        index=[f"m{i}" for i in range(50)],
    )
    better = base.assign(ll_runs=base["ll_runs"] + 1.0)
    grid = {GridPoint(365, 0.5): base, GridPoint(730, 0.5): better}

    share = bootstrap_choice(grid, n_resamples=200, seed=0)

    assert share[GridPoint(730, 0.5)] == 1.0
    assert share.get(GridPoint(365, 0.5), 0.0) == 0.0


def test_edges_hit() -> None:
    half_lives, weights = [90, 365, 730], [0.0, 0.5, 1.0]
    assert edges_hit(GridPoint(365, 0.5), half_lives, weights) == []
    assert edges_hit(GridPoint(90, 1.0), half_lives, weights) == [
        "half_life_days at the low edge (90)",
        "t20i_weight at the high edge (1)",
    ]
