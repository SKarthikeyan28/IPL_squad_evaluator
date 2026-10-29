from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from cricket_value.ratings.split_eval import compare, paired_difference

PHASES = ["powerplay", "middle", "death"]


def league() -> pd.DataFrame:
    """The same runs distribution for every (season, phase): uniform over 0-6."""
    index = pd.MultiIndex.from_product([[2020], PHASES], names=["season", "phase"])
    return pd.DataFrame(1 / 7, index=index, columns=range(7))


def balls(n: int = 8) -> pd.DataFrame:
    """Half the balls end in a dismissal. The unsplit rating says 5%; the split rating, 50%.
    Every other input is neutral (the bowler is the average bowler, runs per ball is 3)."""
    return pd.DataFrame(
        {
            "match_id": [f"m{i // 2}" for i in range(n)],
            "season": 2020,
            "phase": "middle",
            "runs_batter": 3,
            "batter_dismissed": [i % 2 == 0 for i in range(n)],
            "bat_dismissal": 0.05,
            "bowl_wicket": 0.05,
            "bowl_wicket_mean": 0.05,
            "bat_runs": 3.0,
            "bowl_runs": 1.0,
            "bowl_runs_mean": 1.0,
            "split_dismissal": 0.5,
            "split_runs": 3.0,
        }
    )


def test_a_split_rating_that_matches_the_outcomes_beats_one_that_does_not() -> None:
    totals = compare(balls(), league())

    n = totals["balls"].sum()
    loss_all = -totals["ll_dismissal_all"].sum() / n
    loss_split = -totals["ll_dismissal_split"].sum() / n
    # dismissed balls: -log(0.05); others: -log(0.95)  vs  -log(0.5) on every ball
    assert loss_all == pytest.approx((-np.log(0.05) - np.log(0.95)) / 2)
    assert loss_split == pytest.approx(-np.log(0.5))
    assert loss_split < loss_all


def test_identical_runs_inputs_give_identical_runs_scores() -> None:
    totals = compare(balls(), league())

    assert totals["ll_runs_all"].to_numpy() == pytest.approx(totals["ll_runs_split"].to_numpy())


def test_totals_are_per_match() -> None:
    totals = compare(balls(8), league())

    assert list(totals.index) == ["m0", "m1", "m2", "m3"]
    assert (totals["balls"] == 2).all()


def test_paired_difference_is_split_minus_all_per_1000_balls_with_an_interval() -> None:
    totals = compare(balls(40), league())

    out = paired_difference(totals, n_resamples=500, seed=1)

    # every match is identical, so the difference is exact and the interval has no width
    expected = 1000 * (-np.log(0.5) - (-np.log(0.05) - np.log(0.95)) / 2)
    assert out["dismissal"].diff == pytest.approx(expected)
    assert out["dismissal"].low == pytest.approx(expected)
    assert out["dismissal"].high == pytest.approx(expected)
    assert out["runs"].diff == pytest.approx(0, abs=1e-9)
    assert out["total"].diff == pytest.approx(expected)


def test_paired_difference_interval_covers_the_observed_value_and_is_reproducible() -> None:
    rng = np.random.default_rng(0)
    totals = pd.DataFrame(
        {
            "balls": 6,
            "ll_dismissal_all": rng.normal(-1.0, 0.3, 80),
            "ll_dismissal_split": rng.normal(-0.95, 0.3, 80),
            "ll_runs_all": rng.normal(-8, 1, 80),
            "ll_runs_split": rng.normal(-8, 1, 80),
        },
        index=[f"m{i}" for i in range(80)],
    )

    a = paired_difference(totals, n_resamples=500, seed=3)
    b = paired_difference(totals, n_resamples=500, seed=3)

    assert a["total"] == b["total"]
    assert a["total"].low < a["total"].diff < a["total"].high
