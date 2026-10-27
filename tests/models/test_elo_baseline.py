from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from cricket_value.models.elo import EloParams
from cricket_value.models.elo_baseline import evaluate, games_from_silver, tune

GRID = [
    EloParams(k=k, home_advantage=h, season_regression=r)
    for k in (10, 30)
    for h in (0, 50)
    for r in (0.0, 0.5)
]


def synthetic_games(seed: int = 0) -> pd.DataFrame:
    """Six teams with fixed true strengths, 2008-2016, 30 games a season."""
    rng = np.random.default_rng(seed)
    teams = ["a", "b", "c", "d", "e", "f"]
    strength = dict(zip(teams, [200, 100, 50, 0, -50, -150], strict=True))
    rows = []
    for season in range(2008, 2017):
        for i in range(30):
            ta, tb = rng.choice(teams, 2, replace=False)
            p = 1 / (1 + 10 ** (-(strength[ta] - strength[tb]) / 400))
            winner = ta if rng.random() < p else tb
            rows.append(
                {
                    "match_id": f"{season}-{i:02d}",
                    "match_date": dt.date(season, 4, 1) + dt.timedelta(days=i),
                    "season": season,
                    "franchise_a": ta,
                    "franchise_b": tb,
                    "winner": winner,
                    "home_a": "neutral",
                    "toss_winner_is_a": bool(rng.random() < 0.5),
                }
            )
    return pd.DataFrame(rows)


def test_tuning_for_a_season_never_sees_that_season_or_later() -> None:
    g = synthetic_games()
    chosen = tune(g, test_season=2013, grid=GRID, first_scored_season=2009)

    # Rewrite every result from 2013 on: the parameters chosen for 2013 must not change
    later = g["season"] >= 2013
    flipped = g.copy()
    flipped.loc[later, "winner"] = np.where(
        g.loc[later, "winner"] == g.loc[later, "franchise_a"],
        g.loc[later, "franchise_b"],
        g.loc[later, "franchise_a"],
    )

    assert tune(flipped, test_season=2013, grid=GRID, first_scored_season=2009) == chosen


def test_evaluate_scores_only_the_test_seasons_and_beats_a_coin_on_real_signal() -> None:
    result = evaluate(
        synthetic_games(),
        test_seasons=[2014, 2015, 2016],
        grid=GRID,
        first_scored_season=2009,
        n_resamples=300,
    )

    assert set(result.predictions["season"]) == {2014, 2015, 2016}
    assert set(result.per_season["season"]) == {2014, 2015, 2016}
    loss = result.pooled.set_index("model")["log_loss"].to_dict()
    assert loss["elo"] < loss["coin_flip"]


def test_games_from_silver_maps_names_to_franchises_and_winners() -> None:
    matches = pd.DataFrame(
        {
            "match_id": ["1", "2", "3"],
            "season": [2018, 2019, 2019],
            "match_date": [dt.date(2018, 4, 1), dt.date(2019, 4, 1), dt.date(2019, 4, 2)],
            "competition": ["ipl"] * 3,
            "team1": ["Delhi Daredevils", "Delhi Capitals", "Mumbai Indians"],
            "team2": ["Mumbai Indians", "Mumbai Indians", "Gujarat Lions"],
            "winner": ["Mumbai Indians", "Delhi Capitals", None],
            "result": ["win", "win", "no_result"],
            "toss_winner": ["Mumbai Indians", "Delhi Capitals", "Mumbai Indians"],
            "venue_name": ["Feroz Shah Kotla", "Arun Jaitley Stadium", "Wankhede Stadium"],
            "event_stage": [None, None, None],
        }
    )

    g = games_from_silver(matches).set_index("match_id")

    assert list(g["franchise_a"]) == ["dc", "dc", "mi"]  # one franchise across the rename
    assert list(g["winner"].fillna("-")) == ["mi", "dc", "-"]
    assert list(g["home_a"]) == ["home", "home", "home"]
    assert list(g["toss_winner_is_a"]) == [False, True, True]
