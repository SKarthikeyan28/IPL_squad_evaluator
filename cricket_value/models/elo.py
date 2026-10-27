"""Team Elo ratings for IPL franchises: the first baseline.

Each franchise has a rating. Before a match, the rating gap (plus a home bonus) gives
the probability that team A wins:

    p_a = 1 / (1 + 10 ** (-(rating_a - rating_b + home_advantage * home) / 400))

where ``home`` is +1 if A is at home, -1 if B is, 0 at a neutral ground. After the match
both ratings move by ``k * (result - p_a)``: an upset moves them a lot, an expected
result barely at all. No-results are predicted but do not change ratings.

At the first match of each season every rating is pulled ``season_regression`` of the way
back to the initial rating, because squads change a lot between seasons.

Point-in-time: matches are processed in date order, and a match's prediction uses the
ratings *before* it. Only the two teams' own earlier matches have moved those ratings,
and a team never plays twice on one day, so no prediction uses a match on or after its
own date.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

HOME_SIGN = {"home": 1, "away": -1, "neutral": 0}


@dataclass(frozen=True)
class EloParams:
    k: float = 20.0
    home_advantage: float = 0.0  # rating points
    season_regression: float = 1 / 3
    initial: float = 1500.0


def expected_score(rating_a: float, rating_b: float, *, home: int, home_advantage: float) -> float:
    return 1 / (1 + 10 ** (-(rating_a - rating_b + home_advantage * home) / 400))


def run_elo(games: pd.DataFrame, params: EloParams) -> tuple[pd.DataFrame, dict[str, float]]:
    """Predict every game in date order, updating ratings as it goes.

    ``games`` needs match_id, season, match_date, franchise_a, franchise_b, winner
    (a franchise id, or null for no result) and home_a ('home', 'away' or 'neutral').
    Returns one prediction row per game, and the ratings after the last game.
    """
    ratings: dict[str, float] = {}
    season: int | None = None
    rows = []
    ordered = games.sort_values(["match_date", "match_id"])
    for g in ordered.to_dict("records"):
        if season is not None and g["season"] != season:
            c = params.season_regression
            ratings = {
                t: params.initial + (1 - c) * (r - params.initial) for t, r in ratings.items()
            }
        season = int(g["season"])

        a, b = str(g["franchise_a"]), str(g["franchise_b"])
        ra = ratings.setdefault(a, params.initial)
        rb = ratings.setdefault(b, params.initial)
        home = HOME_SIGN[str(g["home_a"])]
        p = expected_score(ra, rb, home=home, home_advantage=params.home_advantage)

        winner = g["winner"]
        if winner is None or (isinstance(winner, float) and math.isnan(winner)):
            outcome = math.nan
        else:
            outcome = 1.0 if winner == a else 0.0
            delta = params.k * (outcome - p)
            ratings[a] = ra + delta
            ratings[b] = rb - delta

        rows.append(
            {
                "match_id": g["match_id"],
                "season": season,
                "match_date": g["match_date"],
                "franchise_a": a,
                "franchise_b": b,
                "home_a": g["home_a"],
                "rating_a_pre": ra,
                "rating_b_pre": rb,
                "p_a": p,
                "outcome_a": outcome,
            }
        )
    return pd.DataFrame(rows), ratings
