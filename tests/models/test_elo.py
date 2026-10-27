from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from cricket_value.models.elo import EloParams, expected_score, run_elo


def games(*rows: tuple[str, str, str, str, str | None, str]) -> pd.DataFrame:
    """(match_id, date, franchise_a, franchise_b, winner, home_of_a) -> games frame."""
    df = pd.DataFrame(
        rows, columns=["match_id", "match_date", "franchise_a", "franchise_b", "winner", "home_a"]
    )
    df["match_date"] = [dt.date.fromisoformat(d) for d in df["match_date"]]
    df["season"] = [d.year for d in df["match_date"]]
    return df


P = EloParams(k=20, home_advantage=0, season_regression=0)


def test_expected_score() -> None:
    assert expected_score(1500, 1500, home=0, home_advantage=50) == pytest.approx(0.5)
    assert expected_score(1500, 1500, home=1, home_advantage=50) == pytest.approx(
        1 / (1 + 10 ** (-50 / 400))
    )
    assert expected_score(1500, 1500, home=-1, home_advantage=50) == pytest.approx(
        1 / (1 + 10 ** (50 / 400))
    )


def test_winner_gains_what_loser_loses() -> None:
    out, ratings = run_elo(games(("1", "2010-04-01", "mi", "csk", "mi", "neutral")), P)

    assert out["p_a"].item() == pytest.approx(0.5)
    assert ratings["mi"] == pytest.approx(1510)
    assert ratings["csk"] == pytest.approx(1490)


def test_ratings_before_a_match_come_only_from_earlier_matches() -> None:
    g = games(
        ("1", "2010-04-01", "mi", "csk", "mi", "neutral"),
        ("2", "2010-04-05", "csk", "kkr", "kkr", "neutral"),
        ("3", "2010-04-09", "mi", "kkr", None, "neutral"),
    )

    out, _ = run_elo(g, P)
    shuffled, _ = run_elo(g.sample(frac=1, random_state=3), P)

    r = out.set_index("match_id")
    assert (r.loc["2", "rating_a_pre"], r.loc["2", "rating_b_pre"]) == (1490, 1500)
    assert r.loc["3", "rating_a_pre"] == pytest.approx(1510)
    pd.testing.assert_frame_equal(out, shuffled)  # input order never matters


def test_no_result_is_predicted_but_does_not_move_ratings() -> None:
    out, ratings = run_elo(games(("1", "2010-04-01", "mi", "csk", None, "neutral")), P)

    assert pd.isna(out["outcome_a"].item())
    assert ratings == {"mi": 1500, "csk": 1500}


def test_home_advantage_shifts_the_prediction() -> None:
    p = EloParams(k=20, home_advantage=60, season_regression=0)
    out, _ = run_elo(
        games(
            ("1", "2010-04-01", "mi", "csk", "mi", "home"),
            ("2", "2010-04-01", "rr", "kkr", "rr", "away"),
        ),
        p,
    )

    p_a = out.set_index("match_id")["p_a"].to_dict()
    assert p_a["1"] > 0.5
    assert p_a["2"] < 0.5


def test_season_start_pulls_ratings_toward_the_mean() -> None:
    p = EloParams(k=20, home_advantage=0, season_regression=0.5)
    out, _ = run_elo(
        games(
            ("1", "2010-04-01", "mi", "csk", "mi", "neutral"),
            ("2", "2011-04-01", "mi", "csk", "mi", "neutral"),
        ),
        p,
    )

    second = out.set_index("match_id").loc["2"]
    assert second["rating_a_pre"] == pytest.approx(1505)  # 1510 pulled halfway to 1500
    assert second["rating_b_pre"] == pytest.approx(1495)


def test_new_franchise_starts_at_the_initial_rating() -> None:
    out, _ = run_elo(
        games(
            ("1", "2010-04-01", "mi", "csk", "mi", "neutral"),
            ("2", "2022-04-01", "gt", "mi", "gt", "neutral"),
        ),
        P,
    )

    assert out.set_index("match_id").loc["2", "rating_a_pre"] == 1500
