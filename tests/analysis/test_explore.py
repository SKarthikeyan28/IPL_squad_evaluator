from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from cricket_value.analysis.explore import (
    chasing_effect,
    first_innings_by_season,
    home_advantage,
    matches_per_season,
    rate_by,
    toss_decisions,
    toss_effect,
    venue_effects,
    wilson,
)
from cricket_value.reference.reference import load_reference

CONFIG = Path(__file__).parents[2] / "config" / "reference"


def matches(*rows: tuple[str, str, str, str, str | None, str, str]) -> pd.DataFrame:
    """(match_id, date, team1, team2, winner, toss_winner, venue) -> IPL matches."""
    df = pd.DataFrame(
        rows,
        columns=["match_id", "match_date", "team1", "team2", "winner", "toss_winner", "venue_name"],
    )
    df["match_date"] = [dt.date.fromisoformat(d) for d in df["match_date"]]
    df["season"] = [d.year for d in df["match_date"]]
    df["competition"] = "ipl"
    # pandas 3 stores a missing string as NaN, which is truthy: test with notna
    df["result"] = df["winner"].notna().map({True: "win", False: "no_result"})
    df["toss_decision"] = "field"
    df["win_method"] = None
    df["event_stage"] = None
    df["decided_by_super_over"] = False
    return df


def innings(*rows: tuple[str, int, str, int, float | None]) -> pd.DataFrame:
    """(match_id, innings_number, batting_team, runs, target_overs)."""
    df = pd.DataFrame(
        rows, columns=["match_id", "innings_number", "batting_team", "runs", "target_overs"]
    )
    df["is_super_over"] = False
    df["season"] = 2019  # silver innings carry the season partition column too
    return df


def test_wilson_interval() -> None:
    p, low, high = wilson(50, 100)

    assert p == 0.5
    assert (low, high) == pytest.approx((0.4038, 0.5962), abs=1e-4)
    assert wilson(0, 0)[0] != wilson(0, 0)[0]  # nan when there is no data


def test_rate_by_counts_and_intervals() -> None:
    df = pd.DataFrame({"season": [2020, 2020, 2020, 2021], "won": [True, False, True, True]})

    rows = {r["season"]: r for r in rate_by(df, "season", "won").to_dict("records")}

    assert rows[2020]["n"] == 3
    assert rows[2020]["rate"] == pytest.approx(2 / 3)
    assert rows[2021]["low"] < 1.0  # an interval, not just the point estimate


def test_matches_per_season_counts_no_results() -> None:
    m = matches(
        (
            "1",
            "2019-04-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "2",
            "2019-04-02",
            "Mumbai Indians",
            "Chennai Super Kings",
            None,
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
    )

    row = matches_per_season(m).iloc[0]

    assert (row["season"], row["matches"], row["no_result"]) == (2019, 2, 1)


def test_toss_effect_excludes_no_results() -> None:
    m = matches(
        (
            "1",
            "2019-04-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "2",
            "2019-04-02",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "3",
            "2019-04-03",
            "Mumbai Indians",
            "Chennai Super Kings",
            None,
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
    )

    out = toss_effect(m).iloc[0]

    assert (out["n"], out["rate"]) == (2, 0.5)


def test_toss_decisions_share_choosing_to_field() -> None:
    m = matches(
        (
            "1",
            "2019-04-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "2",
            "2019-04-02",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
    )
    m.loc[1, "toss_decision"] = "bat"

    assert toss_decisions(m).iloc[0]["rate"] == 0.5


def test_chasing_effect_uses_the_side_batting_second() -> None:
    m = matches(
        (
            "1",
            "2019-04-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "2",
            "2019-04-02",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
    )
    i = innings(
        ("1", 1, "Mumbai Indians", 150, None),
        ("1", 2, "Chennai Super Kings", 151, 20.0),
        ("2", 1, "Chennai Super Kings", 140, None),
        ("2", 2, "Mumbai Indians", 141, 20.0),
    )

    out = chasing_effect(m, i).iloc[0]

    assert (out["n"], out["rate"]) == (2, 1.0)  # the chasing side won both


def test_venue_effects_merge_aliases_and_skip_reduced_matches() -> None:
    m = matches(
        (
            "1",
            "2019-04-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "2",
            "2019-04-02",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium, Mumbai",
        ),
        (
            "3",
            "2019-04-03",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
    )
    i = innings(
        ("1", 1, "Mumbai Indians", 180, None),
        ("1", 2, "Chennai Super Kings", 150, 20.0),
        ("2", 1, "Mumbai Indians", 160, None),
        ("2", 2, "Chennai Super Kings", 100, 20.0),
        ("3", 1, "Mumbai Indians", 60, None),
        ("3", 2, "Chennai Super Kings", 30, 6.0),  # rain-reduced: not a full first innings
    )

    out = venue_effects(m, i, load_reference(CONFIG), min_matches=1).set_index("venue_id")

    assert out.loc["wankhede", "matches"] == 3  # both name spellings are one ground
    assert out.loc["wankhede", "avg_first_innings"] == 170  # (180 + 160) / 2, match 3 skipped
    assert out.loc["wankhede", "first_innings_n"] == 2


def test_home_advantage_skips_neutral_and_no_results() -> None:
    m = matches(
        (
            "1",
            "2019-04-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "2",
            "2019-04-02",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Mumbai Indians",
            "MA Chidambaram Stadium",
        ),
        (
            "3",
            "2020-10-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "4",
            "2019-04-05",
            "Mumbai Indians",
            "Chennai Super Kings",
            None,
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
    )

    games = home_advantage(m, load_reference(CONFIG))

    assert list(games["match_id"]) == ["1", "2"]  # 2020 is neutral; match 4 had no result
    assert list(games["home_won"]) == [True, False]
    assert list(games["home_franchise"]) == ["mi", "csk"]


def test_first_innings_by_season_uses_full_unreduced_innings() -> None:
    m = matches(
        (
            "1",
            "2019-04-01",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "2",
            "2019-04-02",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
        (
            "3",
            "2023-04-03",
            "Mumbai Indians",
            "Chennai Super Kings",
            "Mumbai Indians",
            "Mumbai Indians",
            "Wankhede Stadium",
        ),
    )
    i = innings(
        ("1", 1, "Mumbai Indians", 160, None),
        ("1", 2, "Chennai Super Kings", 150, 20.0),
        ("2", 1, "Mumbai Indians", 90, None),
        ("2", 2, "Chennai Super Kings", 40, 8.0),
        ("3", 1, "Mumbai Indians", 200, None),
        ("3", 2, "Chennai Super Kings", 150, 20.0),
    )

    out = first_innings_by_season(m, i).set_index("season")

    assert out.loc[2019, "avg_first_innings"] == 160  # the 8-over match is left out
    assert out.loc[2019, "n"] == 1
    assert out.loc[2023, "avg_first_innings"] == 200
