from __future__ import annotations

from pathlib import Path

import pandas as pd

from cricket_value.reference.home import match_home_status
from cricket_value.reference.reference import load_reference

CONFIG = Path(__file__).parents[2] / "config" / "reference"


def matches(*rows: tuple[str, str, str, str, str]) -> pd.DataFrame:
    """(match_id, date, team1, team2, venue_name) -> a minimal IPL matches frame."""
    df = pd.DataFrame(rows, columns=["match_id", "match_date", "team1", "team2", "venue_name"])
    df["match_date"] = pd.to_datetime(df["match_date"]).dt.date
    df["season"] = [d.year for d in df["match_date"]]
    df["competition"] = "ipl"
    df["event_stage"] = None
    return df


def status(df: pd.DataFrame) -> dict[tuple[str, str], str]:
    out = match_home_status(df, load_reference(CONFIG))
    return {(str(r.match_id), str(r.franchise_id)): str(r.home_status) for r in out.itertuples()}


def test_primary_home_ground() -> None:
    s = status(
        matches(("1", "2019-04-01", "Mumbai Indians", "Chennai Super Kings", "Wankhede Stadium"))
    )

    assert s == {("1", "mi"): "home", ("1", "csk"): "away"}


def test_renamed_team_and_renamed_venue_resolve() -> None:
    s = status(
        matches(("1", "2012-04-10", "Delhi Daredevils", "Kings XI Punjab", "Feroz Shah Kotla"))
    )

    assert s == {("1", "dc"): "home", ("1", "pbks"): "away"}


def test_secondary_venue_only_in_its_seasons() -> None:
    s = status(
        matches(
            ("1", "2017-05-01", "Kings XI Punjab", "Mumbai Indians", "Holkar Cricket Stadium"),
            ("2", "2011-05-01", "Kochi Tuskers Kerala", "Mumbai Indians", "Holkar Cricket Stadium"),
            ("3", "2019-05-01", "Kings XI Punjab", "Mumbai Indians", "Holkar Cricket Stadium"),
        )
    )

    assert s[("1", "pbks")] == "home"  # Indore was a Kings XI venue in 2017
    assert s[("2", "ktk")] == "home"  # and Kochi's in 2011
    assert s[("3", "pbks")] == "neutral"  # but not in 2019


def test_neutral_periods_override_home_grounds() -> None:
    s = status(
        matches(
            ("1", "2014-04-20", "Mumbai Indians", "Chennai Super Kings", "Wankhede Stadium"),
            ("2", "2014-05-10", "Mumbai Indians", "Chennai Super Kings", "Wankhede Stadium"),
            ("3", "2021-04-20", "Mumbai Indians", "Chennai Super Kings", "Wankhede Stadium"),
        )
    )

    assert s[("1", "mi")] == "neutral"  # inside the 2014 UAE window (dates only matter)
    assert s[("2", "mi")] == "home"  # after it
    assert s[("3", "mi")] == "neutral"  # 2021: no team played at home


def test_both_teams_at_home_is_neutral() -> None:
    venue = "Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium"
    s = status(matches(("1", "2016-05-10", "Mumbai Indians", "Rising Pune Supergiants", venue)))

    assert s == {("1", "mi"): "neutral", ("1", "rps"): "neutral"}


def test_unmapped_names_come_back_null() -> None:
    out = match_home_status(
        matches(("1", "2019-04-01", "Mystery XI", "Mumbai Indians", "Moon Oval")),
        load_reference(CONFIG),
    )

    row = out[out["team"] == "Mystery XI"].iloc[0]
    assert pd.isna(row["franchise_id"])
    assert pd.isna(row["venue_id"])
    assert row["home_status"] == "neutral"


def test_t20i_matches_are_ignored() -> None:
    df = matches(("1", "2019-04-01", "India", "Australia", "Wankhede Stadium"))
    df["competition"] = "t20i"

    assert match_home_status(df, load_reference(CONFIG)).empty


def test_league_only_home_venue_does_not_make_playoffs_home_games() -> None:
    df = matches(
        (
            "1",
            "2010-03-12",
            "Deccan Chargers",
            "Kolkata Knight Riders",
            "Dr DY Patil Sports Academy",
        ),
        ("2", "2010-04-22", "Chennai Super Kings", "Deccan Chargers", "Dr DY Patil Sports Academy"),
    )
    df.loc[df["match_id"] == "2", "event_stage"] = "Semi Final"

    s = status(df)

    assert s[("1", "deccan")] == "home"  # league game at a borrowed home ground
    assert s[("2", "deccan")] == "neutral"  # semi-final at the same ground
