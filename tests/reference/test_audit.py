from __future__ import annotations

from pathlib import Path

import pandas as pd

from cricket_value.reference.audit import audit
from cricket_value.reference.reference import load_reference

CONFIG = Path(__file__).parents[2] / "config" / "reference"


def matches(*rows: tuple[str, str, str, str, str]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["match_id", "match_date", "team1", "team2", "venue_name"])
    df["match_date"] = pd.to_datetime(df["match_date"]).dt.date
    df["season"] = [d.year for d in df["match_date"]]
    df["competition"] = "ipl"
    df["event_stage"] = None
    return df


def checks(findings: pd.DataFrame) -> set[tuple[str, str]]:
    return set(zip(findings["severity"], findings["check"], strict=True))


def test_unmapped_names_are_errors() -> None:
    f = audit(
        matches(("1", "2019-04-01", "Mystery XI", "Mumbai Indians", "Moon Oval")),
        load_reference(CONFIG),
    )

    assert ("error", "unmapped_team") in checks(f)
    assert ("error", "unmapped_venue") in checks(f)


def test_ground_dominated_by_one_team_but_not_listed_is_flagged() -> None:
    # Two 2019 league matches at Indore, both involving Kings XI, but Indore isn't listed
    # as a Kings XI home venue for 2019
    f = audit(
        matches(
            ("1", "2019-04-01", "Kings XI Punjab", "Mumbai Indians", "Holkar Cricket Stadium"),
            ("2", "2019-04-05", "Chennai Super Kings", "Kings XI Punjab", "Holkar Cricket Stadium"),
        ),
        load_reference(CONFIG),
    )

    missing = f[f["check"] == "missing_home_venue"]
    assert list(zip(missing["franchise_id"], missing["venue_id"], strict=True)) == [
        ("pbks", "holkar")
    ]


def test_neutral_seasons_are_not_audited_for_home_grounds() -> None:
    f = audit(
        matches(
            ("1", "2020-10-01", "Mumbai Indians", "Kings XI Punjab", "Sheikh Zayed Stadium"),
            ("2", "2020-10-05", "Mumbai Indians", "Delhi Capitals", "Sheikh Zayed Stadium"),
        ),
        load_reference(CONFIG),
    )

    assert "missing_home_venue" not in set(f["check"])
