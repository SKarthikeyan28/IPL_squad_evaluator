"""Derive home / away / neutral for every team in every IPL match.

The rules are SQL (``match_home_status.sql``) run on DuckDB, so the same query can
become a dbt model without being rewritten.
"""

from __future__ import annotations

from importlib.resources import files

import duckdb
import pandas as pd

from cricket_value.reference.reference import Reference

SQL = files("cricket_value.reference").joinpath("match_home_status.sql").read_text()


def match_home_status(matches: pd.DataFrame, ref: Reference) -> pd.DataFrame:
    """One row per IPL match per team. ``matches`` needs match_id, season, match_date,
    competition, team1, team2, venue_name and event_stage (null for league matches)."""
    con = duckdb.connect()
    try:
        con.register("matches", matches)
        con.register("team_aliases", ref.team_aliases)
        con.register("venue_aliases", ref.venue_aliases)
        con.register("home_venues", ref.home_venues)
        con.register("neutral_periods", ref.neutral_periods)
        out: pd.DataFrame = con.sql(SQL).df()
    finally:
        con.close()
    return out
