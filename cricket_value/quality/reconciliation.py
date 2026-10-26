"""Reconciliation checks: do the numbers agree with each other across tables?

Contracts check each row on its own. These checks compare totals across tables, the
way an accountant ties a ledger to a statement. Cricsheet has no innings totals, so the
ball-by-ball runs are tied to the two independent numbers it does record: the second
innings target and the winning margin.

All are ``warn``: a mismatch is flagged in ``_violations`` and kept, never dropped.
Real cricket produces some of them (a side that really fielded 10 players), and a flag
is enough for downstream models to exclude a match if they need to.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from cricket_value.quality.contracts import VIOLATION_COLUMNS, empty_violations

Tables = Mapping[str, pd.DataFrame]

# Not dismissals: the batter left but was not out, so they don't count towards 10 wickets
NOT_OUT_KINDS = ["retired hurt", "retired not out"]


def reconcile(tables: Tables) -> pd.DataFrame:
    matches = tables["matches"]
    innings = _innings_with_runs(tables)
    parts = [
        _starting_xi_has_11(tables["match_players"]),
        _at_most_10_dismissals(tables["wickets"], innings),
        _legal_balls_within_limit(tables["deliveries"], innings),
        _target_is_first_innings_plus_one(innings, matches),
        _margin_matches_scores(innings, matches),
    ]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return empty_violations()
    out = pd.concat(parts, ignore_index=True)[VIOLATION_COLUMNS]
    out["season"] = out["season"].astype("int64")
    return out.sort_values(["season", "match_id", "rule", "failure_case"], ignore_index=True)


def _flags(rule: str, df: pd.DataFrame, failure_case: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "match_id": df["match_id"].to_numpy(dtype=object),
            "season": df["season"].to_numpy(),
            "table": "reconciliation",
            "rule": rule,
            "severity": "warn",
            "column": None,
            "failure_case": failure_case.astype(str).to_numpy(),
        }
    )


def _limit(is_super_over: pd.Series, *, super_over: int, regular: int) -> pd.Series:
    """Per-innings limit. Built with numpy so the result stays integer even when empty."""
    values = np.where(is_super_over.astype(bool).to_numpy(), super_over, regular)
    return pd.Series(values, index=is_super_over.index, dtype="int64")


def _innings_with_runs(tables: Tables) -> pd.DataFrame:
    runs = (
        tables["deliveries"]
        .groupby(["match_id", "innings_number"])["runs_total"]
        .sum()
        .rename("runs")
        .reset_index()
    )
    out = tables["innings"].merge(runs, on=["match_id", "innings_number"], how="left")
    out["runs"] = out["runs"].fillna(0).astype("int64")
    return out


def _starting_xi_has_11(match_players: pd.DataFrame) -> pd.DataFrame:
    xi = match_players.groupby(["match_id", "season", "team"], as_index=False)[
        "is_starting_xi"
    ].sum()
    bad = xi[xi["is_starting_xi"] != 11]
    return _flags("starting_xi_has_11", bad, bad["team"] + ": " + bad["is_starting_xi"].astype(str))


def _at_most_10_dismissals(wickets: pd.DataFrame, innings: pd.DataFrame) -> pd.DataFrame:
    out = wickets[~wickets["kind"].isin(NOT_OUT_KINDS)]
    n = out.groupby(["match_id", "innings_number"]).size().rename("dismissals").reset_index()
    n = n.merge(innings[["match_id", "season", "innings_number", "is_super_over"]])
    limit = _limit(n["is_super_over"], super_over=2, regular=10)
    bad = n[n["dismissals"] > limit]
    return _flags(
        "at_most_10_dismissals",
        bad,
        "innings " + bad["innings_number"].astype(str) + ": " + bad["dismissals"].astype(str),
    )


def _legal_balls_within_limit(deliveries: pd.DataFrame, innings: pd.DataFrame) -> pd.DataFrame:
    legal = deliveries[deliveries["is_legal"].astype(bool)]
    n = legal.groupby(["match_id", "innings_number"]).size().rename("legal_balls").reset_index()
    n = n.merge(
        innings[["match_id", "season", "innings_number", "is_super_over", "has_miscounted_overs"]]
    )
    limit = _limit(n["is_super_over"], super_over=6, regular=120)
    # Cricsheet flags innings where the umpires miscounted an over; extra balls are real there
    bad = n[(n["legal_balls"] > limit) & ~n["has_miscounted_overs"].astype(bool)]
    return _flags(
        "legal_balls_within_limit",
        bad,
        "innings " + bad["innings_number"].astype(str) + ": " + bad["legal_balls"].astype(str),
    )


def _first_two_innings(innings: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    """One row per match with both regular innings, plus the match result columns."""
    regular = innings[~innings["is_super_over"].astype(bool)]
    first = regular[regular["innings_number"] == 1][["match_id", "batting_team", "runs"]]
    second = regular[regular["innings_number"] == 2][
        ["match_id", "batting_team", "runs", "target_runs", "target_overs"]
    ]
    both = first.merge(second, on="match_id", suffixes=("_1", "_2"))
    return both.merge(
        matches[
            [
                "match_id",
                "season",
                "result",
                "winner",
                "win_by_runs",
                "win_by_wickets",
                "win_method",
            ]
        ],
        on="match_id",
    )


def _target_is_first_innings_plus_one(innings: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    df = _first_two_innings(innings, matches)
    # Only full 20-over chases with no rain rule: DLS and reduced matches set other targets
    df = df[df["target_runs"].notna() & df["win_method"].isna() & (df["target_overs"] == 20)]
    bad = df[df["target_runs"] != df["runs_1"] + 1]
    return _flags(
        "target_is_first_innings_plus_one",
        bad,
        "target " + bad["target_runs"].astype(str) + ", first innings " + bad["runs_1"].astype(str),
    )


def _margin_matches_scores(innings: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    df = _first_two_innings(innings, matches)
    df = df[(df["result"] == "win") & df["win_method"].isna()]

    by_runs = df[df["win_by_runs"].notna()]
    runs_ok = (by_runs["winner"] == by_runs["batting_team_1"]) & (
        by_runs["runs_1"] - by_runs["runs_2"] == by_runs["win_by_runs"]
    )
    bad_runs = by_runs[~runs_ok.astype(bool)]

    by_wkts = df[df["win_by_wickets"].notna()]
    wkts_ok = (by_wkts["winner"] == by_wkts["batting_team_2"]) & (
        by_wkts["runs_2"] > by_wkts["runs_1"]
    )
    bad_wkts = by_wkts[~wkts_ok.astype(bool)]

    bad = pd.concat([bad_runs, bad_wkts], ignore_index=True)
    case = (
        bad["winner"].astype(str)
        + " won; scores "
        + bad["runs_1"].astype(str)
        + " v "
        + bad["runs_2"].astype(str)
        + "; margin "
        + bad["win_by_runs"]
        .astype(str)
        .where(bad["win_by_runs"].notna(), bad["win_by_wickets"].astype(str) + " wkts")
    )
    return _flags("margin_matches_scores", bad, case)
