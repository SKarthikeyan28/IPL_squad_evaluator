"""Exploratory analysis of IPL matches: seasons, the toss, venues and home advantage.

Every rate comes with a Wilson 95% interval. One IPL season has only about 60-74
matches, so a single season's rate is noisy, and the interval says how noisy.

"Decided" matches are those with an official winner: no-results are left out, and ties
settled by a super over count for the super-over winner.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from cricket_value.reference.home import match_home_status
from cricket_value.reference.reference import Reference

Z_95 = 1.959964


def wilson(k: int, n: int, z: float = Z_95) -> tuple[float, float, float]:
    """Proportion k/n with its Wilson score interval. Better than p ± z·se for small n
    or rates near 0 or 1, and it never leaves [0, 1]."""
    if n == 0:
        return math.nan, math.nan, math.nan
    p = k / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return p, centre - half, centre + half


def rate_by(df: pd.DataFrame, by: str, flag: str) -> pd.DataFrame:
    """For each value of ``by``: n, successes k, rate and its 95% interval."""
    rows = []
    for key, g in df.groupby(by, sort=True):
        n, k = len(g), int(g[flag].sum())
        p, low, high = wilson(k, n)
        rows.append({by: key, "n": n, "k": k, "rate": p, "low": low, "high": high})
    return pd.DataFrame(rows, columns=[by, "n", "k", "rate", "low", "high"])


def pooled(df: pd.DataFrame, flag: str) -> dict[str, float]:
    p, low, high = wilson(int(df[flag].sum()), len(df))
    return {"n": len(df), "rate": p, "low": low, "high": high}


def decided(matches: pd.DataFrame) -> pd.DataFrame:
    return matches[(matches["result"] != "no_result") & matches["winner"].notna()]


def load_ipl(silver_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """IPL matches, and IPL innings with their total runs, from the silver tables."""
    matches = pd.read_parquet(silver_dir / "matches")
    matches["season"] = matches["season"].astype("int64")
    matches = matches[matches["competition"] == "ipl"].reset_index(drop=True)
    ids = set(matches["match_id"])
    innings = pd.read_parquet(silver_dir / "innings")
    innings = innings[innings["match_id"].isin(ids)]
    deliveries = pd.read_parquet(
        silver_dir / "deliveries", columns=["match_id", "innings_number", "runs_total"]
    )
    runs = (
        deliveries[deliveries["match_id"].isin(ids)]
        .groupby(["match_id", "innings_number"])["runs_total"]
        .sum()
    )
    innings = innings.merge(
        runs.rename("runs").reset_index(), on=["match_id", "innings_number"], how="left"
    )
    innings["runs"] = innings["runs"].fillna(0).astype("int64")
    return matches, innings.reset_index(drop=True)


# -- seasons and the toss -----------------------------------------------------------------


def matches_per_season(matches: pd.DataFrame) -> pd.DataFrame:
    g = matches.groupby("season")
    return pd.DataFrame(
        {
            "matches": g.size(),
            "no_result": g["result"].apply(lambda s: int((s == "no_result").sum())),
            "super_over": g["decided_by_super_over"].sum().astype(int),
            "dls": g["win_method"].apply(lambda s: int((s == "D/L").sum())),
        }
    ).reset_index()


def toss_effect(matches: pd.DataFrame) -> pd.DataFrame:
    """Per season: how often the toss winner went on to win."""
    d = decided(matches).assign(won=lambda df: df["toss_winner"] == df["winner"])
    return rate_by(d, "season", "won")


def toss_decisions(matches: pd.DataFrame) -> pd.DataFrame:
    """Per season: share of toss winners choosing to field first."""
    return rate_by(matches.assign(field=matches["toss_decision"] == "field"), "season", "field")


def chases(matches: pd.DataFrame, innings: pd.DataFrame) -> pd.DataFrame:
    """Decided matches with a second innings, flagged by whether the chasing side won."""
    regular = innings[~innings["is_super_over"].astype(bool)]
    second = regular[regular["innings_number"] == 2][["match_id", "batting_team"]]
    d = decided(matches).merge(second, on="match_id")
    return d.assign(chase_won=d["winner"] == d["batting_team"])


def chasing_effect(matches: pd.DataFrame, innings: pd.DataFrame) -> pd.DataFrame:
    """Per season: how often the side batting second won."""
    return rate_by(chases(matches, innings).rename(columns={"chase_won": "won"}), "season", "won")


def _full_first_innings(matches: pd.DataFrame, innings: pd.DataFrame) -> pd.DataFrame:
    """First innings of full 20-over matches with no rain rule, with the match's season
    and venue: the fair basis for comparing totals."""
    regular = innings[~innings["is_super_over"].astype(bool)]
    full = regular[(regular["innings_number"] == 2) & (regular["target_overs"] == 20)]["match_id"]
    no_rain_rule = matches.loc[matches["win_method"].isna(), "match_id"]
    first = regular[
        (regular["innings_number"] == 1)
        & regular["match_id"].isin(full)
        & regular["match_id"].isin(no_rain_rule)
    ]
    # season comes from matches: silver innings carry their own partition column
    first = first.drop(columns=["season"], errors="ignore")
    return first.merge(matches[["match_id", "season", "venue_name"]], on="match_id")


def first_innings_by_season(matches: pd.DataFrame, innings: pd.DataFrame) -> pd.DataFrame:
    """Per season: average first-innings total in full, unreduced matches."""
    first = _full_first_innings(matches, innings)
    out = first.groupby("season")["runs"].agg(avg_first_innings="mean", n="size")
    return out.reset_index()


# -- venues ------------------------------------------------------------------------------


def venue_effects(
    matches: pd.DataFrame, innings: pd.DataFrame, ref: Reference, *, min_matches: int = 20
) -> pd.DataFrame:
    """Per ground (aliases merged): decided matches, chasing win rate with interval, and
    the average first-innings total in full 20-over matches with no rain rule."""
    venue_of = ref.venue_aliases.set_index("venue_name")["venue_id"]
    chased = chases(matches, innings)
    chased["venue_id"] = chased["venue_name"].map(venue_of)
    chase = rate_by(chased, "venue_id", "chase_won")

    first = _full_first_innings(matches, innings)
    first["venue_id"] = first["venue_name"].map(venue_of)
    scores = first.groupby("venue_id")["runs"].agg(avg_first_innings="mean", first_innings_n="size")

    venues = ref.venues.set_index("venue_id")[["canonical_name", "city"]]
    out = (
        chase.rename(
            columns={
                "n": "matches",
                "rate": "chase_win_rate",
                "low": "chase_low",
                "high": "chase_high",
            }
        )
        .drop(columns="k")
        .join(scores, on="venue_id")
        .join(venues, on="venue_id")
    )
    out = out[out["matches"] >= min_matches]
    return out.sort_values("matches", ascending=False, ignore_index=True)


# -- home advantage -----------------------------------------------------------------------


def home_advantage(matches: pd.DataFrame, ref: Reference) -> pd.DataFrame:
    """One row per decided match that had a home side: did the home side win?

    Home status comes from the sourced reference data (neutral seasons, secondary
    grounds, league-only rows), so neutral matches never count either way.
    """
    status = match_home_status(matches, ref)
    home = status[status["home_status"] == "home"][["match_id", "franchise_id", "venue_id"]]
    franchise_of = ref.team_aliases.set_index("team_name")["franchise_id"]
    d = decided(matches)[["match_id", "season", "winner"]].merge(home, on="match_id")
    d["home_won"] = d["winner"].map(franchise_of) == d["franchise_id"]
    d = d.rename(columns={"franchise_id": "home_franchise"})
    return d[["match_id", "season", "home_franchise", "venue_id", "home_won"]].sort_values(
        ["season", "match_id"], ignore_index=True
    )
