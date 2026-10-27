"""Load and validate the reference CSVs in ``config/reference/``.

The CSVs are hand-maintained facts, so they get the same treatment as data: every key
unique, every ID resolving to a parent row, every row carrying a source link. A broken
file fails loudly here instead of silently mislabelling home games downstream.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

ROLES = {"primary", "secondary", "relocated"}


class ReferenceError(ValueError):
    """One or more reference files break a rule. The message lists every problem."""


@dataclass(frozen=True)
class Reference:
    franchises: pd.DataFrame
    team_aliases: pd.DataFrame
    venues: pd.DataFrame
    venue_aliases: pd.DataFrame
    home_venues: pd.DataFrame
    neutral_periods: pd.DataFrame


def load_reference(config_dir: Path) -> Reference:
    def read(
        name: str,
        ints: tuple[str, ...] = (),
        dates: tuple[str, ...] = (),
        bools: tuple[str, ...] = (),
    ) -> pd.DataFrame:
        df = pd.read_csv(config_dir / f"{name}.csv", dtype=str, keep_default_na=False)
        df = df.replace({"": None})
        for c in ints:
            df[c] = pd.to_numeric(df[c]).astype("Int64")
        for c in dates:
            df[c] = pd.to_datetime(df[c]).dt.date
        for c in bools:
            bad = sorted(set(df[c].dropna()) - {"true", "false"})
            if bad or df[c].isna().any():
                raise ReferenceError(f"{name}.{c} must be true or false, got {bad or 'blank'}")
            df[c] = df[c] == "true"
        return df

    ref = Reference(
        franchises=read("franchises", ints=("first_season", "last_season")),
        team_aliases=read("team_aliases"),
        venues=read("venues"),
        venue_aliases=read("venue_aliases"),
        home_venues=read(
            "home_venues", ints=("first_season", "last_season"), bools=("league_only",)
        ),
        neutral_periods=read("neutral_periods", dates=("start_date", "end_date")),
    )
    problems = validate(ref)
    if problems:
        raise ReferenceError("\n".join(problems))
    return ref


def validate(ref: Reference) -> list[str]:
    problems: list[str] = []

    def unique(name: str, df: pd.DataFrame, key: list[str]) -> None:
        dup = df[df.duplicated(key, keep=False)]
        for row in dup[key].drop_duplicates().itertuples(index=False):
            problems.append(f"{name}: duplicate {'/'.join(key)} {tuple(row)}")

    def resolves(name: str, df: pd.DataFrame, column: str, parent: pd.Series) -> None:
        for value in sorted(set(df[column].dropna()) - set(parent)):
            problems.append(f"{name}.{column} {value!r} is not defined")

    def sourced(name: str, df: pd.DataFrame) -> None:
        bad = df["source_url"].isna() | ~df["source_url"].fillna("").str.startswith("https://")
        for i in df.index[bad]:
            problems.append(f"{name}: source_url missing or not https on row {i + 2}")

    unique("franchises", ref.franchises, ["franchise_id"])
    unique("team_aliases", ref.team_aliases, ["team_name"])
    unique("venues", ref.venues, ["venue_id"])
    unique("venue_aliases", ref.venue_aliases, ["venue_name"])
    unique("home_venues", ref.home_venues, ["franchise_id", "venue_id", "first_season"])

    resolves("team_aliases", ref.team_aliases, "franchise_id", ref.franchises["franchise_id"])
    resolves("venue_aliases", ref.venue_aliases, "venue_id", ref.venues["venue_id"])
    resolves("home_venues", ref.home_venues, "franchise_id", ref.franchises["franchise_id"])
    resolves("home_venues", ref.home_venues, "venue_id", ref.venues["venue_id"])

    for name in ("franchises", "team_aliases", "venues", "home_venues", "neutral_periods"):
        sourced(name, getattr(ref, name))

    hv = ref.home_venues
    for row in hv[~hv["role"].isin(ROLES)].itertuples():
        problems.append(f"home_venues: role {row.role!r} must be one of {sorted(ROLES)}")
    for name, df in (("franchises", ref.franchises), ("home_venues", hv)):
        backwards = df["last_season"].notna() & (df["last_season"] < df["first_season"])
        for i in df.index[backwards]:
            problems.append(f"{name}: last_season before first_season on row {i + 2}")
    problems += _overlaps(hv)

    np_ = ref.neutral_periods.sort_values("start_date")
    for row in np_[np_["end_date"] < np_["start_date"]].itertuples():
        problems.append(f"neutral_periods: end before start for {row.start_date}")
    if (np_["start_date"].iloc[1:].to_numpy() <= np_["end_date"].iloc[:-1].to_numpy()).any():
        problems.append("neutral_periods: periods overlap")
    return problems


def _overlaps(hv: pd.DataFrame) -> list[str]:
    """The same franchise and venue must not have two rows covering one season."""
    problems = []
    for (franchise, venue), g in hv.groupby(["franchise_id", "venue_id"]):
        g = g.sort_values("first_season")
        last = g["last_season"].fillna(9999).astype(int).to_numpy()
        first = g["first_season"].astype(int).to_numpy()
        if (first[1:] <= last[:-1]).any():
            problems.append(f"home_venues: overlapping seasons for {franchise} at {venue}")
    return problems
