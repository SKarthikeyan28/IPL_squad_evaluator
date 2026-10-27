from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from cricket_value.reference.reference import ReferenceError, load_reference

CONFIG = Path(__file__).parents[2] / "config" / "reference"


@pytest.fixture
def config(tmp_path: Path) -> Path:
    out = tmp_path / "reference"
    shutil.copytree(CONFIG, out)
    return out


def rewrite(config: Path, name: str, df: pd.DataFrame) -> None:
    df.to_csv(config / f"{name}.csv", index=False)


def test_repo_reference_files_are_valid() -> None:
    ref = load_reference(CONFIG)

    assert ref.team_aliases["team_name"].is_unique
    assert set(ref.team_aliases["franchise_id"]) == set(ref.franchises["franchise_id"])
    assert ref.home_venues["last_season"].dtype == "Int64"  # nullable: open-ended ranges
    assert ref.home_venues["league_only"].dtype == bool


def test_every_row_needs_a_source(config: Path) -> None:
    df = pd.read_csv(config / "home_venues.csv", dtype=str)
    df.loc[0, "source_url"] = None
    rewrite(config, "home_venues", df)

    with pytest.raises(ReferenceError, match="home_venues: source_url missing"):
        load_reference(config)


def test_alias_must_point_at_a_known_franchise(config: Path) -> None:
    df = pd.read_csv(config / "team_aliases.csv", dtype=str)
    df.loc[0, "franchise_id"] = "nope"
    rewrite(config, "team_aliases", df)

    with pytest.raises(ReferenceError, match=r"team_aliases\.franchise_id 'nope'"):
        load_reference(config)


def test_duplicate_alias_is_rejected(config: Path) -> None:
    df = pd.read_csv(config / "venue_aliases.csv", dtype=str)
    rewrite(config, "venue_aliases", pd.concat([df, df.head(1)]))

    with pytest.raises(ReferenceError, match="venue_aliases: duplicate venue_name"):
        load_reference(config)


def test_overlapping_home_ranges_are_rejected(config: Path) -> None:
    df = pd.read_csv(config / "home_venues.csv", dtype=str)
    extra = df[df["franchise_id"] == "pbks"].head(1).assign(first_season=2009, last_season=2009)
    extra = pd.concat([extra, extra.assign(first_season=2009, last_season=2010)])
    rewrite(config, "home_venues", pd.concat([df, extra]))

    with pytest.raises(ReferenceError, match="overlapping seasons"):
        load_reference(config)


def test_unknown_role_is_rejected(config: Path) -> None:
    df = pd.read_csv(config / "home_venues.csv", dtype=str)
    df.loc[0, "role"] = "spiritual"
    rewrite(config, "home_venues", df)

    with pytest.raises(ReferenceError, match="role 'spiritual'"):
        load_reference(config)


def test_league_only_must_be_true_or_false(config: Path) -> None:
    df = pd.read_csv(config / "home_venues.csv", dtype=str)
    df.loc[0, "league_only"] = "maybe"
    rewrite(config, "home_venues", df)

    with pytest.raises(ReferenceError, match="league_only must be true or false"):
        load_reference(config)
