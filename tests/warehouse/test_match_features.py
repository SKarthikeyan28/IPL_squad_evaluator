"""The match_features mart: one row per decided IPL team-match, team minus opponent."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.warehouse.conftest import query
from tests.warehouse.test_ratings import CUTOFF
from tests.warehouse.test_squad_features import reordered_warehouse  # noqa: F401

CONTEXT = {"home_edge", "chase_edge"}
FLAGS = ["decided_by_super_over", "reduced_overs", "impact_player_era"]
KEYS = ["team_match_key", "match_id", "team_id", "opponent_team_id", "season", "match_date"]


def features(db: Path) -> pd.DataFrame:
    return query(db, "select * from match_features order by team_match_key")


def difference_columns(frame: pd.DataFrame) -> list[str]:
    return [c for c in frame.columns if c.startswith("d_")]


def test_one_row_per_team_of_each_decided_ipl_match(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    got = features(db)
    decided = query(
        db,
        "select team_match_key from fact_team_match "
        "where competition = 'ipl' and result in ('won', 'lost')",
    )

    assert set(got["team_match_key"]) == set(decided["team_match_key"])
    assert not got["team_match_key"].duplicated().any()
    assert got.groupby("match_id").size().eq(2).all()
    assert got.groupby("match_id")["won"].sum().eq(1).all()  # exactly one winner


def test_abandoned_matches_are_left_out_and_super_over_matches_have_a_winner(
    warehouse: tuple[Path, Path],
) -> None:
    _, db = warehouse
    got = features(db)
    no_result = query(
        db,
        "select match_id from fact_team_match where competition = 'ipl' and result = 'no_result'",
    )

    assert len(no_result) > 0  # the fixture has them, so this isn't vacuous
    assert not set(no_result["match_id"]) & set(got["match_id"])
    assert got["decided_by_super_over"].any()


def test_the_label_is_the_teams_own_result(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    got = features(db)
    results = query(db, "select team_match_key, result from fact_team_match")

    merged = got.merge(results, on="team_match_key")
    assert (merged["won"] == (merged["result"] == "won")).all()


def test_the_two_orientations_of_a_match_are_mirror_images(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    got = features(db)
    columns = [*difference_columns(got), *CONTEXT]
    assert len(columns) > 15

    for _, pair in got.groupby("match_id"):
        a, b = pair.iloc[0], pair.iloc[1]
        assert a["team_id"] == b["opponent_team_id"] and b["team_id"] == a["opponent_team_id"]
        np.testing.assert_allclose(
            a[columns].astype(float), -b[columns].astype(float), equal_nan=True
        )
        assert a["won"] != b["won"]
        assert all(a[f] == b[f] for f in FLAGS)  # symmetric: the same for both sides


def test_differences_are_team_minus_opponent_squad_features(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    got = features(db)
    squad = query(db, "select * from squad_features").set_index("team_match_key").to_dict("index")

    for row in got.to_dict("records"):
        own = squad[row["team_match_key"]]
        other = squad[f"{row['match_id']}-{row['opponent_team_id']}"]
        for column in difference_columns(got):
            name = column.removeprefix("d_")
            want = own[name] - other[name]
            assert row[column] == pytest.approx(want, nan_ok=True), (row["team_match_key"], column)


def test_context_is_what_is_known_at_the_toss(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    got = features(db)
    team = query(
        db,
        "select team_match_key, home_status, batted_first, won_toss from fact_team_match",
    )
    merged = got.merge(team, on="team_match_key")

    home = merged["home_status"].map({"home": 1, "away": -1, "neutral": 0})
    assert (merged["home_edge"] == home).all()
    assert (merged["chase_edge"] == np.where(merged["batted_first"], -1, 1)).all()
    # who won the toss is deliberately not a feature: who chases is what carries the edge
    assert not {"won_toss", "toss_winner_edge"} & set(got.columns)


def test_flags(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    got = features(db)
    teams = query(db, "select team_match_key, win_method from fact_team_match")
    merged = got.merge(teams, on="team_match_key")

    assert merged["reduced_overs"].any()
    assert (merged["reduced_overs"] == (merged["win_method"] == "D/L")).all()
    assert (merged["impact_player_era"] == (merged["season"] >= 2023)).all()


def test_nothing_but_the_label_depends_on_what_happens_after_the_toss(
    warehouse: tuple[Path, Path],
    reordered_warehouse: Path,  # noqa: F811
) -> None:
    """Rewriting the balls of every match from CUTOFF leaves every non-label column unchanged."""
    _, db = warehouse
    before, after = features(db), features(reordered_warehouse)
    inputs = [c for c in before.columns if c != "won"]
    early = before["match_date"].dt.date <= CUTOFF
    assert early.sum() > 0

    pd.testing.assert_frame_equal(
        before.loc[early, inputs].reset_index(drop=True),
        after.loc[early, inputs].reset_index(drop=True),
    )
