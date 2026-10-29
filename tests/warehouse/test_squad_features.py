"""Batting positions, the starting XI and the squad_features mart, on the fixture warehouse."""

from __future__ import annotations

import json
import math
import shutil
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml

from cricket_value.warehouse.dbt import REPO
from tests.warehouse.conftest import build_warehouse, query
from tests.warehouse.test_ratings import CUTOFF, falsify_matches_from

VARS = yaml.safe_load((REPO / "dbt" / "dbt_project.yml").read_text())["vars"]
DECAY = math.log(2) / VARS["rating_half_life_days"]
PRIOR_MATCHES = VARS["batting_position_prior_matches"]
FALLBACK = VARS["batting_position_fallback"]
IPL_FIXTURES = REPO / "tests" / "fixtures" / "cricsheet" / "ipl"


def cricsheet_batting_order(path: Path) -> dict[str, dict[str, int]]:
    """Position by first appearance as striker or non-striker, straight from the raw file."""
    match = json.loads(path.read_text())
    people = match["info"]["registry"]["people"]
    out: dict[str, dict[str, int]] = {}
    for number, innings in enumerate(match["innings"], start=1):
        if innings.get("super_over"):
            continue
        seen: dict[str, int] = {}
        for over in innings["overs"]:
            for ball in over["deliveries"]:
                for name in (ball["batter"], ball["non_striker"]):
                    seen.setdefault(people[name], len(seen) + 1)
        out[str(number)] = seen
    return out


def test_batting_position_is_the_order_of_first_appearance(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    got = query(db, "select * from int_batting_positions")

    checked = 0
    for path in sorted(IPL_FIXTURES.glob("*.json")):
        expected = cricsheet_batting_order(path)
        rows = got[got["match_id"] == path.stem]
        for innings, order in expected.items():
            in_innings = rows[rows["innings_number"] == int(innings)]
            assert dict(zip(in_innings["player_id"], in_innings["position"], strict=True)) == order
            checked += len(order)
    assert checked > 100


def test_super_overs_have_no_batting_positions(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    tie = query(db, "select * from int_batting_positions where match_id = '1082625'")

    assert set(tie["innings_number"]) == {1, 2}


def test_the_xi_is_the_starting_xi_and_never_the_impact_player(
    warehouse: tuple[Path, Path],
) -> None:
    _, db = warehouse
    xi = query(db, "select match_id, team_id, player_id from int_squad_xi")
    listed = query(
        db,
        """
        select mp.match_id, mp.player_id, mp.is_starting_xi, mp.joined_reason
        from stg_silver__match_players mp
        join stg_silver__matches m using (match_id)
        where m.competition = 'ipl'
        """,
    )
    starters = listed[listed["is_starting_xi"]]
    impact = listed[listed["joined_reason"] == "impact_player"]

    assert len(impact) > 0  # the fixture has Impact Players, so this isn't vacuous
    got = set(zip(xi["match_id"], xi["player_id"], strict=True))
    assert got == set(zip(starters["match_id"], starters["player_id"], strict=True))
    assert not got & set(zip(impact["match_id"], impact["player_id"], strict=True))


def test_slots_follow_expected_batting_position(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    xi = query(db, "select * from int_squad_xi order by team_match_key, slot")

    for _, side in xi.groupby("team_match_key"):
        assert list(side["slot"]) == list(range(1, len(side) + 1))
        assert side["expected_position"].is_monotonic_increasing


def positions_with_roles(db: Path) -> pd.DataFrame:
    return query(
        db,
        """
        select p.player_id, p.match_date, p.position, r.role
        from int_batting_positions p
        join (
            select distinct player_id, match_date, role from int_player_ratings_long
        ) r using (player_id, match_date)
        where p.competition = 'ipl'
        """,
    )


def expected_position_in_python(
    history: pd.DataFrame, player: str, day: pd.Timestamp, role: str
) -> float:
    """Decay-weighted mean of earlier positions, shrunk to the role's average before the year."""
    earlier_in_year = history[history["match_date"].dt.year < day.year]
    in_role = earlier_in_year[earlier_in_year["role"] == role]
    default = in_role["position"].mean() if len(in_role) else FALLBACK[role]

    own = history[(history["player_id"] == player) & (history["match_date"] < day)]
    weight = ((day - own["match_date"]).dt.days.rsub(0) * DECAY).map(math.exp)
    return float(
        ((weight * own["position"]).sum() + PRIOR_MATCHES * default)
        / (weight.sum() + PRIOR_MATCHES)
    )


def test_expected_position_matches_an_independent_calculation(
    warehouse: tuple[Path, Path],
) -> None:
    _, db = warehouse
    history = positions_with_roles(db)
    history["match_date"] = pd.to_datetime(history["match_date"])
    got = query(db, "select * from int_expected_batting_position")
    got["match_date"] = pd.to_datetime(got["match_date"])

    assert len(got) > 0
    for row in got.to_dict("records"):
        want = expected_position_in_python(
            history, str(row["player_id"]), pd.Timestamp(row["match_date"]), str(row["role"])
        )
        assert row["expected_position"] == pytest.approx(want, rel=1e-9), row


def test_a_player_with_no_earlier_innings_gets_their_roles_typical_position(
    warehouse: tuple[Path, Path],
) -> None:
    _, db = warehouse
    got = query(db, "select * from int_expected_batting_position where decayed_innings = 0")

    assert len(got) > 0
    first_ever = got[got["match_date"] == got["match_date"].min()]
    assert len(first_ever) > 0
    # nothing earlier exists for the very first match, so the constants are used
    for row in first_ever.itertuples():
        assert row.expected_position == pytest.approx(FALLBACK[row.role])


def test_squad_features_has_one_row_per_ipl_team_match_with_an_xi(
    warehouse: tuple[Path, Path],
) -> None:
    _, db = warehouse
    features = query(db, "select * from squad_features")
    team_matches = query(db, "select team_match_key from fact_team_match where competition = 'ipl'")

    assert not features["team_match_key"].duplicated().any()
    assert set(features["team_match_key"]) == set(team_matches["team_match_key"])
    assert (features["n_xi"] == 11).all()


def test_squad_features_match_a_calculation_from_the_xi_and_ratings(
    warehouse: tuple[Path, Path],
) -> None:
    _, db = warehouse
    features = query(db, "select * from squad_features").set_index("team_match_key")
    xi = query(
        db,
        """
        select x.*, r.*  exclude (player_id, match_date, role, bowl_share)
        from int_squad_xi x
        join player_ratings_asof r using (player_id, match_date)
        """,
    )

    for key, side in xi.groupby("team_match_key"):
        row = features.loc[key]
        top, middle, finish = (
            side[side["slot"] <= 3],
            side[side["slot"].between(4, 6)],
            side[side["slot"].between(5, 8)],
        )
        assert row["top_bat_runs_per_ball"] == pytest.approx(
            top["bat_powerplay_runs_per_ball"].mean()
        )
        assert row["mid_bat_dismissal_rate"] == pytest.approx(
            middle["bat_middle_dismissal_rate"].mean()
        )
        assert row["finish_bat_runs_per_ball"] == pytest.approx(
            finish["bat_death_runs_per_ball"].mean()
        )

        attack = (
            side.assign(share=side["bowl_share"].fillna(0))
            .sort_values(["share", "slot"], ascending=[False, True])
            .head(5)
        )
        assert row["death_bowl_wicket_rate"] == pytest.approx(
            attack["bowl_death_wicket_rate"].mean()
        )

        assert row["n_all_rounders"] == (side["role"] == "all_rounder").sum()
        assert row["n_low_sample_players"] == (side["role"] == "unknown").sum()
        options = side["bowl_share"].fillna(0) >= VARS["rating_role_batter_max_share"]
        assert row["n_bowling_options"] == options.sum()
        assert row["n_pace_options"] + row["n_spin_options"] <= row["n_bowling_options"]
        depth = side["bowl_share"].fillna(0) < VARS["rating_role_bowler_min_share"]
        assert row["batting_depth"] == depth.sum()


def reverse_batting_order_from(silver: Path, cutoff: date) -> None:
    """Rewrite who batted where in every match on or after ``cutoff``: in each innings the first
    batter to appear trades identities with the last, the second with the second last, and so on."""
    matches = pd.read_parquet(silver / "matches")
    late = set(matches.loc[pd.to_datetime(matches["match_date"]).dt.date >= cutoff, "match_id"])
    for part in (silver / "deliveries").glob("*/*.parquet"):
        df = pd.read_parquet(part)
        for _, innings in df[df["match_id"].isin(late)].groupby(["match_id", "innings_number"]):
            in_order = innings.sort_values(["over_number", "ball_in_over"])
            seen: list[str] = []
            for pair in zip(in_order["batter_id"], in_order["non_striker_id"], strict=True):
                seen += [p for p in pair if p not in seen]
            reverse = dict(zip(seen, reversed(seen), strict=True))
            for column in ("batter_id", "non_striker_id"):
                df.loc[innings.index, column] = innings[column].map(reverse)
        df.to_parquet(part, index=False)


@pytest.fixture(scope="module")
def reordered_warehouse(
    warehouse: tuple[Path, Path], tmp_path_factory: pytest.TempPathFactory
) -> Path:
    silver, _ = warehouse
    root = tmp_path_factory.mktemp("wh_reordered")
    shutil.copytree(silver, root / "silver")
    falsify_matches_from(root / "silver", CUTOFF)
    reverse_batting_order_from(root / "silver", CUTOFF)
    return build_warehouse(root / "silver", root)


def test_no_feature_reads_the_batting_order_or_outcome_of_its_own_match_or_later(
    warehouse: tuple[Path, Path], reordered_warehouse: Path
) -> None:
    """Rewriting every ball from CUTOFF onwards (including the order the batters came in) leaves
    the features of every match up to and including CUTOFF unchanged."""
    _, db = warehouse
    sql = "select * from squad_features"
    cols = "order by team_match_key"
    before = query(db, f"{sql} {cols}")
    after = query(reordered_warehouse, f"{sql} {cols}")
    early = before["match_date"].dt.date <= CUTOFF
    assert early.sum() > 0

    pd.testing.assert_frame_equal(
        before[early].reset_index(drop=True), after[early].reset_index(drop=True)
    )

    # the rewrite is visible in the batting positions themselves, so the check isn't vacuous
    positions = "select * from int_batting_positions order by match_id, innings_number, player_id"
    assert not query(db, positions).equals(query(reordered_warehouse, positions))
