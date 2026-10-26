from __future__ import annotations

import copy
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

import pytest

from cricket_value.ingest.parse import (
    ParsedMatch,
    ParseError,
    UnsupportedSchemaVersion,
    parse_match,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "cricsheet"
ALL_FIXTURES = sorted(FIXTURES.glob("*/*.json"))


def load(rel: str) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads((FIXTURES / rel).read_text())
    return doc


def parse(rel: str, doc: dict[str, Any] | None = None) -> ParsedMatch:
    competition, name = rel.split("/")
    return parse_match(
        doc if doc is not None else load(rel),
        match_id=name.removesuffix(".json"),
        competition=competition,
        file_sha256="f" * 64,
    )


def player_row(p: ParsedMatch, name: str) -> dict[str, Any]:
    return next(r for r in p.match_players if r["player_name"] == name)


# -- matches -----------------------------------------------------------------


def test_completed_match() -> None:
    m = parse("ipl/1082591.json").match

    assert m["match_id"] == "1082591"
    assert m["competition"] == "ipl"
    assert m["season"] == 2017
    assert m["season_label"] == "2017"
    assert m["match_date"] == dt.date(2017, 4, 5)
    assert m["end_date"] == dt.date(2017, 4, 5)
    assert (m["team1"], m["team2"]) == ("Sunrisers Hyderabad", "Royal Challengers Bangalore")
    assert (m["toss_winner"], m["toss_decision"]) == ("Royal Challengers Bangalore", "field")
    assert m["result"] == "win"
    assert m["winner"] == "Sunrisers Hyderabad"
    assert (m["win_by_runs"], m["win_by_wickets"], m["win_method"]) == (35, None, None)
    assert m["decided_by_super_over"] is False
    assert m["venue_name"] == "Rajiv Gandhi International Stadium, Uppal"
    assert m["venue_registry_id"] is None  # registry.venues only exists from schema 1.3.0
    assert m["data_version"] == "1.2.0"
    assert m["file_sha256"] == "f" * 64


def test_season_comes_from_first_date_not_the_label() -> None:
    m = parse("ipl/335982.json").match

    assert m["season"] == 2008
    assert m["season_label"] == "2007/08"


def test_tie_decided_by_super_over() -> None:
    p = parse("ipl/1082625.json")

    assert p.match["result"] == "tie"
    assert p.match["winner"] == "Mumbai Indians"
    assert p.match["decided_by_super_over"] is True
    assert [i["is_super_over"] for i in p.innings] == [False, False, True, True]


def test_tie_decided_by_bowl_out() -> None:
    m = parse("t20i/237242.json").match

    assert m["result"] == "tie"
    assert m["winner"] == "New Zealand"
    assert m["decided_by_bowl_out"] is True
    assert m["decided_by_super_over"] is False


def test_dls_result_over_two_dates() -> None:
    p = parse("ipl/1082648.json")

    assert p.match["win_method"] == "D/L"
    assert p.match["win_by_wickets"] == 7
    assert p.match["match_date"] == dt.date(2017, 5, 17)
    assert p.match["end_date"] == dt.date(2017, 5, 18)
    assert p.match["n_dates"] == 2
    assert (p.innings[1]["target_runs"], p.innings[1]["target_overs"]) == (48, 6.0)


@pytest.mark.parametrize("rel", ["ipl/1178424.json", "ipl/1359519.json"])
def test_no_result(rel: str) -> None:
    m = parse(rel).match

    assert m["result"] == "no_result"
    assert m["winner"] is None


def test_awarded_match_has_winner_but_no_margin() -> None:
    p = parse("t20i/1268191.json")

    assert p.match["win_method"] == "Awarded"
    assert p.match["winner"] == "Malta"
    assert (p.match["win_by_runs"], p.match["win_by_wickets"]) == (None, None)
    assert p.innings[1]["penalty_runs_post"] == 5
    assert p.innings[0]["penalty_runs_post"] == 0


def test_venue_registry_id_from_schema_1_3_0() -> None:
    m = parse("t20i/1496587.json").match

    assert m["data_version"] == "1.3.0"
    assert m["venue_registry_id"] == "2a20f4bc"


# -- innings -----------------------------------------------------------------


def test_innings_teams_and_targets() -> None:
    inns = parse("ipl/1082591.json").innings

    assert [i["innings_number"] for i in inns] == [1, 2]
    assert [i["batting_team"] for i in inns] == [
        "Sunrisers Hyderabad",
        "Royal Challengers Bangalore",
    ]
    assert [i["bowling_team"] for i in inns] == [
        "Royal Challengers Bangalore",
        "Sunrisers Hyderabad",
    ]
    assert inns[0]["target_runs"] is None
    assert inns[1]["target_runs"] == 208


def test_innings_flags() -> None:
    assert any(i["has_miscounted_overs"] for i in parse("ipl/1136564.json").innings)
    assert [i["n_absent_hurt"] for i in parse("t20i/1115800.json").innings] == [0, 1]


# -- deliveries and wickets ----------------------------------------------------


def test_deliveries_reconcile_with_raw_file() -> None:
    p = parse("ipl/1082591.json")

    by_inn: dict[int, list[dict[str, Any]]] = {}
    for d in p.deliveries:
        by_inn.setdefault(d["innings_number"], []).append(d)
    assert [len(v) for v in by_inn.values()] == [125, 123]
    assert [sum(d["runs_total"] for d in v) for v in by_inn.values()] == [207, 172]
    for d in p.deliveries:
        assert d["runs_total"] == d["runs_batter"] + d["runs_extras"]
        assert d["is_legal"] == (d["extras_wides"] == 0 and d["extras_noballs"] == 0)


def test_delivery_uses_registry_ids() -> None:
    p = parse("ipl/1082591.json")
    hex8 = re.compile(r"^[0-9a-f]{8}$")

    for d in p.deliveries:
        for col in ("batter_id", "non_striker_id", "bowler_id"):
            assert hex8.match(d[col]), (col, d[col])


def test_two_wickets_on_one_delivery() -> None:
    p = parse("t20i/1383095.json")

    double = [d for d in p.deliveries if d["n_wickets"] == 2]
    assert len(double) == 1
    key = {k: double[0][k] for k in ("innings_number", "over_number", "ball_in_over")}
    on_ball = [w for w in p.wickets if all(w[k] == v for k, v in key.items())]
    assert [w["wicket_seq"] for w in on_ball] == [1, 2]
    assert len(p.wickets) == sum(d["n_wickets"] for d in p.deliveries)


@pytest.mark.parametrize("path", ALL_FIXTURES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_actual_delivery_is_derived_identically_for_old_schema(path: Path) -> None:
    """Schema < 1.2.0 has no actual_delivery; the parser derives it the same way Cricsheet does."""
    rel = f"{path.parent.name}/{path.name}"
    original = parse(rel)

    old = copy.deepcopy(load(rel))
    old["meta"]["data_version"] = "1.1.0"
    old["info"]["registry"].pop("venues", None)
    for inn in old["innings"]:
        for over in inn["overs"]:
            for ball in over["deliveries"]:
                del ball["actual_delivery"]
    derived = parse(rel, old)

    assert [d["actual_delivery"] for d in derived.deliveries] == [
        d["actual_delivery"] for d in original.deliveries
    ]


# -- players and replacements ---------------------------------------------------


def test_impact_player_is_recorded_but_not_in_starting_xi() -> None:
    p = parse("ipl/1359475.json")

    reps = [r for r in p.replacements if r["replacement_type"] == "match"]
    assert {(r["team"], r["reason"]) for r in reps} == {
        ("Chennai Super Kings", "impact_player"),
        ("Gujarat Titans", "impact_player"),
    }
    for team in ("Chennai Super Kings", "Gujarat Titans"):
        rows = [r for r in p.match_players if r["team"] == team]
        assert len(rows) == 12
        assert sum(r["is_starting_xi"] for r in rows) == 11

    sub = player_row(p, "TU Deshpande")
    assert (sub["is_starting_xi"], sub["joined_reason"]) == (False, "impact_player")
    replaced = player_row(p, "AT Rayudu")
    assert (replaced["is_starting_xi"], replaced["joined_reason"]) == (True, None)


def test_concussion_and_impact_subs_both_excluded_from_starting_xi() -> None:
    p = parse("ipl/1370352.json")

    mi = [r for r in p.match_players if r["team"] == "Mumbai Indians"]
    assert len(mi) == 13
    assert sum(r["is_starting_xi"] for r in mi) == 11
    assert player_row(p, "Vishnu Vinod")["joined_reason"] == "concussion_substitute"


def test_team_with_ten_listed_players_is_parsed_not_rejected() -> None:
    """Too few players is a data quality issue for the contract checks, not a parse failure."""
    p = parse("t20i/1452625.json")

    serbia = [r for r in p.match_players if r["team"] == "Serbia"]
    assert len(serbia) == 10


# -- schema versions and failures ----------------------------------------------


def test_unknown_schema_version_is_rejected() -> None:
    doc = load("ipl/1082591.json")
    doc["meta"]["data_version"] = "9.0.0"

    with pytest.raises(UnsupportedSchemaVersion, match=r"9\.0\.0"):
        parse("ipl/1082591.json", doc)


def test_person_missing_from_registry_is_a_parse_error() -> None:
    doc = load("ipl/1082591.json")
    first_batter = doc["innings"][0]["overs"][0]["deliveries"][0]["batter"]
    del doc["info"]["registry"]["people"][first_batter]

    with pytest.raises(ParseError, match="not in registry"):
        parse("ipl/1082591.json", doc)


@pytest.mark.parametrize("path", ALL_FIXTURES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_every_fixture_parses(path: Path) -> None:
    p = parse(f"{path.parent.name}/{path.name}")

    assert p.match["season"] == p.match["match_date"].year
    assert all(d["match_id"] == p.match["match_id"] for d in p.deliveries)
