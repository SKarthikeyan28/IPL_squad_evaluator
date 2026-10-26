from __future__ import annotations

import pandas as pd

from cricket_value.quality.contracts import (
    VIOLATION_COLUMNS,
    apply_quarantine,
    check_contracts,
)
from tests.quality.conftest import Tables, edit


def rules(violations: pd.DataFrame, severity: str = "error") -> set[tuple[str, str, str]]:
    v = violations[violations["severity"] == severity]
    return set(zip(v["match_id"], v["table"], v["rule"], strict=True))


def test_real_fixtures_pass_every_error_rule(tables: Tables) -> None:
    violations = check_contracts(tables)

    assert list(violations.columns) == VIOLATION_COLUMNS
    assert rules(violations) == set()


def test_runs_that_do_not_add_up_are_an_error(tables: Tables) -> None:
    d = tables["deliveries"]
    tables["deliveries"] = edit(d, d["match_id"] == "1082591", "runs_total", 99)

    violations = check_contracts(tables)

    assert ("1082591", "deliveries", "runs_total_is_batter_plus_extras") in rules(violations)
    # a whole-row rule reports the failing row once, identified by its key
    row = violations[violations["rule"] == "runs_total_is_batter_plus_extras"]
    assert len(row) == 1
    assert (
        row["failure_case"].item()
        == "match_id=1082591 innings_number=1 over_number=0 ball_in_over=1"
    )


def test_unknown_wicket_kind_is_an_error(tables: Tables) -> None:
    w = tables["wickets"]
    tables["wickets"] = edit(w, w["match_id"] == "1082625", "kind", "eaten by dog")

    assert ("1082625", "wickets", "kind_accepted") in rules(check_contracts(tables))


def test_duplicate_delivery_key_is_an_error(tables: Tables) -> None:
    d = tables["deliveries"]
    dup = d[d["match_id"] == "1178424"].head(1)
    tables["deliveries"] = pd.concat([d, dup], ignore_index=True)

    assert ("1178424", "deliveries", "key_unique") in rules(check_contracts(tables))


def test_toss_winner_must_be_one_of_the_teams(tables: Tables) -> None:
    m = tables["matches"]
    tables["matches"] = edit(m, m["match_id"] == "335982", "toss_winner", "Somebody Else")

    assert ("335982", "matches", "toss_winner_is_a_team") in rules(check_contracts(tables))


def test_rows_for_a_match_that_does_not_exist_are_an_error(tables: Tables) -> None:
    i = tables["innings"]
    orphan = i.head(1).assign(match_id="999999")
    tables["innings"] = pd.concat([i, orphan], ignore_index=True)

    assert ("999999", "innings", "match_exists") in rules(check_contracts(tables))


def test_batting_team_must_play_in_the_match(tables: Tables) -> None:
    i = tables["innings"]
    tables["innings"] = edit(i, i["match_id"] == "1359475", "batting_team", "Mumbai Indians")

    assert ("1359475", "innings", "batting_team_is_a_team") in rules(check_contracts(tables))


def test_wrong_overs_metadata_is_only_a_warning(tables: Tables) -> None:
    m = tables["matches"]
    tables["matches"] = edit(m, m["match_id"] == "1383095", "overs_per_side", 50)

    violations = check_contracts(tables)

    assert ("1383095", "matches", "overs_per_side_is_20") in rules(violations, "warn")
    assert rules(violations) == set()


def test_quarantine_removes_the_whole_match_from_every_table(tables: Tables) -> None:
    d = tables["deliveries"]
    tables["deliveries"] = edit(d, d["match_id"] == "1082591", "runs_total", 99)
    n_deliveries = int((d["match_id"] == "1082591").sum())

    clean, quarantined = apply_quarantine(tables, check_contracts(tables))

    for name, df in clean.items():
        assert "1082591" not in set(df["match_id"]), name
    q = quarantined["deliveries"]
    assert len(q) == n_deliveries
    assert set(q["quarantine_rules"]) == {"deliveries.runs_total_is_batter_plus_extras"}
    assert len(quarantined["matches"]) == 1
    # every other match is untouched
    assert len(clean["matches"]) == len(tables["matches"]) - 1


def test_warnings_do_not_quarantine(tables: Tables) -> None:
    m = tables["matches"]
    tables["matches"] = edit(m, m["match_id"] == "1383095", "overs_per_side", 50)

    clean, quarantined = apply_quarantine(tables, check_contracts(tables))

    assert "1383095" in set(clean["matches"]["match_id"])
    assert all(q.empty for q in quarantined.values())
