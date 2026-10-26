from __future__ import annotations

import pandas as pd

from cricket_value.quality.contracts import VIOLATION_COLUMNS
from cricket_value.quality.reconciliation import reconcile
from tests.quality.conftest import Tables, edit


def flagged(violations: pd.DataFrame) -> set[tuple[str, str]]:
    assert (violations["severity"] == "warn").all()
    return set(zip(violations["match_id"], violations["rule"], strict=True))


def test_real_fixtures_only_flag_the_ten_player_team(tables: Tables) -> None:
    violations = reconcile(tables)

    assert list(violations.columns) == VIOLATION_COLUMNS
    assert flagged(violations) == {("1452625", "starting_xi_has_11")}
    assert violations["failure_case"].item() == "Serbia: 10"


def test_target_must_be_first_innings_runs_plus_one(tables: Tables) -> None:
    i = tables["innings"]
    second = (i["match_id"] == "1082591") & (i["innings_number"] == 2)
    tables["innings"] = edit(i, second, "target_runs", 300)

    assert ("1082591", "target_is_first_innings_plus_one") in flagged(reconcile(tables))


def test_dls_targets_are_not_compared(tables: Tables) -> None:
    i = tables["innings"]
    second = (i["match_id"] == "1082648") & (i["innings_number"] == 2)
    tables["innings"] = edit(i, second, "target_runs", 300)

    assert "1082648" not in {m for m, _ in flagged(reconcile(tables))}


def test_runs_margin_must_match_the_scores(tables: Tables) -> None:
    m = tables["matches"]
    tables["matches"] = edit(m, m["match_id"] == "1082591", "win_by_runs", 36)

    assert ("1082591", "margin_matches_scores") in flagged(reconcile(tables))


def test_wickets_margin_winner_must_have_batted_second(tables: Tables) -> None:
    m = tables["matches"]
    # 1136564: Sunrisers chased and won by 9 wickets; credit the side that batted first
    tables["matches"] = edit(m, m["match_id"] == "1136564", "winner", "Rajasthan Royals")

    assert ("1136564", "margin_matches_scores") in flagged(reconcile(tables))


def test_more_than_ten_dismissals_in_an_innings(tables: Tables) -> None:
    w = tables["wickets"]
    one = w[w["match_id"] == "335982"].head(1)
    extra = pd.concat([one.assign(wicket_seq=k) for k in range(2, 12)], ignore_index=True)
    tables["wickets"] = pd.concat([w, extra], ignore_index=True)

    assert ("335982", "at_most_10_dismissals") in flagged(reconcile(tables))
