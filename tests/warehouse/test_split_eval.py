"""The pace/spin comparison's warehouse queries, on the fixture warehouse where every
player is labelled pace or spin."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import yaml

from cricket_value.ratings.split_eval import (
    DEFAULT,
    Setting,
    compare,
    complete_cases,
    load_split_balls,
    paired_difference,
    run_sweep,
)
from cricket_value.ratings.tuning import HISTORY_SQL, league_runs_distribution
from cricket_value.warehouse.dbt import REPO
from tests.warehouse.conftest import FIXTURE_VARS, query

SEASONS = range(2017, 2024)


def test_every_ball_against_a_labelled_bowler_is_loaded_once(split_warehouse: Path) -> None:
    balls = load_split_balls(split_warehouse, SEASONS)  # raises if a join changed the count

    faced = query(
        split_warehouse,
        "select count(*) as n from fact_delivery where competition = 'ipl' "
        "and not is_super_over and extras_wides = 0 and season between 2017 and 2023",
    )
    assert len(balls) == faced["n"].iloc[0] > 0  # everyone is labelled
    assert set(balls["vs"]) == {"pace", "spin"}


def test_the_split_rating_is_the_one_for_the_bowlers_kind(split_warehouse: Path) -> None:
    balls = complete_cases(load_split_balls(split_warehouse, SEASONS))
    assert len(balls) > 0

    # a batter's runs rating against pace is not his rating against spin, so a ball's split
    # rating differs from the unsplit one for at least some balls
    assert not np.allclose(balls["split_runs"], balls["bat_runs"])
    assert balls["split_runs"].notna().all()


def test_the_comparison_runs_end_to_end(split_warehouse: Path) -> None:
    balls = complete_cases(load_split_balls(split_warehouse, SEASONS))
    history = query(split_warehouse, HISTORY_SQL.format(last=2023))
    league = league_runs_distribution(history, SEASONS)

    totals = compare(balls, league)
    diff = paired_difference(totals, n_resamples=100)

    assert totals["balls"].sum() == len(balls)
    assert np.isfinite(totals[["ll_dismissal_split", "ll_runs_split"]].to_numpy()).all()
    assert diff["total"].low <= diff["total"].diff <= diff["total"].high


def test_locked_seasons_are_refused(split_warehouse: Path) -> None:
    with pytest.raises(ValueError, match="locked"):
        load_split_balls(split_warehouse, range(2020, 2026))


def test_the_evaluations_default_setting_is_the_dbt_default() -> None:
    config = yaml.safe_load((REPO / "dbt" / "dbt_project.yml").read_text())["vars"]

    assert DEFAULT.shrink_to == config["rating_split_shrink_to"]
    assert DEFAULT.multiplier == config["rating_split_prior_multiplier"]


def test_the_sweep_rebuilds_the_ratings_for_each_prior_setting(
    split_silver: Path, tmp_path: Path
) -> None:
    settings = [Setting("role", 1), DEFAULT]

    result = run_sweep(
        split_silver,
        tmp_path,
        settings,
        seasons=SEASONS,
        n_resamples=50,
        extra_vars=FIXTURE_VARS,
    )

    assert set(result.sweep) == set(settings)
    assert set(result.slices) >= {"all balls", "against pace", "against spin"}
    role, overall = (result.sweep[s]["total"].diff for s in settings)
    assert np.isfinite([role, overall]).all()
    assert role != overall  # the two priors give different ratings
