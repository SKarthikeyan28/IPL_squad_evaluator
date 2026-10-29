from __future__ import annotations

from pathlib import Path

import duckdb

from cricket_value.attributes.audit import audit, render_markdown


def test_audit_measures_coverage_and_queues_gaps_by_balls(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    with duckdb.connect(str(db)) as con:
        result = audit(con)

    coverage = result.coverage.set_index("attribute")
    assert set(coverage.index) == {
        "date_of_birth",
        "nationality",
        "batting_hand",
        "bowling_kind",
        "bowling_arm",
    }
    assert (coverage["players_with"] <= coverage["players"]).all()
    assert coverage["ball_share"].between(0, 1).all()
    # Dhoni's gaps are filled by overrides, so he isn't queued; players with no source are
    queue = result.hand_check_queue
    assert "MS Dhoni" not in set(queue["player_name"])
    assert not queue.empty
    assert queue["balls"].is_monotonic_decreasing


def test_report_has_every_section(warehouse: tuple[Path, Path]) -> None:
    _, db = warehouse
    with duckdb.connect(str(db)) as con:
        text = render_markdown(audit(con))

    for heading in (
        "# Player attributes audit",
        "## Coverage",
        "## Where nationality came from",
        "## Hand-check queue",
    ):
        assert heading in text
