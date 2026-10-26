from __future__ import annotations

import json
from pathlib import Path

import pytest

from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.silver import build_silver
from cricket_value.quality.report import render_markdown, season_summary
from tests.ingest.test_silver import COMPETITIONS, FIXTURES, fixture_members, stage_archive


@pytest.fixture
def silver(tmp_path: Path) -> Path:
    """Silver built from the fixtures, with one IPL 2017 match broken so it is quarantined."""
    raw = tmp_path / "raw"
    doc = json.loads((FIXTURES / "ipl" / "1082591.json").read_text())
    doc["info"]["toss"]["winner"] = "Somebody Else"
    ipl = {**fixture_members("cricsheet_ipl"), "1082591.json": json.dumps(doc).encode()}
    stage_archive(raw, "cricsheet_ipl", ipl, "2026-10-26T07:00:00Z")
    stage_archive(
        raw, "cricsheet_t20i_male", fixture_members("cricsheet_t20i_male"), "2026-10-26T07:00:00Z"
    )
    out = tmp_path / "silver"
    build_silver(raw, Manifest(raw / "manifest"), out, COMPETITIONS)
    return out


def test_season_summary_counts_clean_and_quarantined_matches(silver: Path) -> None:
    s = season_summary(silver)

    def cell(season: int, competition: str, column: str) -> int:
        row = s[(s["season"] == season) & (s["competition"] == competition)]
        return int(row[column].item())

    assert cell(2017, "ipl", "matches") == 2  # three fixtures, one quarantined
    assert cell(2017, "ipl", "quarantined") == 1
    assert cell(2017, "ipl", "deliveries") > 0
    assert cell(2017, "ipl", "errors") == 1
    assert cell(2024, "t20i", "warnings") == 1  # the 10-player Serbia side
    assert int(s["matches"].sum()) == 14


def test_markdown_report_lists_seasons_and_reasons(silver: Path) -> None:
    md = render_markdown(silver, snapshot_id="abc123")

    assert md.startswith("# Data quality report")
    assert "abc123" in md
    assert "| 2017 | ipl | 2 | 1 |" in md
    assert "matches.toss_winner_is_a_team" in md
    assert "starting_xi_has_11" in md
    assert "Serbia: 10" in md
