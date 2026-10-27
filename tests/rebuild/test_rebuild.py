from __future__ import annotations

from pathlib import Path

import pytest

from cricket_value.build.rebuild import Baseline, RebuildPaths, check_baseline, rebuild
from tests.ingest.test_silver import (
    NAMES_CSV,
    PEOPLE_CSV,
    fixture_members,
    stage_archive,
    stage_file,
)

REPO = Path(__file__).parents[2]


def stage_root(root: Path) -> RebuildPaths:
    raw = root / "data" / "raw"
    stage_archive(raw, "cricsheet_ipl", fixture_members("cricsheet_ipl"), "2026-10-26T07:00:00Z")
    stage_archive(
        raw, "cricsheet_t20i_male", fixture_members("cricsheet_t20i_male"), "2026-10-26T07:00:00Z"
    )
    stage_file(raw, "cricsheet_people", PEOPLE_CSV, "2026-10-26T07:00:00Z")
    stage_file(raw, "cricsheet_names", NAMES_CSV, "2026-10-26T07:00:00Z")
    return RebuildPaths(root=root, config=REPO / "config")


def test_two_independent_rebuilds_are_identical(tmp_path: Path) -> None:
    a = rebuild(stage_root(tmp_path / "a"), include_models=False)
    b = rebuild(stage_root(tmp_path / "b"), include_models=False)

    assert a.fingerprints == b.fingerprints
    assert a.snapshot_id == b.snapshot_id
    assert a.fingerprints["silver/matches"]["rows"] == 15
    assert "reports/data_quality.md" in a.fingerprints


def test_rebuild_removes_stale_derived_files(tmp_path: Path) -> None:
    paths = stage_root(tmp_path)
    stale = paths.silver / "matches" / "season=1999" / "part-0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"stale")

    rebuild(paths, include_models=False)

    assert not stale.exists()


def test_tampered_raw_file_stops_the_rebuild(tmp_path: Path) -> None:
    paths = stage_root(tmp_path)
    zip_path = next((paths.raw / "cricsheet_ipl").glob("*.zip"))
    zip_path.write_bytes(b"tampered")

    with pytest.raises(RuntimeError, match="raw files missing or altered"):
        rebuild(paths, include_models=False)


def test_baseline_is_stored_then_compared(tmp_path: Path) -> None:
    paths = stage_root(tmp_path)
    result = rebuild(paths, include_models=False)

    first = check_baseline(paths, result)
    second = check_baseline(paths, result)

    assert first == Baseline(status="created", differences=[])
    assert second == Baseline(status="matched", differences=[])

    changed = rebuild(paths, include_models=False)
    changed.fingerprints["silver/matches"] = {
        **changed.fingerprints["silver/matches"],
        "sha256": "x",
    }
    assert check_baseline(paths, changed).status == "mismatch"


def test_baseline_is_per_raw_snapshot(tmp_path: Path) -> None:
    paths = stage_root(tmp_path)
    result = rebuild(paths, include_models=False)
    assert check_baseline(paths, result).status == "created"

    result.snapshot_id = "different-raw-data"  # new raw data: a fresh baseline, not a mismatch

    assert check_baseline(paths, result).status == "created"
