from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from cricket_value.ingest.manifest import ArchiveRecord, FileRecord, Manifest
from cricket_value.ingest.schemas import SILVER_TABLES
from cricket_value.ingest.silver import build_registers, build_silver

FIXTURES = Path(__file__).parents[1] / "fixtures" / "cricsheet"
COMPETITIONS = {"cricsheet_ipl": "ipl", "cricsheet_t20i_male": "t20i"}
SOURCE_DIRS = {"cricsheet_ipl": "ipl", "cricsheet_t20i_male": "t20i"}


def stage_archive(raw: Path, source: str, members: dict[str, bytes], fetched_at: str) -> None:
    """Write a zip into the raw layer and record it in the manifest, as the downloader would."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    body = buf.getvalue()
    rel = f"{source}/{fetched_at.replace('-', '').replace(':', '')}.zip"
    (raw / rel).parent.mkdir(parents=True, exist_ok=True)
    (raw / rel).write_bytes(body)

    manifest = Manifest(raw / "manifest")
    known = manifest.known_file_versions(source)
    manifest.add_files(
        [
            FileRecord(
                source=source,
                member=name,
                match_id=name.removesuffix(".json"),
                sha256=hashlib.sha256(data).hexdigest(),
                archive_path=rel,
                first_seen_at=fetched_at,
            )
            for name, data in members.items()
            if hashlib.sha256(data).hexdigest() not in known.get(name, set())
        ]
    )
    manifest.add_archive(
        ArchiveRecord(
            source=source,
            url=f"https://example.test/{source}.zip",
            path=rel,
            sha256=hashlib.sha256(body).hexdigest(),
            n_bytes=len(body),
            fetched_at=fetched_at,
            last_modified=None,
        )
    )


def fixture_members(source: str) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted((FIXTURES / SOURCE_DIRS[source]).glob("*.json"))}


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    for source in COMPETITIONS:
        stage_archive(raw, source, fixture_members(source), "2026-10-26T07:00:00Z")
    return raw


def build(raw: Path, out: Path) -> dict[str, int]:
    result = build_silver(raw, Manifest(raw / "manifest"), out, COMPETITIONS)
    return result.row_counts


def read(out: Path, table: str) -> pd.DataFrame:
    df = pd.read_parquet(out / table)
    df["season"] = df["season"].astype(int)
    keys = ["season", *SILVER_TABLES[table][1]]
    return df.sort_values(keys).reset_index(drop=True)


def test_tables_are_partitioned_by_season(raw: Path, tmp_path: Path) -> None:
    out = tmp_path / "silver"
    counts = build(raw, out)

    matches = read(out, "matches")
    assert len(matches) == counts["matches"] == 15
    assert sorted(matches["season"].unique()) == [
        2006,
        2008,
        2017,
        2018,
        2019,
        2021,
        2023,
        2024,
        2026,
    ]
    assert (out / "matches" / "season=2008" / "part-0.parquet").exists()
    assert set(matches.loc[matches["match_id"] == "335982", "season"]) == {2008}

    deliveries = read(out, "deliveries")
    assert len(deliveries) == counts["deliveries"]
    # child rows land in the same season partition as their match
    season_of = dict(zip(matches["match_id"], matches["season"], strict=True))
    assert (deliveries["match_id"].map(season_of) == deliveries["season"]).all()


def test_files_have_the_declared_schema(raw: Path, tmp_path: Path) -> None:
    out = tmp_path / "silver"
    build(raw, out)

    for table, (schema, _) in SILVER_TABLES.items():
        for part in (out / table).glob("season=*/part-0.parquet"):
            assert pq.read_schema(part).remove_metadata() == schema, (table, part)


def test_rebuild_is_idempotent(raw: Path, tmp_path: Path) -> None:
    out = tmp_path / "silver"
    first_counts = build(raw, out)
    first = {t: read(out, t) for t in SILVER_TABLES}
    first_files = sorted(p.relative_to(out) for p in out.rglob("*.parquet"))

    second_counts = build(raw, out)

    assert second_counts == first_counts
    assert sorted(p.relative_to(out) for p in out.rglob("*.parquet")) == first_files
    for table in SILVER_TABLES:
        pd.testing.assert_frame_equal(read(out, table), first[table])


def test_latest_version_of_a_corrected_match_wins(raw: Path, tmp_path: Path) -> None:
    doc = json.loads((FIXTURES / "ipl" / "1082591.json").read_text())
    doc["info"]["city"] = "Hyderabad (corrected)"
    corrected = json.dumps(doc).encode()
    stage_archive(
        raw,
        "cricsheet_ipl",
        {**fixture_members("cricsheet_ipl"), "1082591.json": corrected},
        "2026-10-27T07:00:00Z",
    )
    out = tmp_path / "silver"

    build(raw, out)

    row = read(out, "matches").query("match_id == '1082591'")
    assert len(row) == 1
    assert row["city"].item() == "Hyderabad (corrected)"
    assert row["file_sha256"].item() == hashlib.sha256(corrected).hexdigest()


def test_unparseable_file_is_recorded_and_does_not_stop_the_build(
    raw: Path, tmp_path: Path
) -> None:
    doc = json.loads((FIXTURES / "ipl" / "1082591.json").read_text())
    doc["meta"]["data_version"] = "9.0.0"
    stage_archive(
        raw,
        "cricsheet_ipl",
        {**fixture_members("cricsheet_ipl"), "1082591.json": json.dumps(doc).encode()},
        "2026-10-27T07:00:00Z",
    )
    out = tmp_path / "silver"

    result = build_silver(raw, Manifest(raw / "manifest"), out, COMPETITIONS)

    assert [(f.member, f.error_type) for f in result.failures] == [
        ("1082591.json", "UnsupportedSchemaVersion")
    ]
    assert "9.0.0" in result.failures[0].reason
    assert "1082591" not in set(read(out, "matches")["match_id"])
    assert result.row_counts["matches"] == 14
    failures = pd.read_parquet(out / "_parse_failures.parquet")
    assert failures["member"].tolist() == ["1082591.json"]


def test_rebuild_removes_stale_partitions(raw: Path, tmp_path: Path) -> None:
    out = tmp_path / "silver"
    stale = out / "matches" / "season=1999" / "part-0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"stale")

    build(raw, out)

    assert not stale.exists()


PEOPLE_CSV = (
    b"identifier,name,unique_name,key_bcci,key_cricinfo,key_cricinfo_2\n"
    b"ba607b88,V Kohli,V Kohli,164,253802,\n"
    b"462411b3,MS Dhoni,MS Dhoni,1,28081,\n"
)
NAMES_CSV = b"identifier,name\nba607b88,V Kohli\nba607b88,Virat Kohli\n462411b3,MS Dhoni\n"


def stage_file(raw: Path, source: str, body: bytes, fetched_at: str) -> None:
    rel = f"{source}/{fetched_at.replace('-', '').replace(':', '')}.csv"
    (raw / rel).parent.mkdir(parents=True, exist_ok=True)
    (raw / rel).write_bytes(body)
    Manifest(raw / "manifest").add_archive(
        ArchiveRecord(
            source=source,
            url=f"https://example.test/{source}.csv",
            path=rel,
            sha256=hashlib.sha256(body).hexdigest(),
            n_bytes=len(body),
            fetched_at=fetched_at,
            last_modified=None,
        )
    )


def test_registers_become_players_and_name_variants(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    stage_file(raw, "cricsheet_people", PEOPLE_CSV, "2026-10-26T07:00:00Z")
    stage_file(raw, "cricsheet_names", NAMES_CSV, "2026-10-26T07:00:00Z")
    out = tmp_path / "silver"

    counts = build_registers(raw, Manifest(raw / "manifest"), out)

    players = pd.read_parquet(out / "players").sort_values("player_id")
    assert counts == {"players": 2, "player_name_variants": 3}
    assert players["player_id"].tolist() == ["462411b3", "ba607b88"]
    kohli = players[players["player_id"] == "ba607b88"].iloc[0]
    assert kohli["key_cricinfo"] == "253802"  # IDs stay strings, never 253802.0
    assert pd.isna(kohli["key_cricinfo_2"])
    variants = pd.read_parquet(out / "player_name_variants")
    assert sorted(variants.loc[variants["player_id"] == "ba607b88", "name"]) == [
        "V Kohli",
        "Virat Kohli",
    ]


def test_registers_use_latest_download_and_check_its_checksum(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    stage_file(raw, "cricsheet_people", PEOPLE_CSV, "2026-10-26T07:00:00Z")
    newer = PEOPLE_CSV + b"aaaaaaaa,New Player,New Player,,999,\n"
    stage_file(raw, "cricsheet_people", newer, "2026-10-27T07:00:00Z")
    stage_file(raw, "cricsheet_names", NAMES_CSV, "2026-10-26T07:00:00Z")
    out = tmp_path / "silver"

    assert build_registers(raw, Manifest(raw / "manifest"), out)["players"] == 3

    (raw / "cricsheet_people" / "20261027T070000Z.csv").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum mismatch"):
        build_registers(raw, Manifest(raw / "manifest"), out)


def test_contract_errors_quarantine_the_match_and_are_logged(raw: Path, tmp_path: Path) -> None:
    doc = json.loads((FIXTURES / "ipl" / "1082591.json").read_text())
    doc["info"]["toss"]["winner"] = "Somebody Else"
    stage_archive(
        raw,
        "cricsheet_ipl",
        {**fixture_members("cricsheet_ipl"), "1082591.json": json.dumps(doc).encode()},
        "2026-10-27T07:00:00Z",
    )
    out = tmp_path / "silver"

    result = build_silver(raw, Manifest(raw / "manifest"), out, COMPETITIONS)

    assert result.quarantined_matches == 1
    for table in SILVER_TABLES:
        assert "1082591" not in set(read(out, table)["match_id"]), table
    q = pd.read_parquet(out / "_quarantine" / "matches.parquet")
    assert q["match_id"].tolist() == ["1082591"]
    assert q["quarantine_rules"].item() == "matches.toss_winner_is_a_team"
    assert len(pd.read_parquet(out / "_quarantine" / "deliveries.parquet")) == 248

    violations = pd.read_parquet(out / "_violations.parquet")
    errors = violations[violations["severity"] == "error"]
    assert set(zip(errors["match_id"], errors["rule"], strict=True)) == {
        ("1082591", "toss_winner_is_a_team")
    }
    # the real 10-player Serbia side is flagged, not quarantined
    warn = violations[violations["severity"] == "warn"]
    assert ("1452625", "starting_xi_has_11") in set(
        zip(warn["match_id"], warn["rule"], strict=True)
    )
