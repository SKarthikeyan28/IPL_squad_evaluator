from __future__ import annotations

from pathlib import Path

from cricket_value.ingest.manifest import ArchiveRecord, FileRecord, Manifest


def archive(sha: str = "a" * 64, fetched_at: str = "2026-10-26T07:00:00Z") -> ArchiveRecord:
    return ArchiveRecord(
        source="cricsheet_ipl",
        url="https://example.test/ipl_json.zip",
        path="cricsheet_ipl/20261026T070000Z.zip",
        sha256=sha,
        n_bytes=10,
        fetched_at=fetched_at,
        last_modified=None,
    )


def file_rec(match_id: str, sha: str) -> FileRecord:
    return FileRecord(
        source="cricsheet_ipl",
        member=f"{match_id}.json",
        match_id=match_id,
        sha256=sha,
        archive_path="cricsheet_ipl/20261026T070000Z.zip",
        first_seen_at="2026-10-26T07:00:00Z",
    )


def test_empty_manifest_has_typed_empty_tables(tmp_path: Path) -> None:
    m = Manifest(tmp_path / "manifest")

    assert m.archives().empty
    assert "sha256" in m.archives().columns
    assert m.files().empty
    assert m.latest_archive("cricsheet_ipl") is None


def test_manifest_persists_across_instances(tmp_path: Path) -> None:
    Manifest(tmp_path / "m").add_archive(archive())

    reloaded = Manifest(tmp_path / "m")

    latest = reloaded.latest_archive("cricsheet_ipl")
    assert latest is not None
    assert latest.sha256 == "a" * 64
    assert latest.last_modified is None


def test_latest_archive_is_the_most_recent_fetch(tmp_path: Path) -> None:
    m = Manifest(tmp_path / "m")
    m.add_archive(archive("a" * 64, "2026-10-26T07:00:00Z"))
    m.add_archive(archive("b" * 64, "2026-10-27T07:00:00Z"))

    latest = m.latest_archive("cricsheet_ipl")
    assert latest is not None
    assert latest.sha256 == "b" * 64


def test_adding_the_same_file_versions_twice_is_idempotent(tmp_path: Path) -> None:
    m = Manifest(tmp_path / "m")
    recs = [file_rec("101", "x" * 64), file_rec("102", "y" * 64)]

    assert m.add_files(recs) == 2
    assert m.add_files(recs) == 0
    assert len(m.files()) == 2


def test_tables_are_written_sorted_for_stable_diffs(tmp_path: Path) -> None:
    m = Manifest(tmp_path / "m")
    m.add_files([file_rec("102", "y" * 64), file_rec("101", "x" * 64)])

    lines = (tmp_path / "m" / "files.csv").read_text().splitlines()
    assert lines[1].split(",")[2] == "101"
    assert lines[2].split(",")[2] == "102"


def test_snapshot_id_depends_only_on_file_versions(tmp_path: Path) -> None:
    a = Manifest(tmp_path / "a")
    b = Manifest(tmp_path / "b")
    a.add_files([file_rec("101", "x" * 64), file_rec("102", "y" * 64)])
    b.add_files([file_rec("102", "y" * 64), file_rec("101", "x" * 64)])

    assert a.snapshot_id() == b.snapshot_id()

    b.add_files([file_rec("102", "z" * 64)])
    assert a.snapshot_id() != b.snapshot_id()
