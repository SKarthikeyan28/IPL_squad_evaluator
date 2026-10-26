from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from cricket_value.ingest.download import Downloader, FetchStatus, load_sources
from cricket_value.ingest.manifest import Manifest
from tests.ingest.conftest import IPL, PEOPLE, FakeServer, make_zip

README = b"This zip archive contains data files from Cricsheet"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_downloader(
    tmp_path: Path, client: httpx.Client, clock: Callable[[], datetime]
) -> Downloader:
    raw = tmp_path / "raw"
    return Downloader(client=client, raw_dir=raw, manifest=Manifest(raw / "manifest"), clock=clock)


def test_first_fetch_stores_zip_unchanged_and_records_it(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    body = make_zip({"README.txt": README, "101.json": b'{"a":1}', "102.json": b'{"b":2}'})
    server.set(IPL.url, body, last_modified="Wed, 07 Oct 2026 17:05:16 GMT")
    dl = make_downloader(tmp_path, client, clock)

    result = dl.fetch(IPL)

    assert result.status is FetchStatus.NEW
    assert result.path is not None
    assert result.path == tmp_path / "raw" / "cricsheet_ipl" / "20261026T070000Z.zip"
    assert result.path.read_bytes() == body

    archives = dl.manifest.archives()
    assert len(archives) == 1
    row = archives.iloc[0]
    assert row["source"] == "cricsheet_ipl"
    assert row["url"] == IPL.url
    assert row["sha256"] == sha(body)
    assert row["n_bytes"] == len(body)
    assert row["fetched_at"] == "2026-10-26T07:00:00Z"
    assert row["last_modified"] == "Wed, 07 Oct 2026 17:05:16 GMT"
    assert row["path"] == "cricsheet_ipl/20261026T070000Z.zip"


def test_zip_members_are_recorded_per_match_and_non_json_is_ignored(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    server.set(IPL.url, make_zip({"README.txt": README, "101.json": b"x", "102.json": b"y"}))
    dl = make_downloader(tmp_path, client, clock)

    result = dl.fetch(IPL)

    files = dl.manifest.files().sort_values("match_id")
    assert files["match_id"].tolist() == ["101", "102"]
    assert files["sha256"].tolist() == [sha(b"x"), sha(b"y")]
    assert set(files["archive_path"]) == {"cricsheet_ipl/20261026T070000Z.zip"}
    assert set(files["first_seen_at"]) == {"2026-10-26T07:00:00Z"}
    assert (result.new_files, result.changed_files) == (2, 0)


def test_refetching_identical_bytes_stores_nothing_new(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    server.set(IPL.url, make_zip({"101.json": b"x"}))
    dl = make_downloader(tmp_path, client, clock)
    dl.fetch(IPL)

    second = dl.fetch(IPL)

    assert second.status is FetchStatus.UNCHANGED
    assert second.path is None
    assert len(list((tmp_path / "raw" / "cricsheet_ipl").iterdir())) == 1
    assert len(dl.manifest.archives()) == 1
    assert len(dl.manifest.files()) == 1


def test_sends_if_modified_since_and_skips_on_304(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    lm = "Wed, 07 Oct 2026 17:05:16 GMT"
    server.set(IPL.url, make_zip({"101.json": b"x"}), last_modified=lm)
    dl = make_downloader(tmp_path, client, clock)
    dl.fetch(IPL)

    second = dl.fetch(IPL)

    assert server.requests[0].headers.get("If-Modified-Since") is None
    assert server.requests[1].headers.get("If-Modified-Since") == lm
    assert second.status is FetchStatus.NOT_MODIFIED
    assert len(dl.manifest.archives()) == 1


def test_changed_archive_records_new_and_corrected_matches_only(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    server.set(IPL.url, make_zip({"101.json": b"x", "102.json": b"y"}))
    dl = make_downloader(tmp_path, client, clock)
    dl.fetch(IPL)

    # 101 unchanged, 102 corrected by Cricsheet, 103 is a new match
    server.set(IPL.url, make_zip({"101.json": b"x", "102.json": b"y-fixed", "103.json": b"z"}))
    second = dl.fetch(IPL)

    assert second.status is FetchStatus.NEW
    assert (second.new_files, second.changed_files) == (1, 1)
    files = dl.manifest.files()
    assert len(files) == 4
    versions = files.groupby("match_id")["sha256"].nunique().to_dict()
    assert versions == {"101": 1, "102": 2, "103": 1}
    corrected = files[(files["match_id"] == "102") & (files["sha256"] == sha(b"y-fixed"))]
    assert corrected["archive_path"].item() == "cricsheet_ipl/20261026T070100Z.zip"


def test_plain_file_source_is_stored_without_member_rows(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    body = b"identifier,name\nabc12345,V Kohli\n"
    server.set(PEOPLE.url, body)
    dl = make_downloader(tmp_path, client, clock)

    result = dl.fetch(PEOPLE)

    assert result.path == tmp_path / "raw" / "cricsheet_people" / "20261026T070000Z.csv"
    assert result.path.read_bytes() == body
    assert len(dl.manifest.files()) == 0


def test_http_error_raises_and_stores_nothing(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    server.set(IPL.url, b"")
    server.status_override[IPL.url] = 500
    dl = make_downloader(tmp_path, client, clock)

    with pytest.raises(httpx.HTTPStatusError):
        dl.fetch(IPL)

    assert not (tmp_path / "raw" / "cricsheet_ipl").exists()
    assert len(dl.manifest.archives()) == 0


def test_corrupt_zip_raises_and_stores_nothing(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    server.set(IPL.url, b"not a zip")
    dl = make_downloader(tmp_path, client, clock)

    with pytest.raises(ValueError, match="not a valid zip"):
        dl.fetch(IPL)

    assert len(dl.manifest.archives()) == 0
    assert not any((tmp_path / "raw" / "cricsheet_ipl").glob("*.zip"))


def test_verify_detects_tampered_raw_file(
    tmp_path: Path, server: FakeServer, client: httpx.Client, clock: Callable[[], datetime]
) -> None:
    server.set(IPL.url, make_zip({"101.json": b"x"}))
    dl = make_downloader(tmp_path, client, clock)
    result = dl.fetch(IPL)
    assert dl.verify() == []

    assert result.path is not None
    result.path.write_bytes(b"tampered")

    assert dl.verify() == ["cricsheet_ipl/20261026T070000Z.zip"]


def test_load_sources_reads_repo_config() -> None:
    sources = load_sources(Path(__file__).parents[2] / "config" / "sources.toml")

    by_name = {s.name: s for s in sources}
    assert by_name["cricsheet_ipl"].kind == "zip"
    assert by_name["cricsheet_people"].kind == "file"
    assert all(s.url.startswith("https://cricsheet.org/") for s in sources)
