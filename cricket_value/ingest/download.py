"""Incremental, idempotent downloader for raw sources.

Every download that changed is stored unchanged as ``<raw_dir>/<source>/<fetched_at>.<ext>``
and recorded in the manifest. Unchanged downloads store nothing:

1. If the source sent ``Last-Modified`` last time, send ``If-Modified-Since``; a 304 means skip.
2. Otherwise compare the body's SHA-256 with the latest stored download; equal means skip.

Stored files are never overwritten, which keeps the raw layer immutable.
"""

from __future__ import annotations

import enum
import hashlib
import io
import os
import tomllib
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Literal

import httpx

from cricket_value.ingest.manifest import ArchiveRecord, FileRecord, Manifest

SourceKind = Literal["zip", "file"]


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    kind: SourceKind


class FetchStatus(enum.Enum):
    NEW = "new"
    UNCHANGED = "unchanged"
    NOT_MODIFIED = "not_modified"


@dataclass(frozen=True)
class FetchResult:
    source: str
    status: FetchStatus
    path: Path | None = None
    new_files: int = 0
    changed_files: int = 0


def load_sources(path: Path) -> list[Source]:
    with path.open("rb") as f:
        config = tomllib.load(f)
    return [Source(name=s["name"], url=s["url"], kind=s["kind"]) for s in config["source"]]


def utc_now() -> datetime:
    return datetime.now(UTC)


class Downloader:
    def __init__(
        self,
        client: httpx.Client,
        raw_dir: Path,
        manifest: Manifest,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.client = client
        self.raw_dir = raw_dir
        self.manifest = manifest
        self.clock = clock

    def fetch(self, source: Source) -> FetchResult:
        previous = self.manifest.latest_archive(source.name)
        headers = {}
        if previous is not None and previous.last_modified:
            headers["If-Modified-Since"] = previous.last_modified

        response = self.client.get(source.url, headers=headers, follow_redirects=True)
        if response.status_code == 304:
            return FetchResult(source.name, FetchStatus.NOT_MODIFIED)
        response.raise_for_status()

        body = response.content
        digest = hashlib.sha256(body).hexdigest()
        if previous is not None and previous.sha256 == digest:
            return FetchResult(source.name, FetchStatus.UNCHANGED)

        members = _hash_json_members(body) if source.kind == "zip" else {}

        fetched_at = self.clock().astimezone(UTC)
        ext = PurePosixPath(source.url).suffix or ".bin"
        rel_path = f"{source.name}/{fetched_at:%Y%m%dT%H%M%SZ}{ext}"
        path = self.raw_dir / rel_path
        _write_new_file(path, body)

        fetched_iso = fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        new_files, changed_files = self._record_members(source, members, rel_path, fetched_iso)
        self.manifest.add_archive(
            ArchiveRecord(
                source=source.name,
                url=source.url,
                path=rel_path,
                sha256=digest,
                n_bytes=len(body),
                fetched_at=fetched_iso,
                last_modified=response.headers.get("Last-Modified"),
            )
        )
        return FetchResult(source.name, FetchStatus.NEW, path, new_files, changed_files)

    def verify(self) -> list[str]:
        """Re-hash every stored download. Returns the paths that are missing or altered."""
        bad = []
        for row in self.manifest.archives().itertuples():
            path = self.raw_dir / str(row.path)
            if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != row.sha256:
                bad.append(str(row.path))
        return bad

    def _record_members(
        self, source: Source, members: dict[str, str], archive_path: str, fetched_at: str
    ) -> tuple[int, int]:
        known = self.manifest.known_file_versions(source.name)
        records = []
        new_files = changed_files = 0
        for member, digest in members.items():
            seen = known.get(member)
            if seen is not None and digest in seen:
                continue
            if seen is None:
                new_files += 1
            else:
                changed_files += 1
            records.append(
                FileRecord(
                    source=source.name,
                    member=member,
                    match_id=PurePosixPath(member).stem,
                    sha256=digest,
                    archive_path=archive_path,
                    first_seen_at=fetched_at,
                )
            )
        self.manifest.add_files(records)
        return new_files, changed_files


def _hash_json_members(body: bytes) -> dict[str, str]:
    """SHA-256 of each JSON member, read in memory without extracting to disk."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as e:
        raise ValueError("download is not a valid zip archive") from e
    with zf:
        return {
            info.filename: hashlib.sha256(zf.read(info)).hexdigest()
            for info in zf.infolist()
            if info.filename.endswith(".json") and not info.is_dir()
        }


def _write_new_file(path: Path, body: bytes) -> None:
    """Write atomically, refusing to overwrite: raw files are immutable."""
    if path.exists():
        raise FileExistsError(f"raw file already exists, refusing to overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_bytes(body)
    os.replace(tmp, path)
