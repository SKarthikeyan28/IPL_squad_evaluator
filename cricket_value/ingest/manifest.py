"""Manifest of raw files: what was downloaded, when, from where, and with which checksum.

Two tables, stored as sorted CSV under ``data/raw/manifest/``:

- ``archives.csv``: one row per stored download (a zip or a plain file).
- ``files.csv``: one row per *version* of each JSON member inside a zip. A match that
  Cricsheet later corrects gets a second row with a new checksum; the history is kept.

Both tables are append-only and deduplicated on their natural keys, so recording the
same thing twice is a no-op (idempotency).
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, dataclass, fields
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ArchiveRecord:
    source: str
    url: str
    path: str  # relative to the raw directory
    sha256: str
    n_bytes: int
    fetched_at: str  # ISO-8601 UTC, e.g. 2026-10-26T07:00:00Z
    last_modified: str | None  # HTTP Last-Modified header as received


@dataclass(frozen=True)
class FileRecord:
    source: str
    member: str  # file name inside the archive
    match_id: str
    sha256: str
    archive_path: str  # the archive this version was first seen in
    first_seen_at: str


ARCHIVE_COLUMNS = [f.name for f in fields(ArchiveRecord)]
FILE_COLUMNS = [f.name for f in fields(FileRecord)]
ARCHIVE_KEY = ["source", "sha256"]
FILE_KEY = ["source", "member", "sha256"]


class Manifest:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._archives_path = root / "archives.csv"
        self._files_path = root / "files.csv"

    # -- reads ---------------------------------------------------------------

    def archives(self) -> pd.DataFrame:
        return _read(self._archives_path, ARCHIVE_COLUMNS, int_columns=["n_bytes"])

    def files(self) -> pd.DataFrame:
        return _read(self._files_path, FILE_COLUMNS)

    def latest_archive(self, source: str) -> ArchiveRecord | None:
        df = self.archives()
        df = df[df["source"] == source]
        if df.empty:
            return None
        row = df.sort_values("fetched_at").iloc[-1]
        last_modified = row["last_modified"]
        return ArchiveRecord(
            source=str(row["source"]),
            url=str(row["url"]),
            path=str(row["path"]),
            sha256=str(row["sha256"]),
            n_bytes=int(row["n_bytes"]),
            fetched_at=str(row["fetched_at"]),
            last_modified=None if pd.isna(last_modified) else str(last_modified),
        )

    def known_file_versions(self, source: str) -> dict[str, set[str]]:
        """Map each member of ``source`` to the set of checksums already recorded for it."""
        df = self.files()
        df = df[df["source"] == source]
        return {str(member): set(g["sha256"]) for member, g in df.groupby("member")}

    def snapshot_id(self) -> str:
        """Hash identifying the raw data a build would use.

        Covers the latest version of every match file, plus the latest download of each
        source that has no per-file rows (plain files such as the people register).
        Independent of row order and of fetches that changed nothing.
        """
        files = self.files().sort_values("first_seen_at")
        latest = files.groupby(["source", "member"], as_index=False).last()
        parts = [f"{r.source}/{r.member}:{r.sha256}" for r in latest.itertuples()]

        archives = self.archives()
        plain = archives[~archives["source"].isin(files["source"])].sort_values("fetched_at")
        for r in plain.groupby("source", as_index=False).last().itertuples():
            parts.append(f"{r.source}:{r.sha256}")

        digest = hashlib.sha256("\n".join(sorted(parts)).encode())
        return digest.hexdigest()

    # -- writes --------------------------------------------------------------

    def add_archive(self, record: ArchiveRecord) -> bool:
        """Record a stored download. Returns False if it was already recorded."""
        return self._append(self._archives_path, self.archives(), [asdict(record)], ARCHIVE_KEY) > 0

    def add_files(self, records: list[FileRecord]) -> int:
        """Record member versions. Returns how many were new."""
        return self._append(self._files_path, self.files(), [asdict(r) for r in records], FILE_KEY)

    def _append(
        self, path: Path, existing: pd.DataFrame, rows: list[dict[str, object]], key: list[str]
    ) -> int:
        if not rows:
            return 0
        new = pd.DataFrame(rows, columns=existing.columns).drop_duplicates(subset=key)
        merged = new.merge(existing[key], on=key, how="left", indicator=True)
        new = new[(merged["_merge"] == "left_only").to_numpy()]
        if new.empty:
            return 0
        combined = pd.concat([existing, new], ignore_index=True) if not existing.empty else new
        _write_atomic(path, combined.sort_values(key, ignore_index=True))
        return len(new)


def _read(path: Path, columns: list[str], int_columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame({c: pd.Series(dtype="object") for c in columns})
    dtypes: dict[str, str] = {c: "object" for c in columns}
    for c in int_columns or []:
        dtypes[c] = "int64"
    return pd.read_csv(path, dtype=dtypes, keep_default_na=False, na_values={"last_modified": [""]})


def _write_atomic(path: Path, df: pd.DataFrame) -> None:
    """Write to a temp file and rename, so a crash never leaves a half-written manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)
