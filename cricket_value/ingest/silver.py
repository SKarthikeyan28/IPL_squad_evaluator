"""Build the staging (silver) tables from the raw layer, one season at a time.

Two steps:

1. **Match index** (``build_match_index``): for the latest version of every match file in
   the manifest, check its checksum and read its date, giving ``match file -> season``.
   Files that can't be read at all are recorded as index failures.
2. **Season build** (``build_season``): parse that season's files, check them against the
   data contracts and reconciliation rules, quarantine broken matches, and write
   ``<table>/season=YYYY/part-0.parquet`` for every table.

A season is self-contained: parsing, checking and writing one season never reads
another. That is what lets Dagster rebuild or backfill a single season partition.
``build_silver`` runs both steps for every season (``make silver``), so the command
line and Dagster share one code path.

Writes are idempotent: each partition file is written to a temp file and renamed over
the old one, so a rerun replaces it (no duplicates) and a crash leaves the old file.

Layout under the silver directory:

- ``<table>/season=YYYY/part-0.parquet``: clean rows
- ``_quarantine/<table>/season=YYYY/part-0.parquet``: rows of quarantined matches
- ``_violations/season=YYYY/part-0.parquet``: every contract and reconciliation finding
- ``_parse_failures/season=YYYY/part-0.parquet``: files that failed to parse
- ``_match_index/part-0.parquet`` and ``_index_failures/part-0.parquet``
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
import zipfile
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.parse import ParsedMatch, ParseError, parse_match
from cricket_value.ingest.schemas import PARSE_FAILURES, SILVER_TABLES
from cricket_value.quality.contracts import (
    apply_quarantine,
    check_contracts,
    empty_violations,
    violations_table,
)
from cricket_value.quality.reconciliation import reconcile

MATCH_INDEX = pa.schema(
    [
        ("source", pa.string()),
        ("member", pa.string()),
        ("match_id", pa.string()),
        ("sha256", pa.string()),
        ("archive_path", pa.string()),
        ("competition", pa.string()),
        ("season", pa.int64()),
    ]
)

QUARANTINE_DIR = "_quarantine"
VIOLATIONS_DIR = "_violations"
PARSE_FAILURES_DIR = "_parse_failures"
INDEX_DIR = "_match_index"
INDEX_FAILURES_DIR = "_index_failures"


@dataclass(frozen=True)
class ParseFailure:
    source: str
    member: str
    sha256: str
    archive_path: str
    error_type: str
    reason: str


@dataclass
class SilverBuild:
    """What a build produced: clean row counts, failures and quality findings."""

    row_counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(SILVER_TABLES, 0))
    failures: list[ParseFailure] = field(default_factory=list)
    quarantined_matches: int = 0
    violations: pd.DataFrame = field(default_factory=empty_violations)

    def add(self, other: SilverBuild) -> None:
        for table, n in other.row_counts.items():
            self.row_counts[table] = self.row_counts.get(table, 0) + n
        self.failures.extend(other.failures)
        self.quarantined_matches += other.quarantined_matches
        frames = [v for v in (self.violations, other.violations) if not v.empty]
        if frames:
            self.violations = pd.concat(frames, ignore_index=True)


# -- step 1: match index --------------------------------------------------------------


def latest_versions(manifest: Manifest, sources: list[str]) -> pd.DataFrame:
    """The most recently seen version of each match file in ``sources``."""
    files = manifest.files()
    files = files[files["source"].isin(sources)].sort_values("first_seen_at")
    return files.groupby(["source", "member"], as_index=False).last()


def build_match_index(
    raw_dir: Path, manifest: Manifest, competitions: Mapping[str, str]
) -> tuple[pd.DataFrame, list[ParseFailure]]:
    """Map the latest version of every match file to its season (year of its first date)."""
    rows: list[dict[str, Any]] = []
    failures: list[ParseFailure] = []
    versions = latest_versions(manifest, list(competitions))
    for archive_path, group in versions.groupby("archive_path"):
        with zipfile.ZipFile(raw_dir / str(archive_path)) as zf:
            for f in group.itertuples():
                source, member, sha256 = str(f.source), str(f.member), str(f.sha256)
                try:
                    doc = _read_member(zf, member, sha256)
                    season = min(dt.date.fromisoformat(d) for d in doc["info"]["dates"]).year
                except (ParseError, KeyError, TypeError, ValueError) as e:
                    failures.append(_failure(source, member, sha256, str(archive_path), e))
                    continue
                rows.append(
                    {
                        "source": source,
                        "member": member,
                        "match_id": member.removesuffix(".json"),
                        "sha256": sha256,
                        "archive_path": str(archive_path),
                        "competition": competitions[source],
                        "season": season,
                    }
                )
    index = pa.Table.from_pylist(rows, schema=MATCH_INDEX).to_pandas()
    return index.sort_values(["season", "source", "member"], ignore_index=True), failures


def write_match_index(out_dir: Path, index: pd.DataFrame, failures: list[ParseFailure]) -> None:
    _write_file(out_dir / INDEX_DIR / "part-0.parquet", pa.Table.from_pandas(index, MATCH_INDEX))
    _write_file(out_dir / INDEX_FAILURES_DIR / "part-0.parquet", _failures_table(failures))


def read_match_index(out_dir: Path) -> pd.DataFrame:
    return pd.read_parquet(out_dir / INDEX_DIR / "part-0.parquet")


# -- step 2: one season ---------------------------------------------------------------


def build_season(raw_dir: Path, index: pd.DataFrame, season: int, out_dir: Path) -> SilverBuild:
    """Parse, check and write one season. A season with no matches writes empty files,
    so stale rows from an earlier run can never survive."""
    result = SilverBuild()
    parsed: list[ParsedMatch] = []
    files = index[index["season"] == season]
    for archive_path, group in files.groupby("archive_path"):
        with zipfile.ZipFile(raw_dir / str(archive_path)) as zf:
            for f in group.itertuples():
                source, member, sha256 = str(f.source), str(f.member), str(f.sha256)
                try:
                    doc = _read_member(zf, member, sha256)
                    parsed.append(
                        parse_match(
                            doc,
                            match_id=str(f.match_id),
                            competition=str(f.competition),
                            file_sha256=sha256,
                        )
                    )
                except ParseError as e:
                    result.failures.append(_failure(source, member, sha256, str(archive_path), e))

    tables = to_frames(parsed)
    violations = pd.concat([check_contracts(tables), reconcile(tables)], ignore_index=True)
    clean, quarantined = apply_quarantine(tables, violations)

    for table, (schema, _) in SILVER_TABLES.items():
        _write_partition(out_dir / table, season, clean[table], schema)
        q_schema = schema.append(pa.field("quarantine_rules", pa.string()))
        _write_partition(out_dir / QUARANTINE_DIR / table, season, quarantined[table], q_schema)
        result.row_counts[table] = len(clean[table])
    # season is the partition folder, so it is not stored inside the file
    found = violations_table(violations).drop_columns(["season"])
    _write_file(_partition_path(out_dir / VIOLATIONS_DIR, season), found)
    failures = sorted(result.failures, key=lambda f: (f.source, f.member))
    _write_file(_partition_path(out_dir / PARSE_FAILURES_DIR, season), _failures_table(failures))

    result.quarantined_matches = len(quarantined["matches"])
    result.violations = violations
    return result


# -- everything -----------------------------------------------------------------------


def build_silver(
    raw_dir: Path, manifest: Manifest, out_dir: Path, competitions: Mapping[str, str]
) -> SilverBuild:
    """Rebuild every season, then delete partitions of seasons that no longer exist."""
    index, index_failures = build_match_index(raw_dir, manifest, competitions)
    write_match_index(out_dir, index, index_failures)

    result = SilverBuild(failures=list(index_failures))
    seasons = sorted(int(s) for s in index["season"].unique())
    for season in seasons:
        result.add(build_season(raw_dir, index, season, out_dir))
    remove_stale_seasons(out_dir, seasons)
    return result


def season_dirs(out_dir: Path) -> list[Path]:
    """Every directory that holds season=YYYY partitions."""
    dirs = [out_dir / t for t in SILVER_TABLES]
    dirs += [out_dir / QUARANTINE_DIR / t for t in SILVER_TABLES]
    dirs += [out_dir / VIOLATIONS_DIR, out_dir / PARSE_FAILURES_DIR]
    return dirs


def remove_stale_seasons(out_dir: Path, seasons: list[int]) -> None:
    keep = {f"season={s}" for s in seasons}
    for d in season_dirs(out_dir):
        for part in d.glob("season=*"):
            if part.name not in keep:
                shutil.rmtree(part)


def to_frames(parsed: list[ParsedMatch]) -> dict[str, pd.DataFrame]:
    """Parsed matches -> one typed DataFrame per silver table, with a ``season`` column,
    sorted by season and the table's key."""
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    season_of: dict[str, int] = {}
    for p in parsed:
        season_of[p.match["match_id"]] = p.match["season"]
        rows["matches"].append(p.match)
        for table in SILVER_TABLES:
            if table != "matches":
                rows[table].extend(getattr(p, table))

    frames = {}
    for table, (schema, sort_key) in SILVER_TABLES.items():
        df = _to_frame(rows[table], schema)
        df["season"] = df["match_id"].map(season_of).astype("int64")
        frames[table] = df.sort_values(["season", *sort_key], ignore_index=True)
    return frames


# -- helpers --------------------------------------------------------------------------


def _read_member(zf: zipfile.ZipFile, member: str, sha256: str) -> dict[str, Any]:
    data = zf.read(member)
    actual = hashlib.sha256(data).hexdigest()
    if actual != sha256:
        raise ParseError(f"checksum mismatch: manifest {sha256}, file {actual}")
    try:
        doc: dict[str, Any] = json.loads(data)
    except json.JSONDecodeError as e:
        raise ParseError(f"invalid JSON: {e}") from e
    return doc


def _failure(
    source: str, member: str, sha256: str, archive_path: str, e: Exception
) -> ParseFailure:
    return ParseFailure(
        source=source,
        member=member,
        sha256=sha256,
        archive_path=archive_path,
        error_type=type(e).__name__,
        reason=str(e),
    )


def _failures_table(failures: list[ParseFailure]) -> pa.Table:
    return pa.Table.from_pylist([asdict(f) for f in failures], schema=PARSE_FAILURES)


def _to_frame(rows: list[dict[str, Any]], schema: pa.Schema) -> pd.DataFrame:
    """Rows -> Arrow-backed DataFrame. Going through Arrow applies the declared types,
    so nullable integers stay integers instead of becoming float NaN."""
    table = pa.Table.from_pylist(rows, schema=schema)
    df: pd.DataFrame = table.to_pandas(types_mapper=pd.ArrowDtype)
    return df


def _partition_path(table_dir: Path, season: int) -> Path:
    return table_dir / f"season={season}" / "part-0.parquet"


def _write_partition(table_dir: Path, season: int, df: pd.DataFrame, schema: pa.Schema) -> None:
    """Write one season of a table. ``season`` lives in the folder name, not the file."""
    arrow = pa.Table.from_pandas(df[schema.names], schema=schema, preserve_index=False)
    _write_file(_partition_path(table_dir, season), arrow)


def _write_file(path: Path, table: pa.Table) -> None:
    """Atomic write. The pandas-specific metadata is dropped so files stay engine-neutral
    (DuckDB and dbt read them)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    pq.write_table(table.replace_schema_metadata(None), tmp)
    os.replace(tmp, path)


# -- registers ------------------------------------------------------------------------

REGISTERS = {"players": "cricsheet_people", "player_name_variants": "cricsheet_names"}


def build_register(raw_dir: Path, manifest: Manifest, out_dir: Path, table: str) -> int:
    """Stage one Cricsheet register (people or names) from its latest download.

    Not partitioned: a person is not tied to a season. Every column is read as a string,
    so IDs such as ``key_cricinfo`` never turn into floats. Cricsheet adds ``key_*``
    columns as it links new sites, so the column set is taken from the file.
    """
    source = REGISTERS[table]
    latest = manifest.latest_archive(source)
    if latest is None:
        raise FileNotFoundError(f"no download of {source} in the manifest")
    path = raw_dir / latest.path
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != latest.sha256:
        raise ValueError(f"checksum mismatch for {latest.path}: file {actual}")
    df = pd.read_csv(path, dtype="string", keep_default_na=False, na_values=[""])
    df = df.rename(columns={"identifier": "player_id"})
    df = df.sort_values(["player_id", "name"], ignore_index=True)
    _write_file(out_dir / table / "part-0.parquet", pa.Table.from_pandas(df, preserve_index=False))
    return len(df)


def build_registers(raw_dir: Path, manifest: Manifest, out_dir: Path) -> dict[str, int]:
    return {t: build_register(raw_dir, manifest, out_dir, t) for t in REGISTERS}


def competitions_from_config(sources_path: Path) -> dict[str, str]:
    from cricket_value.ingest.download import load_sources

    return {s.name: s.competition for s in load_sources(sources_path) if s.competition}


def main() -> None:
    repo = Path(__file__).resolve().parents[2]
    raw_dir = repo / "data" / "raw"
    out_dir = repo / "data" / "silver"
    manifest = Manifest(raw_dir / "manifest")
    competitions = competitions_from_config(repo / "config" / "sources.toml")
    result = build_silver(raw_dir, manifest, out_dir, competitions)
    result.row_counts.update(build_registers(raw_dir, manifest, out_dir))
    for table, n in result.row_counts.items():
        print(f"{table:<20} {n:>10,} rows")
    print(f"{'parse failures':<20} {len(result.failures):>10,}")
    print(f"{'quarantined matches':<20} {result.quarantined_matches:>10,}")
    counts = result.violations.groupby(["severity", "table", "rule"]).size().reset_index()
    for row in counts.itertuples(index=False):
        print(f"  {row[0]:<5} {row[1]}.{row[2]}: {row[3]:,}")
    for f in result.failures:
        print(f"  {f.source}/{f.member}: {f.error_type}: {f.reason}")


if __name__ == "__main__":
    main()
