"""Build the staging (silver) tables from the raw layer.

For every match file, the latest version recorded in the manifest is read straight from
its stored zip, checked against its manifest checksum, parsed, and written to
``<out_dir>/<table>/season=YYYY/part-0.parquet``.

Each table is written to a temporary directory and swapped in only when complete, so a
rebuild replaces the whole table (no duplicates, no stale partitions) and a crash leaves
the previous version in place. Files that cannot be parsed are recorded in
``_parse_failures.parquet`` with the reason; they never stop the build.
"""

from __future__ import annotations

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
    row_counts: dict[str, int] = field(default_factory=dict)
    failures: list[ParseFailure] = field(default_factory=list)


def latest_versions(manifest: Manifest, sources: list[str]) -> pd.DataFrame:
    """The most recently seen version of each match file in ``sources``."""
    files = manifest.files()
    files = files[files["source"].isin(sources)].sort_values("first_seen_at")
    return files.groupby(["source", "member"], as_index=False).last()


def build_silver(
    raw_dir: Path, manifest: Manifest, out_dir: Path, competitions: Mapping[str, str]
) -> SilverBuild:
    """Rebuild every silver table. ``competitions`` maps source name -> competition code."""
    result = SilverBuild()
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    season_of: dict[str, int] = {}

    versions = latest_versions(manifest, list(competitions))
    for archive_path, group in versions.groupby("archive_path"):
        with zipfile.ZipFile(raw_dir / str(archive_path)) as zf:
            for f in group.itertuples():
                source, member, sha256 = str(f.source), str(f.member), str(f.sha256)
                try:
                    parsed = _parse_member(zf, member, sha256, competitions[source])
                except ParseError as e:
                    result.failures.append(
                        ParseFailure(
                            source=source,
                            member=member,
                            sha256=sha256,
                            archive_path=str(archive_path),
                            error_type=type(e).__name__,
                            reason=str(e),
                        )
                    )
                    continue
                season_of[parsed.match["match_id"]] = parsed.match["season"]
                rows["matches"].append(parsed.match)
                for table in SILVER_TABLES:
                    if table != "matches":
                        rows[table].extend(getattr(parsed, table))

    out_dir.mkdir(parents=True, exist_ok=True)
    for table, (schema, sort_key) in SILVER_TABLES.items():
        df = _to_frame(rows[table], schema)
        df["season"] = df["match_id"].map(season_of).astype("int64")
        _write_partitioned(out_dir / table, df.sort_values(["season", *sort_key]), schema)
        result.row_counts[table] = len(df)

    failures = sorted(result.failures, key=lambda f: (f.source, f.member))
    _write_file_atomic(
        out_dir / "_parse_failures.parquet",
        pa.Table.from_pylist([asdict(f) for f in failures], schema=PARSE_FAILURES),
    )
    return result


def _parse_member(zf: zipfile.ZipFile, member: str, sha256: str, competition: str) -> ParsedMatch:
    data = zf.read(member)
    actual = hashlib.sha256(data).hexdigest()
    if actual != sha256:
        raise ParseError(f"checksum mismatch: manifest {sha256}, file {actual}")
    try:
        doc = json.loads(data)
    except json.JSONDecodeError as e:
        raise ParseError(f"invalid JSON: {e}") from e
    return parse_match(
        doc,
        match_id=member.removesuffix(".json"),
        competition=competition,
        file_sha256=sha256,
    )


def _to_frame(rows: list[dict[str, Any]], schema: pa.Schema) -> pd.DataFrame:
    """Rows -> Arrow-backed DataFrame. Going through Arrow applies the declared types,
    so nullable integers stay integers instead of becoming float NaN."""
    table = pa.Table.from_pylist(rows, schema=schema)
    df: pd.DataFrame = table.to_pandas(types_mapper=pd.ArrowDtype)
    return df


def _write_partitioned(table_dir: Path, df: pd.DataFrame, schema: pa.Schema) -> None:
    tmp = table_dir.with_name(f".{table_dir.name}.tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    for season, part in df.groupby("season", sort=True):
        path = tmp / f"season={season}" / "part-0.parquet"
        path.parent.mkdir()
        arrow = pa.Table.from_pandas(
            part.drop(columns="season"), schema=schema, preserve_index=False
        )
        # Drop the pandas-specific metadata: files stay engine-neutral (DuckDB/dbt read them)
        pq.write_table(arrow.replace_schema_metadata(None), path)
    shutil.rmtree(table_dir, ignore_errors=True)
    os.replace(tmp, table_dir)


def _write_file_atomic(path: Path, table: pa.Table) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)


REGISTERS = {"players": "cricsheet_people", "player_name_variants": "cricsheet_names"}


def build_registers(raw_dir: Path, manifest: Manifest, out_dir: Path) -> dict[str, int]:
    """Stage the Cricsheet people and name registers from their latest downloads.

    Not partitioned: a person is not tied to a season. Every column is read as a string,
    so IDs such as ``key_cricinfo`` never turn into floats. Cricsheet adds ``key_*``
    columns as it links new sites, so the column set is taken from the file.
    """
    counts = {}
    for table, source in REGISTERS.items():
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
        _write_single(out_dir / table, pa.Table.from_pandas(df, preserve_index=False))
        counts[table] = len(df)
    return counts


def _write_single(table_dir: Path, table: pa.Table) -> None:
    tmp = table_dir.with_name(f".{table_dir.name}.tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    pq.write_table(table.replace_schema_metadata(None), tmp / "part-0.parquet")
    shutil.rmtree(table_dir, ignore_errors=True)
    os.replace(tmp, table_dir)


def main() -> None:
    from cricket_value.ingest.download import load_sources

    repo = Path(__file__).resolve().parents[2]
    raw_dir = repo / "data" / "raw"
    out_dir = repo / "data" / "silver"
    manifest = Manifest(raw_dir / "manifest")
    competitions = {
        s.name: s.competition
        for s in load_sources(repo / "config" / "sources.toml")
        if s.competition is not None
    }
    result = build_silver(raw_dir, manifest, out_dir, competitions)
    result.row_counts.update(build_registers(raw_dir, manifest, out_dir))
    for table, n in result.row_counts.items():
        print(f"{table:<20} {n:>10,} rows")
    print(f"{'parse failures':<20} {len(result.failures):>10,}")
    for f in result.failures:
        print(f"  {f.source}/{f.member}: {f.error_type}: {f.reason}")


if __name__ == "__main__":
    main()
