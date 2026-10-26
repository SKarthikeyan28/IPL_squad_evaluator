"""The pipeline as Dagster software-defined assets.

Lineage: raw/* -> silver/match_index -> silver/<table> (one partition per season)
-> reports/data_quality. Each asset calls the same functions as the ``make`` targets,
so Dagster only adds scheduling, partitions, lineage and checks. It holds no logic of
its own.

Data quality is reported as asset checks on the silver tables:

- ``contracts``: fails when the season had ``error`` violations in that table (those
  matches were quarantined). Warnings are listed in the metadata but don't fail it.
- ``reconciliation`` (on ``silver/matches``): fails when any reconciliation flag fired.

Both use WARN severity: the data written is still correct, since bad matches are
quarantined, so a failed check is a signal to investigate rather than a broken pipeline.
"""

# No `from __future__ import annotations` here: Dagster reads the real type hints to
# inject `context` and resources, and string annotations hide them.
from collections.abc import Iterator

import dagster as dg
import httpx
import pandas as pd

from cricket_value.ingest.__main__ import USER_AGENT
from cricket_value.ingest.download import Downloader, Source, load_sources
from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.schemas import SILVER_TABLES
from cricket_value.ingest.silver import (
    REGISTERS,
    SilverBuild,
    build_match_index,
    build_register,
    build_season,
    competitions_from_config,
    read_match_index,
    write_match_index,
)
from cricket_value.orchestration.partitions import season_partitions
from cricket_value.orchestration.resources import REPO_ROOT, ProjectPaths
from cricket_value.quality.report import render_markdown


class CricsheetHttp(dg.ConfigurableResource):
    """HTTP client settings for raw downloads."""

    user_agent: str = USER_AGENT
    timeout_seconds: float = 120

    def client(self) -> httpx.Client:
        return httpx.Client(headers={"User-Agent": self.user_agent}, timeout=self.timeout_seconds)


SOURCES = load_sources(REPO_ROOT / "config" / "sources.toml")
MATCH_SOURCES = [s for s in SOURCES if s.competition]


def raw_key(source: Source) -> dg.AssetKey:
    return dg.AssetKey(["raw", source.name])


MATCH_INDEX_KEY = dg.AssetKey(["silver", "match_index"])
SILVER_KEYS = {table: dg.AssetKey(["silver", table]) for table in SILVER_TABLES}
REGISTER_KEYS = {table: dg.AssetKey(["silver", table]) for table in REGISTERS}
REGISTER_SOURCE = {s.name: s for s in SOURCES}


# -- raw ------------------------------------------------------------------------------


def _raw_asset(source: Source) -> dg.AssetsDefinition:
    @dg.asset(
        key=raw_key(source),
        group_name="raw",
        description=f"Immutable downloads of {source.url}, recorded in the manifest.",
        kinds={"python"},
    )
    def _fetch(paths: ProjectPaths, http: CricsheetHttp) -> dg.MaterializeResult:
        manifest = Manifest(paths.raw_dir / "manifest")
        with http.client() as client:
            r = Downloader(client=client, raw_dir=paths.raw_dir, manifest=manifest).fetch(source)
        return dg.MaterializeResult(
            metadata={
                "status": r.status.value,
                "stored_file": str(r.path) if r.path else "(nothing new)",
                "new_match_files": r.new_files,
                "changed_match_files": r.changed_files,
                "snapshot_id": manifest.snapshot_id(),
            }
        )

    return _fetch


raw_assets = [_raw_asset(s) for s in SOURCES]


# -- silver ---------------------------------------------------------------------------


@dg.asset(
    key=MATCH_INDEX_KEY,
    deps=[raw_key(s) for s in MATCH_SOURCES],
    group_name="silver",
    description="Latest version of every match file, mapped to its season.",
    kinds={"python", "parquet"},
)
def match_index(paths: ProjectPaths) -> dg.MaterializeResult:
    manifest = Manifest(paths.raw_dir / "manifest")
    index, failures = build_match_index(
        paths.raw_dir, manifest, competitions_from_config(paths.sources_path)
    )
    write_match_index(paths.silver_dir, index, failures)
    per_season = index.groupby(["season", "competition"]).size().unstack(fill_value=0)
    return dg.MaterializeResult(
        metadata={
            "match_files": len(index),
            "unreadable_files": len(failures),
            "matches_per_season": dg.MetadataValue.md(per_season.to_markdown()),
            "snapshot_id": manifest.snapshot_id(),
        }
    )


@dg.multi_asset(
    name="silver_season",
    specs=[
        dg.AssetSpec(
            key,
            deps=[MATCH_INDEX_KEY],
            group_name="silver",
            description=f"Staging table `{table}`, one partition per season.",
            kinds={"python", "parquet"},
        )
        for table, key in SILVER_KEYS.items()
    ],
    partitions_def=season_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    check_specs=[
        *(
            dg.AssetCheckSpec(
                "contracts",
                asset=key,
                description="No data contract errors (matches with errors are quarantined).",
            )
            for key in SILVER_KEYS.values()
        ),
        dg.AssetCheckSpec(
            "reconciliation",
            asset=SILVER_KEYS["matches"],
            description="Targets, margins, XI sizes, dismissals and legal balls agree.",
        ),
    ],
)
def silver_season(
    context: dg.AssetExecutionContext, paths: ProjectPaths
) -> Iterator[dg.MaterializeResult | dg.AssetCheckResult]:
    # One run covers every season in the selected range (single-run backfill policy).
    seasons = sorted(int(k) for k in context.partition_keys)
    index = read_match_index(paths.silver_dir)
    result = SilverBuild()
    per_season = []
    for season in seasons:
        built = build_season(paths.raw_dir, index, season, paths.silver_dir)
        per_season.append({"season": season, **built.row_counts})
        result.add(built)
    violations = result.violations
    rows_by_season = pd.DataFrame(per_season).set_index("season")

    for table, key in SILVER_KEYS.items():
        yield dg.MaterializeResult(
            asset_key=key,
            metadata={
                "rows": result.row_counts[table],
                "seasons": f"{seasons[0]}-{seasons[-1]}" if len(seasons) > 1 else str(seasons[0]),
                "rows_per_season": dg.MetadataValue.md(rows_by_season[[table]].to_markdown()),
                "path": str(paths.silver_dir / table),
            },
        )

    for table, key in SILVER_KEYS.items():
        found = violations[violations["table"] == table]
        errors = found[found["severity"] == "error"]
        yield dg.AssetCheckResult(
            asset_key=key,
            check_name="contracts",
            passed=errors.empty,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "errors": len(errors),
                "warnings": int((found["severity"] == "warn").sum()),
                "quarantined_matches": result.quarantined_matches,
                "by_rule": dg.MetadataValue.md(_rule_counts(found)),
            },
        )

    flags = violations[violations["table"] == "reconciliation"]
    yield dg.AssetCheckResult(
        asset_key=SILVER_KEYS["matches"],
        check_name="reconciliation",
        passed=flags.empty,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "flags": len(flags),
            "detail": dg.MetadataValue.md(_flag_table(flags)),
        },
    )


def _rule_counts(found: pd.DataFrame) -> str:
    if found.empty:
        return "No violations."
    counts = found.groupby(["severity", "rule"]).size().rename("count").reset_index()
    return counts.to_markdown(index=False)


def _flag_table(flags: pd.DataFrame) -> str:
    if flags.empty:
        return "No flags."
    return flags[["match_id", "rule", "failure_case"]].to_markdown(index=False)


def _register_asset(table: str) -> dg.AssetsDefinition:
    source = REGISTER_SOURCE[REGISTERS[table]]

    @dg.asset(
        key=REGISTER_KEYS[table],
        deps=[raw_key(source)],
        group_name="silver",
        description=f"Staged Cricsheet register `{source.name}` (not partitioned).",
        kinds={"python", "parquet"},
    )
    def _stage(paths: ProjectPaths) -> dg.MaterializeResult:
        manifest = Manifest(paths.raw_dir / "manifest")
        rows = build_register(paths.raw_dir, manifest, paths.silver_dir, table)
        return dg.MaterializeResult(metadata={"rows": rows})

    return _stage


register_assets = [_register_asset(t) for t in REGISTERS]


# -- reports --------------------------------------------------------------------------


@dg.asset(
    key=dg.AssetKey(["reports", "data_quality"]),
    deps=list(SILVER_KEYS.values()),
    group_name="reports",
    description="reports/data_quality.md: rows, quarantines and violations per season.",
    kinds={"python", "markdown"},
)
def data_quality_report(paths: ProjectPaths) -> dg.MaterializeResult:
    snapshot = Manifest(paths.raw_dir / "manifest").snapshot_id()
    out = paths.reports_dir / "data_quality.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_markdown(paths.silver_dir, snapshot))
    return dg.MaterializeResult(metadata={"path": str(out), "snapshot_id": snapshot})
