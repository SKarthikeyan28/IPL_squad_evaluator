"""Dagster code location. Start the UI with ``make dagster``."""

from __future__ import annotations

import dagster as dg

from cricket_value.orchestration.assets import (
    CricsheetHttp,
    data_quality_report,
    elo_baseline,
    match_index,
    raw_assets,
    register_assets,
    silver_season,
)
from cricket_value.orchestration.resources import ProjectPaths

# Download changed sources, then index match files and stage the registers.
ingest_job = dg.define_asset_job(
    "ingest",
    selection=dg.AssetSelection.groups("raw")
    | dg.AssetSelection.assets(match_index, *register_assets),
)

# Build silver for chosen seasons: launch one partition, or backfill a range from the UI.
silver_job = dg.define_asset_job(
    "silver_by_season",
    selection=dg.AssetSelection.assets(silver_season),
)

report_job = dg.define_asset_job(
    "data_quality_report", selection=dg.AssetSelection.assets(data_quality_report)
)

# Cricsheet updates a few times a week during the season. Off by default: turn it on in
# the UI to keep the raw layer current.
weekly_ingest = dg.ScheduleDefinition(
    job=ingest_job,
    cron_schedule="0 6 * * 1",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        *raw_assets,
        match_index,
        silver_season,
        *register_assets,
        data_quality_report,
        elo_baseline,
    ],
    jobs=[ingest_job, silver_job, report_job],
    schedules=[weekly_ingest],
    resources={"paths": ProjectPaths(), "http": CricsheetHttp()},
)
