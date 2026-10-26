"""Season partitions: one partition per calendar year, from the first T20I season (2005).

Yearly time-window partitions extend themselves: a new season appears on 1 January with
no config change. ``end_offset=1`` includes the current, still-running year.
"""

from __future__ import annotations

import dagster as dg

FIRST_SEASON = 2005

season_partitions = dg.TimeWindowPartitionsDefinition(
    start=str(FIRST_SEASON),  # parsed with `fmt`
    cron_schedule="0 0 1 1 *",
    fmt="%Y",
    end_offset=1,
)
