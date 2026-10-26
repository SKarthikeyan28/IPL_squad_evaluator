"""Physical schemas of the staging (silver) Parquet tables.

Explicit Arrow types keep files stable across runs: pandas would otherwise guess a type
per batch, for example int64 in one season and float64 in another once a null appears.
``season`` is not stored in the files. It is the hive partition directory (season=YYYY).
"""

from __future__ import annotations

from typing import Final

import pyarrow as pa

STR = pa.string()
I8, I16, I32 = pa.int8(), pa.int16(), pa.int32()
BOOL = pa.bool_()

MATCHES = pa.schema(
    [
        ("match_id", STR),
        ("competition", STR),
        ("season_label", STR),
        ("match_date", pa.date32()),
        ("end_date", pa.date32()),
        ("n_dates", I8),
        ("data_version", STR),
        ("file_sha256", STR),
        ("event_name", STR),
        ("match_number", I16),
        ("event_stage", STR),
        ("event_group", STR),
        ("match_type", STR),
        ("team_type", STR),
        ("gender", STR),
        ("balls_per_over", I8),
        ("overs_per_side", I8),
        ("city", STR),
        ("venue_name", STR),
        ("venue_registry_id", STR),
        ("team1", STR),
        ("team2", STR),
        ("toss_winner", STR),
        ("toss_decision", STR),
        ("toss_uncontested", BOOL),
        ("result", STR),
        ("winner", STR),
        ("win_by_runs", I16),
        ("win_by_wickets", I8),
        ("win_method", STR),
        ("decided_by_super_over", BOOL),
        ("decided_by_bowl_out", BOOL),
    ]
)

INNINGS = pa.schema(
    [
        ("match_id", STR),
        ("innings_number", I8),
        ("batting_team", STR),
        ("bowling_team", STR),
        ("is_super_over", BOOL),
        ("is_forfeited", BOOL),
        ("target_runs", I16),
        ("target_overs", pa.float32()),
        ("penalty_runs_pre", I16),
        ("penalty_runs_post", I16),
        ("n_absent_hurt", I8),
        ("has_miscounted_overs", BOOL),
    ]
)

_BALL_KEY = [
    ("match_id", STR),
    ("innings_number", I8),
    ("over_number", I8),
    ("ball_in_over", I8),
]

DELIVERIES = pa.schema(
    [
        *_BALL_KEY,
        ("actual_delivery", STR),
        ("batter_id", STR),
        ("non_striker_id", STR),
        ("bowler_id", STR),
        ("runs_batter", I8),
        ("runs_extras", I8),
        ("runs_total", I8),
        ("is_non_boundary", BOOL),
        ("extras_wides", I8),
        ("extras_noballs", I8),
        ("extras_byes", I8),
        ("extras_legbyes", I8),
        ("extras_penalty", I8),
        ("is_legal", BOOL),
        ("n_wickets", I8),
    ]
)

WICKETS = pa.schema(
    [
        *_BALL_KEY,
        ("wicket_seq", I8),
        ("player_out_id", STR),
        ("kind", STR),
        ("fielder_ids", pa.list_(STR)),
        ("has_substitute_fielder", BOOL),
    ]
)

REPLACEMENTS = pa.schema(
    [
        *_BALL_KEY,
        ("replacement_type", STR),
        ("team", STR),
        ("player_in_id", STR),
        ("player_out_id", STR),
        ("reason", STR),
        ("role", STR),
    ]
)

MATCH_PLAYERS = pa.schema(
    [
        ("match_id", STR),
        ("team", STR),
        ("player_id", STR),
        ("player_name", STR),
        ("is_starting_xi", BOOL),
        ("joined_reason", STR),
    ]
)

PARSE_FAILURES = pa.schema(
    [
        ("source", STR),
        ("member", STR),
        ("sha256", STR),
        ("archive_path", STR),
        ("error_type", STR),
        ("reason", STR),
    ]
)

# table name -> (schema, sort key). Rows are written sorted so output is deterministic.
SILVER_TABLES: Final[dict[str, tuple[pa.Schema, list[str]]]] = {
    "matches": (MATCHES, ["match_id"]),
    "innings": (INNINGS, ["match_id", "innings_number"]),
    "deliveries": (DELIVERIES, [n for n, _ in _BALL_KEY]),
    "wickets": (WICKETS, [*(n for n, _ in _BALL_KEY), "wicket_seq"]),
    "replacements": (
        REPLACEMENTS,
        [*(n for n, _ in _BALL_KEY), "replacement_type", "player_in_id"],
    ),
    "match_players": (MATCH_PLAYERS, ["match_id", "team", "player_id"]),
}
