"""Data contracts for the staging (silver) tables, and the quarantine they feed.

Each rule has a name and a severity:

- ``error``: the data is unusable. The whole match is quarantined (removed from every
  silver table and kept in ``_quarantine``), because a match with one broken delivery
  would make every innings total, rating and feature built on it wrong.
- ``warn``: the data is odd but usable. The match is kept, and the violation is logged.

Row-level rules are pandera schemas. Rules that need another table (does this match
exist? is this team playing in it?) are plain pandas joins, reported the same way.

Accepted values come from https://cricsheet.org/format/json/ (checked 2026-10-26), plus
values found in real data that the documentation does not list. Each of those is marked.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Literal

import pandas as pd
import pandera.pandas as pa
import pyarrow

from cricket_value.ingest.schemas import SILVER_TABLES

Severity = Literal["error", "warn"]
Tables = Mapping[str, pd.DataFrame]

VIOLATION_COLUMNS = ["match_id", "season", "table", "rule", "severity", "column", "failure_case"]

REGISTRY_ID = r"^[0-9a-f]{8}$"

WICKET_KINDS = [
    "bowled",
    "caught",
    "caught and bowled",
    "lbw",
    "stumped",
    "run out",
    "retired hurt",
    "hit wicket",
    "obstructing the field",
    "hit the ball twice",
    "handled the ball",
    "timed out",
    # Not in the documented list, but present in real data and valid under the Laws:
    "retired out",
    "retired not out",
]
MATCH_REPLACEMENT_REASONS = [
    "concussion_substitute",
    "covid_replacement",
    "injury_substitute",
    "national_callup",
    "national_release",
    "supersub",
    "tactical_substitute",
    "unknown",
    # Not documented; Cricsheet's label for IPL Impact Player substitutions (2023 onwards):
    "impact_player",
]
ROLE_REPLACEMENT_REASONS = [
    "excluded - excessive short-pitched deliveries",
    "excluded - high full pitched balls",
    "excluded - running on the pitch",
    "injury",
    "too many overs",
    "unknown",
]
RESULTS = ["win", "tie", "no_result", "draw"]
WIN_METHODS = ["D/L", "VJD", "Awarded", "1st innings score", "Lost fewer wickets"]

BALL_KEY = ["match_id", "innings_number", "over_number", "ball_in_over"]


# -- helpers -------------------------------------------------------------------


def _rule(name: str, fn: Callable[[pd.DataFrame], pd.Series]) -> pa.Check:
    """A row-level rule over the whole frame. Rows where ``fn`` is NA pass."""
    return pa.Check(lambda df: fn(df).fillna(True).astype(bool), name=name)


def _unique(key: list[str]) -> pa.Check:
    return _rule("key_unique", lambda df: ~df.duplicated(key, keep=False))


# Built-in pandera checks report their ``error`` string as the failed check, so rule names
# are passed as ``error=``. Custom checks report their ``name``.
def _col(*checks: pa.Check, nullable: bool = False) -> pa.Column:
    return pa.Column(checks=list(checks), nullable=nullable, required=True)


def _isin(name: str, values: list[str]) -> pa.Check:
    return pa.Check.isin(values, error=name)


def _registry_id(column: str) -> pa.Check:
    return pa.Check.str_matches(REGISTRY_ID, error=f"{column}_is_registry_id")


def _in_teams(column: str) -> Callable[[pd.DataFrame], pd.Series]:
    return lambda df: (df[column] == df["team1"]) | (df[column] == df["team2"])


# -- row-level contracts ---------------------------------------------------------

ERROR_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "matches": pa.DataFrameSchema(
        {
            "match_id": _col(pa.Check.str_matches(r"^\d+$", error="match_id_numeric")),
            "competition": _col(_isin("competition_accepted", ["ipl", "t20i"])),
            "season": _col(pa.Check.in_range(2005, 2100, error="season_in_range")),
            "match_date": _col(),
            "gender": _col(_isin("gender_is_male", ["male"])),
            "match_type": _col(_isin("match_type_is_t20", ["T20"])),
            "balls_per_over": _col(pa.Check.equal_to(6, error="balls_per_over_is_6")),
            "team1": _col(),
            "team2": _col(),
            "toss_decision": _col(_isin("toss_decision_accepted", ["bat", "field"])),
            "result": _col(_isin("result_accepted", RESULTS)),
            "win_method": _col(_isin("win_method_accepted", WIN_METHODS), nullable=True),
            "win_by_runs": _col(pa.Check.ge(0, error="win_by_runs_non_negative"), nullable=True),
            "win_by_wickets": _col(
                pa.Check.in_range(0, 10, error="win_by_wickets_in_range"), nullable=True
            ),
        },
        checks=[
            _unique(["match_id"]),
            _rule("teams_differ", lambda df: df["team1"] != df["team2"]),
            _rule("toss_winner_is_a_team", _in_teams("toss_winner")),
            _rule("winner_is_a_team", _in_teams("winner")),
            _rule(
                "win_has_winner",
                lambda df: (df["result"] != "win") | df["winner"].notna(),
            ),
            _rule(
                "no_result_has_no_winner",
                lambda df: (df["result"] != "no_result") | df["winner"].isna(),
            ),
            _rule("end_not_before_start", lambda df: df["end_date"] >= df["match_date"]),
        ],
    ),
    "innings": pa.DataFrameSchema(
        {
            "innings_number": _col(pa.Check.ge(1, error="innings_number_positive")),
            "batting_team": _col(),
            "bowling_team": _col(),
            "target_runs": _col(pa.Check.ge(0, error="target_non_negative"), nullable=True),
        },
        checks=[
            _unique(["match_id", "innings_number"]),
            _rule("teams_differ", lambda df: df["batting_team"] != df["bowling_team"]),
        ],
    ),
    "deliveries": pa.DataFrameSchema(
        {
            "batter_id": _col(_registry_id("batter_id")),
            "non_striker_id": _col(_registry_id("non_striker_id")),
            "bowler_id": _col(_registry_id("bowler_id")),
            "over_number": _col(pa.Check.in_range(0, 19, error="over_number_in_range")),
            "ball_in_over": _col(pa.Check.ge(1, error="ball_in_over_positive")),
            "runs_batter": _col(pa.Check.in_range(0, 8, error="runs_batter_in_range")),
            "runs_extras": _col(pa.Check.in_range(0, 8, error="runs_extras_in_range")),
            "n_wickets": _col(pa.Check.in_range(0, 2, error="n_wickets_in_range")),
            **{
                c: _col(pa.Check.in_range(0, 7, error=f"{c}_in_range"))
                for c in (
                    "extras_wides",
                    "extras_noballs",
                    "extras_byes",
                    "extras_legbyes",
                    "extras_penalty",
                )
            },
        },
        checks=[
            _unique(BALL_KEY),
            _rule(
                "runs_total_is_batter_plus_extras",
                lambda df: df["runs_total"] == df["runs_batter"] + df["runs_extras"],
            ),
            _rule(
                "extras_add_up",
                lambda df: (
                    df["runs_extras"]
                    == df["extras_wides"]
                    + df["extras_noballs"]
                    + df["extras_byes"]
                    + df["extras_legbyes"]
                    + df["extras_penalty"]
                ),
            ),
            _rule(
                "not_wide_and_no_ball",
                lambda df: ~((df["extras_wides"] > 0) & (df["extras_noballs"] > 0)),
            ),
            _rule(
                "is_legal_matches_extras",
                lambda df: (
                    df["is_legal"] == ((df["extras_wides"] == 0) & (df["extras_noballs"] == 0))
                ),
            ),
        ],
    ),
    "wickets": pa.DataFrameSchema(
        {
            "player_out_id": _col(_registry_id("player_out_id")),
            "kind": _col(_isin("kind_accepted", WICKET_KINDS)),
        },
        checks=[_unique([*BALL_KEY, "wicket_seq"])],
    ),
    "replacements": pa.DataFrameSchema(
        {
            "replacement_type": _col(_isin("replacement_type_accepted", ["match", "role"])),
            "player_in_id": _col(_registry_id("player_in_id")),
            "player_out_id": _col(_registry_id("player_out_id"), nullable=True),
            "role": _col(_isin("role_accepted", ["batter", "bowler"]), nullable=True),
        },
        checks=[
            # accepted reasons depend on the replacement type
            _rule(
                "reason_accepted",
                lambda df: (
                    (
                        (df["replacement_type"] == "match")
                        & df["reason"].isin(MATCH_REPLACEMENT_REASONS)
                    )
                    | (
                        (df["replacement_type"] == "role")
                        & df["reason"].isin(ROLE_REPLACEMENT_REASONS)
                    )
                ),
            ),
            _rule(
                "match_replacement_has_team",
                lambda df: (df["replacement_type"] != "match") | df["team"].notna(),
            ),
        ],
    ),
    "match_players": pa.DataFrameSchema(
        {
            "team": _col(),
            "player_id": _col(_registry_id("player_id")),
            "joined_reason": _col(
                _isin("joined_reason_accepted", MATCH_REPLACEMENT_REASONS), nullable=True
            ),
        },
        checks=[_unique(["match_id", "team", "player_id"])],
    ),
}

WARN_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    # 11 T20Is in Cricsheet say 50 overs per side, but no over past the 20th was bowled.
    # The header field is wrong; the ball-by-ball data is fine.
    "matches": pa.DataFrameSchema(
        {"overs_per_side": _col(pa.Check.equal_to(20, error="overs_per_side_is_20"), nullable=True)}
    ),
    "deliveries": pa.DataFrameSchema(
        checks=[
            _rule(
                "no_batter_runs_off_a_wide",
                lambda df: ~((df["extras_wides"] > 0) & (df["runs_batter"] > 0)),
            )
        ]
    ),
}


# -- running the contracts ---------------------------------------------------------


def check_contracts(tables: Tables) -> pd.DataFrame:
    """Every violation of every contract, one row per failing row and rule."""
    parts = []
    runs: list[tuple[Severity, dict[str, pa.DataFrameSchema]]] = [
        ("error", ERROR_SCHEMAS),
        ("warn", WARN_SCHEMAS),
    ]
    for severity, schemas in runs:
        for table, schema in schemas.items():
            parts.append(_validate(table, tables[table], schema, severity))
    parts.append(_relationships(tables))
    return _finish(parts)


def _validate(
    table: str, df: pd.DataFrame, schema: pa.DataFrameSchema, severity: Severity
) -> pd.DataFrame:
    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        fc = e.failure_cases
    else:
        return empty_violations()
    if fc["index"].isna().any():
        bad = fc[fc["index"].isna()][["column", "check"]].to_dict("records")
        raise ValueError(f"{table}: contract failed for the table as a whole: {bad}")

    # Whole-row rules come back once per column of the failing row: keep one, and
    # describe it by the row's key instead of an arbitrary column value.
    row_level = fc["schema_context"] == "DataFrameSchema"
    fc = pd.concat(
        [fc[~row_level], fc[row_level].drop_duplicates(["index", "check"])], ignore_index=True
    )
    row_level = fc["schema_context"] == "DataFrameSchema"
    idx = fc["index"].astype("int64").to_numpy()
    key = SILVER_TABLES[table][1]
    key_text = df[key].astype(str).radd([f"{k}=" for k in key]).agg(" ".join, axis=1)

    rule = fc["check"].astype(str)
    rule = rule.where(rule != "not_nullable", fc["column"].astype(str) + "_not_null")
    case = fc["failure_case"].astype(str).where(~row_level, key_text.to_numpy()[idx])
    return pd.DataFrame(
        {
            "match_id": df["match_id"].to_numpy(dtype=object)[idx],
            "season": df["season"].to_numpy()[idx],
            "table": table,
            "rule": rule.to_numpy(),
            "severity": severity,
            "column": fc["column"].where(~row_level).to_numpy(),
            "failure_case": case.to_numpy(),
        }
    )


def _relationships(tables: Tables) -> pd.DataFrame:
    """Rules that join two tables. All are errors."""
    matches = tables["matches"][["match_id", "team1", "team2"]]
    innings_key = tables["innings"][["match_id", "innings_number"]]
    parts = []

    def flag(table: str, rule: str, column: str | None, bad: pd.DataFrame, case: str) -> None:
        if not bad.empty:
            parts.append(
                pd.DataFrame(
                    {
                        "match_id": bad["match_id"].to_numpy(dtype=object),
                        "season": bad["season"].to_numpy(),
                        "table": table,
                        "rule": rule,
                        "severity": "error",
                        "column": column,
                        "failure_case": bad[case].astype(str).to_numpy(),
                    }
                )
            )

    for table in ("innings", "deliveries", "wickets", "replacements", "match_players"):
        df = tables[table]
        flag(
            table,
            "match_exists",
            "match_id",
            df[~df["match_id"].isin(matches["match_id"])],
            "match_id",
        )

    for table in ("deliveries", "wickets", "replacements"):
        df = tables[table]
        joined = df.merge(
            innings_key, on=["match_id", "innings_number"], how="left", indicator=True
        )
        flag(
            table,
            "innings_exists",
            "innings_number",
            joined[joined["_merge"] == "left_only"],
            "innings_number",
        )

    wk = tables["wickets"].merge(
        tables["deliveries"][BALL_KEY], on=BALL_KEY, how="left", indicator=True
    )
    flag("wickets", "delivery_exists", None, wk[wk["_merge"] == "left_only"], "ball_in_over")

    for table, column, rule in (
        ("innings", "batting_team", "batting_team_is_a_team"),
        ("match_players", "team", "team_is_a_team"),
        ("replacements", "team", "team_is_a_team"),
    ):
        df = tables[table].merge(matches, on="match_id", how="inner")
        bad = df[df[column].notna() & ~_in_teams(column)(df).fillna(False).astype(bool)]
        flag(table, rule, column, bad, column)

    return _finish(parts)


def _finish(parts: list[pd.DataFrame]) -> pd.DataFrame:
    parts = [p for p in parts if not p.empty]
    if not parts:
        return empty_violations()
    out = pd.concat(parts, ignore_index=True)[VIOLATION_COLUMNS]
    out["season"] = out["season"].astype("int64")
    return out.sort_values(
        ["season", "match_id", "table", "rule", "failure_case"], ignore_index=True
    )


def empty_violations() -> pd.DataFrame:
    return pd.DataFrame(
        {c: pd.Series(dtype="int64" if c == "season" else object) for c in VIOLATION_COLUMNS}
    )


VIOLATIONS_SCHEMA = pyarrow.schema(
    [
        ("match_id", pyarrow.string()),
        ("season", pyarrow.int64()),
        ("table", pyarrow.string()),
        ("rule", pyarrow.string()),
        ("severity", pyarrow.string()),
        ("column", pyarrow.string()),
        ("failure_case", pyarrow.string()),
    ]
)


def violations_table(violations: pd.DataFrame) -> pyarrow.Table:
    return pyarrow.Table.from_pandas(
        violations[VIOLATION_COLUMNS], schema=VIOLATIONS_SCHEMA, preserve_index=False
    ).replace_schema_metadata(None)


# -- quarantine ------------------------------------------------------------------


def apply_quarantine(
    tables: Tables, violations: pd.DataFrame
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Split each table into clean rows and quarantined rows.

    Any ``error`` violation quarantines the whole match. Quarantined rows carry
    ``quarantine_rules``: every ``table.rule`` that match broke, so the reason travels
    with the data.
    """
    errors = violations[violations["severity"] == "error"]
    reasons = (
        (errors["table"] + "." + errors["rule"])
        .groupby(errors["match_id"])
        .agg(lambda s: "; ".join(sorted(set(s))))
    )
    clean, quarantined = {}, {}
    for table, df in tables.items():
        bad = df["match_id"].isin(reasons.index)
        clean[table] = df[~bad].reset_index(drop=True)
        quarantined[table] = (
            df[bad]
            .assign(quarantine_rules=df.loc[bad, "match_id"].map(reasons).astype(str))
            .reset_index(drop=True)
        )
    return clean, quarantined
