"""Parse one Cricsheet match JSON document into flat rows for the staging (silver) tables.

The parser only reshapes and types the data. It rejects a file (raises) when it cannot be
read at all: an unknown schema version, missing required fields, or a person missing from
the file's registry. Questions about whether the data is *plausible* (11 players per side,
runs that reconcile with targets) belong to the data contracts, not here.

Schema version differences (https://cricsheet.org/format/json/#changelog):

- 1.0.0  initial version
- 1.1.0  adds ``toss.uncontested`` and ``innings[].forfeited``
- 1.2.0  adds ``actual_delivery`` to every delivery (derived here for older files)
- 1.3.0  adds ``registry.venues`` (``venue_registry_id`` is null for older files)

Cricsheet records Impact Player substitutions as ``replacements.match`` entries with
reason ``impact_player``, although the format documentation does not list that reason.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

SUPPORTED_VERSIONS = frozenset({"1.0.0", "1.1.0", "1.2.0", "1.3.0"})

Row = dict[str, Any]


class ParseError(ValueError):
    """The file cannot be turned into staging rows."""


class UnsupportedSchemaVersion(ParseError):
    pass


@dataclass
class ParsedMatch:
    match: Row
    innings: list[Row] = field(default_factory=list)
    deliveries: list[Row] = field(default_factory=list)
    wickets: list[Row] = field(default_factory=list)
    replacements: list[Row] = field(default_factory=list)
    match_players: list[Row] = field(default_factory=list)


def parse_match(
    doc: Mapping[str, Any], *, match_id: str, competition: str, file_sha256: str
) -> ParsedMatch:
    try:
        return _Parser(doc, match_id, competition, file_sha256).parse()
    except (KeyError, TypeError, IndexError) as e:
        raise ParseError(f"malformed match file: {type(e).__name__}: {e}") from e


class _Parser:
    def __init__(
        self, doc: Mapping[str, Any], match_id: str, competition: str, file_sha256: str
    ) -> None:
        version = doc["meta"]["data_version"]
        if version not in SUPPORTED_VERSIONS:
            raise UnsupportedSchemaVersion(f"unsupported Cricsheet data_version {version}")
        self.doc = doc
        self.info: Mapping[str, Any] = doc["info"]
        self.version: str = version
        self.match_id = match_id
        self.competition = competition
        self.file_sha256 = file_sha256
        self.people: Mapping[str, str] = self.info["registry"]["people"]
        self.teams: list[str] = list(self.info["teams"])
        # (team, player name) -> reason, for players who joined as a match replacement
        self.joined: dict[tuple[str, str], str] = {}

    def person(self, name: str) -> str:
        try:
            return self.people[name]
        except KeyError:
            raise ParseError(f"person {name!r} not in registry") from None

    def other_team(self, team: str) -> str | None:
        others = [t for t in self.teams if t != team]
        return others[0] if len(others) == 1 else None

    def parse(self) -> ParsedMatch:
        out = ParsedMatch(match=self.match_row())
        for number, inn in enumerate(self.doc["innings"], start=1):
            out.innings.append(self.innings_row(number, inn))
            self.parse_deliveries(number, inn, out)
        out.match_players = self.match_player_rows()
        return out

    # -- match ---------------------------------------------------------------

    def match_row(self) -> Row:
        info = self.info
        dates = sorted(dt.date.fromisoformat(d) for d in info["dates"])
        event = info.get("event", {})
        toss = info["toss"]
        outcome = info["outcome"]
        by = outcome.get("by", {})
        result_raw = outcome.get("result")
        result = {None: "win", "tie": "tie", "no result": "no_result", "draw": "draw"}[result_raw]
        winner = outcome.get("winner") or outcome.get("eliminator") or outcome.get("bowl_out")
        venues = info["registry"].get("venues", {})
        return {
            "match_id": self.match_id,
            "competition": self.competition,
            "season": dates[0].year,
            "season_label": str(info["season"]),
            "match_date": dates[0],
            "end_date": dates[-1],
            "n_dates": len(dates),
            "data_version": self.version,
            "file_sha256": self.file_sha256,
            "event_name": event.get("name"),
            "match_number": event.get("match_number"),
            "event_stage": event.get("stage"),
            "event_group": None if event.get("group") is None else str(event["group"]),
            "match_type": info["match_type"],
            "team_type": info["team_type"],
            "gender": info["gender"],
            "balls_per_over": info["balls_per_over"],
            "overs_per_side": info.get("overs"),
            "city": info.get("city"),
            "venue_name": info["venue"],
            "venue_registry_id": venues.get(info["venue"]),
            "team1": self.teams[0],
            "team2": self.teams[1],
            "toss_winner": toss.get("winner"),
            "toss_decision": toss.get("decision"),
            "toss_uncontested": bool(toss.get("uncontested", False)),
            "result": result,
            "winner": winner,
            "win_by_runs": by.get("runs"),
            "win_by_wickets": by.get("wickets"),
            "win_method": outcome.get("method"),
            "decided_by_super_over": "eliminator" in outcome,
            "decided_by_bowl_out": "bowl_out" in outcome,
        }

    # -- innings -------------------------------------------------------------

    def innings_row(self, number: int, inn: Mapping[str, Any]) -> Row:
        target = inn.get("target", {})
        penalty = inn.get("penalty_runs", {})
        overs = target.get("overs")
        return {
            "match_id": self.match_id,
            "innings_number": number,
            "batting_team": inn["team"],
            "bowling_team": self.other_team(inn["team"]),
            "is_super_over": bool(inn.get("super_over", False)),
            "is_forfeited": bool(inn.get("forfeited", False)),
            "target_runs": target.get("runs"),
            "target_overs": None if overs is None else float(overs),
            "penalty_runs_pre": penalty.get("pre", 0),
            "penalty_runs_post": penalty.get("post", 0),
            "n_absent_hurt": len(inn.get("absent_hurt", [])),
            "has_miscounted_overs": bool(inn.get("miscounted_overs")),
        }

    # -- deliveries, wickets, replacements -----------------------------------

    def parse_deliveries(self, number: int, inn: Mapping[str, Any], out: ParsedMatch) -> None:
        for over in inn.get("overs", []):
            over_number: int = over["over"]
            legal_so_far = 0
            for seq, ball in enumerate(over["deliveries"], start=1):
                extras = ball.get("extras", {})
                is_legal = "wides" not in extras and "noballs" not in extras
                # Cricsheet numbers a delivery by legal balls bowled before it, plus one, so a
                # wide and the ball after it share a number. Derived for files before 1.2.0.
                actual = ball.get("actual_delivery", f"{over_number}.{legal_so_far + 1}")
                legal_so_far += is_legal
                key = {
                    "match_id": self.match_id,
                    "innings_number": number,
                    "over_number": over_number,
                    "ball_in_over": seq,
                }
                wickets = ball.get("wickets", [])
                runs = ball["runs"]
                out.deliveries.append(
                    {
                        **key,
                        "actual_delivery": str(actual),
                        "batter_id": self.person(ball["batter"]),
                        "non_striker_id": self.person(ball["non_striker"]),
                        "bowler_id": self.person(ball["bowler"]),
                        "runs_batter": runs["batter"],
                        "runs_extras": runs["extras"],
                        "runs_total": runs["total"],
                        "is_non_boundary": bool(runs.get("non_boundary", False)),
                        "extras_wides": extras.get("wides", 0),
                        "extras_noballs": extras.get("noballs", 0),
                        "extras_byes": extras.get("byes", 0),
                        "extras_legbyes": extras.get("legbyes", 0),
                        "extras_penalty": extras.get("penalty", 0),
                        "is_legal": is_legal,
                        "n_wickets": len(wickets),
                    }
                )
                for wseq, w in enumerate(wickets, start=1):
                    fielders = [f["name"] for f in w.get("fielders", []) if "name" in f]
                    out.wickets.append(
                        {
                            **key,
                            "wicket_seq": wseq,
                            "player_out_id": self.person(w["player_out"]),
                            "kind": w["kind"],
                            "fielder_ids": [self.people.get(f) for f in fielders],
                            "has_substitute_fielder": any(
                                f.get("substitute", False) for f in w.get("fielders", [])
                            ),
                        }
                    )
                for rtype, reps in ball.get("replacements", {}).items():
                    for r in reps:
                        if rtype == "match":
                            self.joined[(r["team"], r["in"])] = r.get("reason") or "unknown"
                        out.replacements.append(
                            {
                                **key,
                                "replacement_type": rtype,
                                "team": r.get("team"),
                                "player_in_id": self.person(r["in"]),
                                "player_out_id": (self.person(r["out"]) if r.get("out") else None),
                                "reason": r.get("reason"),
                                "role": r.get("role"),
                            }
                        )

    # -- match players -------------------------------------------------------

    def match_player_rows(self) -> list[Row]:
        """Everyone listed for each team. Players who joined as a match replacement
        (Impact Player, concussion sub, ...) are not part of the starting XI."""
        rows = []
        for team, names in self.info["players"].items():
            for name in names:
                reason = self.joined.get((team, name))
                rows.append(
                    {
                        "match_id": self.match_id,
                        "team": team,
                        "player_id": self.person(name),
                        "player_name": name,
                        "is_starting_xi": reason is None,
                        "joined_reason": reason,
                    }
                )
        return rows
