"""Does a batter's rating against pace or spin predict better than his rating against everyone?

The pace/spin split costs data: a batter's balls against spin are a fraction of his balls
faced, so the split rating is noisier even after shrinkage. It is only worth having if it
predicts the next ball better. This scores both on the same balls, the way
``cricket_value.ratings.tuning`` scores the tuning grid:

- **Balls.** IPL 2010-2024 (2025-2026 stay locked), no super overs and no wides, faced
  against a bowler whose bowling kind is known. Only balls where every rating exists are
  kept, so the two scores are paired.
- **Two scores per ball.** The unsplit score uses the batter's rating against all bowlers.
  The split score swaps in his rating against this bowler's kind. The bowler's rating, the
  combination rule and the scoring rule are identical, so the only difference is the
  batter's rating.
- **Uncertainty.** A bootstrap over matches gives a 95% interval for the difference in log
  loss (split minus unsplit, per 1,000 balls; negative means the split predicts better).

- **The prior is a design choice, so it is swept.** A split rating can shrink toward the
  role's average against that kind of bowler (the plan in DECISIONS.md) or toward the
  batter's own rating against everyone, which keeps what his other balls say. Each setting
  rebuilds the ratings in a scratch warehouse (``dbt run --select int_player_ratings_long+``
  with ``--vars``), so ``data/warehouse.duckdb`` is never touched.

Run with ``make split-eval``; writes ``reports/pacespin_split.md``.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd

from cricket_value.ratings.tuning import (
    EVAL_SEASONS,
    HISTORY_SQL,
    RATING_COLUMNS,
    _dbt,
    _eval_sql,
    _query,
    league_runs_distribution,
    score_balls,
)

REPO = Path(__file__).resolve().parents[2]
SPLIT_COLUMNS = ["split_dismissal", "split_runs"]

SPLIT_BALLS_SQL = """
    with balls as ({eval_balls}),
    kinds as (select player_id, bowling_kind from dim_player where bowling_kind is not null),
    long as (select * from int_player_ratings_long),
    priors as (
        select * from int_rating_priors where role = 'any' and skill = 'bowl' and vs = 'all'
    )
    select b.match_id, b.season, b.phase, k.bowling_kind as vs, b.runs_batter,
        b.batter_dismissed,
        bd.rating as bat_dismissal, br.rating as bat_runs,
        sd.rating as split_dismissal, sr.rating as split_runs,
        ww.rating as bowl_wicket, wr.rating as bowl_runs,
        pw.prior_mean as bowl_wicket_mean, pr.prior_mean as bowl_runs_mean
    from balls b
    join kinds k on k.player_id = b.bowler_id
    left join long bd on bd.player_id = b.batter_id and bd.match_date = b.match_date
        and bd.phase = b.phase and bd.skill = 'bat' and bd.stat = 'dismissal_rate'
        and bd.vs = 'all'
    left join long br on br.player_id = b.batter_id and br.match_date = b.match_date
        and br.phase = b.phase and br.skill = 'bat' and br.stat = 'runs_per_ball'
        and br.vs = 'all'
    left join long sd on sd.player_id = b.batter_id and sd.match_date = b.match_date
        and sd.phase = b.phase and sd.skill = 'bat' and sd.stat = 'dismissal_rate'
        and sd.vs = k.bowling_kind
    left join long sr on sr.player_id = b.batter_id and sr.match_date = b.match_date
        and sr.phase = b.phase and sr.skill = 'bat' and sr.stat = 'runs_per_ball'
        and sr.vs = k.bowling_kind
    left join long ww on ww.player_id = b.bowler_id and ww.match_date = b.match_date
        and ww.phase = b.phase and ww.skill = 'bowl' and ww.stat = 'wicket_rate'
        and ww.vs = 'all'
    left join long wr on wr.player_id = b.bowler_id and wr.match_date = b.match_date
        and wr.phase = b.phase and wr.skill = 'bowl' and wr.stat = 'runs_per_ball'
        and wr.vs = 'all'
    left join priors pw on pw.prior_year = year(b.match_date) and pw.phase = b.phase
        and pw.stat = 'wicket_rate'
    left join priors pr on pr.prior_year = year(b.match_date) and pr.phase = b.phase
        and pr.stat = 'runs_per_ball'
"""


@dataclass(frozen=True)
class Setting:
    """How a split rating's prior is built (the dbt vars rating_split_*)."""

    shrink_to: str  # "overall": the batter's own overall rating; "role": the role's average
    multiplier: float  # prior strength relative to the series' own (ignored for "role")

    def dbt_vars(self) -> dict[str, object]:
        return {
            "rating_split_shrink_to": self.shrink_to,
            "rating_split_prior_multiplier": self.multiplier,
        }

    def label(self) -> str:
        if self.shrink_to == "role":
            return "role average"
        return f"own overall rating, strength x{self.multiplier:g}"


# Must equal the defaults in dbt/dbt_project.yml (a test checks)
DEFAULT = Setting("overall", 2)
SETTINGS = [
    Setting("role", 1),
    Setting("overall", 1),
    DEFAULT,
    Setting("overall", 4),
    Setting("overall", 8),
    Setting("overall", 16),
]


class Difference(NamedTuple):
    """Split minus unsplit log loss per 1,000 balls, with a bootstrap 95% interval."""

    diff: float
    low: float
    high: float


def compare(balls: pd.DataFrame, league: pd.DataFrame) -> pd.DataFrame:
    """Per-match log-likelihoods under the unsplit and the split batter rating."""
    all_scores = score_balls(balls, league)
    split_inputs = balls.assign(
        bat_dismissal=balls["split_dismissal"], bat_runs=balls["split_runs"]
    )
    split_scores = score_balls(split_inputs, league)
    scored = pd.DataFrame(
        {
            "match_id": all_scores["match_id"],
            "ll_dismissal_all": all_scores["ll_dismissal"],
            "ll_runs_all": all_scores["ll_runs"],
            "ll_dismissal_split": split_scores["ll_dismissal"],
            "ll_runs_split": split_scores["ll_runs"],
        }
    )
    totals = scored.groupby("match_id").sum()
    totals.insert(0, "balls", scored.groupby("match_id").size())
    return totals


def paired_difference(
    totals: pd.DataFrame, *, n_resamples: int = 1000, seed: int = 0
) -> dict[str, Difference]:
    """Split minus unsplit log loss per 1,000 balls (negative: the split is better), for
    dismissals, runs and both. Matches are resampled, the same ones for every component."""
    balls = totals["balls"].to_numpy(float)
    parts = {
        "dismissal": (totals["ll_dismissal_split"] - totals["ll_dismissal_all"]).to_numpy(float),
        "runs": (totals["ll_runs_split"] - totals["ll_runs_all"]).to_numpy(float),
    }
    parts["total"] = parts["dismissal"] + parts["runs"]
    rng = np.random.default_rng(seed)
    counts = rng.multinomial(len(balls), np.full(len(balls), 1 / len(balls)), size=n_resamples)
    out = {}
    for name, d in parts.items():
        # loss difference = -(change in log-likelihood) / balls
        observed = -1000 * d.sum() / balls.sum()
        resampled = -1000 * (counts @ d) / (counts @ balls)
        low, high = np.percentile(resampled, [2.5, 97.5])
        out[name] = Difference(float(observed), float(low), float(high))
    return out


# --- Loading and report ----------------------------------------------------------------


def load_split_balls(db: Path, seasons: range = EVAL_SEASONS) -> pd.DataFrame:
    """Evaluation balls against bowlers of known kind with every rating present."""
    eval_balls = _eval_sql(seasons)
    balls = _query(db, SPLIT_BALLS_SQL.format(eval_balls=eval_balls))
    expected = int(
        _query(
            db,
            f"select count(*) as n from ({eval_balls}) b "
            "join dim_player p on p.player_id = b.bowler_id and p.bowling_kind is not null",
        )["n"].iloc[0]
    )
    if len(balls) != expected:
        raise ValueError(f"the rating joins turned {expected} balls into {len(balls)}")
    return balls


def complete_cases(balls: pd.DataFrame) -> pd.DataFrame:
    return balls.dropna(subset=[*RATING_COLUMNS, *SPLIT_COLUMNS]).reset_index(drop=True)


def slices(balls: pd.DataFrame) -> dict[str, pd.DataFrame]:
    out = {"all balls": balls}
    for vs in ("pace", "spin"):
        out[f"against {vs}"] = balls[balls["vs"] == vs]
    for phase in ("powerplay", "middle", "death"):
        out[f"{phase}"] = balls[balls["phase"] == phase]
    return out


@dataclass
class SweepResult:
    sweep: dict[Setting, dict[str, Difference]]  # all balls, per prior setting
    slices: dict[str, dict[str, Difference]]  # the default setting, per slice of balls
    balls: int
    dropped: int


def run_sweep(
    silver_dir: Path,
    work_dir: Path,
    settings: list[Setting] = SETTINGS,
    *,
    seasons: range = EVAL_SEASONS,
    n_resamples: int = 1000,
    extra_vars: Mapping[str, object] | None = None,
) -> SweepResult:
    """Build a scratch warehouse in ``work_dir``, then rebuild the ratings under each prior
    setting and score the split against the unsplit rating. ``extra_vars`` are dbt vars for
    every run (the tests' fixture settings)."""
    db = work_dir / "warehouse.duckdb"
    base = extra_vars or {}
    _dbt(["build", "--vars", json.dumps(base)], silver_dir=silver_dir, db=db, work_dir=work_dir)
    history = _query(db, HISTORY_SQL.format(last=max(seasons)))
    league = league_runs_distribution(history, seasons)

    sweep: dict[Setting, dict[str, Difference]] = {}
    slice_diffs: dict[str, dict[str, Difference]] = {}
    counts = (0, 0)
    for setting in settings:
        _dbt(
            [
                "run",
                "--select",
                "int_player_ratings_long+",
                "--vars",
                json.dumps({**base, **setting.dbt_vars()}),
            ],
            silver_dir=silver_dir,
            db=db,
            work_dir=work_dir,
        )
        loaded = load_split_balls(db, seasons)
        balls = complete_cases(loaded)
        sweep[setting] = paired_difference(compare(balls, league), n_resamples=n_resamples)
        total = sweep[setting]["total"]
        print(f"{setting.label()}: {total.diff:+.2f} [{total.low:+.2f}, {total.high:+.2f}]")
        if setting == DEFAULT:
            counts = (len(balls), len(loaded) - len(balls))
            for name, part in slices(balls).items():
                if not part.empty:
                    slice_diffs[name] = paired_difference(
                        compare(part, league), n_resamples=n_resamples
                    )
    return SweepResult(sweep, slice_diffs, *counts)


def render_markdown(result: SweepResult) -> str:
    chosen = result.sweep[DEFAULT]["total"]
    lines = [
        "# Pace/spin batting split: does it predict better?",
        "",
        "Generated by `make split-eval`; every number below comes from that run. Do not edit",
        "by hand. Method: `cricket_value/ratings/split_eval.py` and DECISIONS.md section 7.",
        "",
        f"**Scored:** {result.balls:,} balls faced in IPL {min(EVAL_SEASONS)}-{max(EVAL_SEASONS)} "
        "against bowlers of known bowling kind (no super overs, no wides), where every rating",
        f"exists ({result.dropped:,} balls dropped for a missing rating). Both scores use the same",
        "bowler rating and scoring rule; only the batter's rating differs: against all bowlers,",
        "or against this bowler's kind. 2025-2026 are locked and never read.",
        "",
        "Difference = split minus unsplit log loss, in nats per 1,000 balls, with a bootstrap",
        "95% interval over matches. **Negative means the split predicts better.**",
        "",
        f"## Chosen design: shrink toward {DEFAULT.label()}",
        "",
        f"Overall {chosen.diff:+.2f} [{chosen.low:+.2f}, {chosen.high:+.2f}].",
        "",
        "| Slice | Dismissal | Runs | Total | 95% interval (total) |",
        "|---|---|---|---|---|",
    ]
    for name, diff in result.slices.items():
        total = diff["total"]
        lines.append(
            f"| {name} | {diff['dismissal'].diff:+.2f} | {diff['runs'].diff:+.2f} "
            f"| {total.diff:+.2f} | [{total.low:+.2f}, {total.high:+.2f}] |"
        )
    lines += [
        "",
        "## What the split rating shrinks toward",
        "",
        "All balls, the same scoring, only the split rating's prior changes. The planned design",
        "(role average) is the first row.",
        "",
        "| Prior | Dismissal | Runs | Total | 95% interval (total) |",
        "|---|---|---|---|---|",
    ]
    for setting, diff in result.sweep.items():
        total = diff["total"]
        mark = " (chosen)" if setting == DEFAULT else ""
        lines.append(
            f"| {setting.label()}{mark} | {diff['dismissal'].diff:+.2f} "
            f"| {diff['runs'].diff:+.2f} | {total.diff:+.2f} "
            f"| [{total.low:+.2f}, {total.high:+.2f}] |"
        )
    best = min(
        (s for s in result.sweep if s.shrink_to == "overall"),
        key=lambda s: result.sweep[s]["total"].diff,
    )
    lines += [
        "",
        f"Best of the own-rating strengths on these balls: {best.label()}. The strength is "
        "tuned on the same seasons it is scored on, so its interval is a little optimistic.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-dir", type=Path, default=REPO / "data" / "silver")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        result = run_sweep(args.silver_dir, Path(tmp))
    text = render_markdown(result)
    out = REPO / "reports" / "pacespin_split.md"
    out.write_text(text)
    print(text)
    print(f"wrote {out.relative_to(REPO)}")


if __name__ == "__main__":
    main()
