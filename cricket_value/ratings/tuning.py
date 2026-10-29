"""Tune the ratings' half-life and T20I weight on next-ball log-likelihood.

Protocol (DECISIONS.md section 7):

- **What is predicted.** Every ball faced in an IPL match (no super overs, no wides) from
  seasons 2010-2024: did the batter get out, and how many runs off the bat. 2008-2009 are
  burn-in (no ratings yet). **2025 and 2026 are locked**: their outcomes are never read.
- **Inputs.** The batter's and bowler's as-of ratings for that phase, from
  ``int_player_ratings_long``. Each uses only matches before the ball's match date, so the
  scores are out of sample by construction.
- **Combining batter and bowler.** The batter's rate is the base, and the bowler moves it
  by how far he is from the average bowler (the year's pooled prior mean):
  - dismissal, on the log-odds scale:  logit p = logit p_bat + logit p_bowl - logit m_bowl
  - runs, on the ratio scale:          mu = mu_bat * mu_bowl / m_bowl
  An average bowler leaves the batter's rate unchanged. The bowler is measured against his
  own stat's mean because the two stats differ slightly (run outs count against the
  batter, not the bowler; the bowler is also charged extras).
- **Scoring runs.** Runs per ball are lumpy (0, 1, 4 and 6 are common; 3 and 5 rare), so
  a Gaussian (which is just MSE) fits them badly. Each ball gets a full distribution over
  0-6 runs: the league's distribution for that phase from earlier seasons, tilted
  exponentially (p_k proportional to q_k * e^(theta*k)) until its mean is mu. That is the
  distribution closest to the league's (in KL divergence) with the predicted mean.
- **Criterion.** Log loss per ball = -(log p(dismissal outcome) + log p(runs)), the joint
  log-likelihood if the two outcomes are independent given the ratings. Both parts are
  log-probabilities of discrete outcomes, so they are in the same units (nats) and add.
- **Uncertainty.** A bootstrap over matches (balls in a match share players and
  conditions) gives how often each grid point would win, and paired intervals between
  the chosen point and its grid neighbours.

Every grid point rebuilds the rating models in a scratch warehouse (``dbt run --select
int_rating_evidence+`` with ``--vars``), so the real ``data/warehouse.duckdb`` is never
touched. Run with ``make tune-ratings``; writes ``reports/ratings_tuning.md``.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import numpy.typing as npt
import pandas as pd
from dbt.artifacts.schemas.run import RunExecutionResult

from cricket_value.warehouse.dbt import run_dbt

Array = npt.NDArray[np.float64]

REPO = Path(__file__).resolve().parents[2]

HALF_LIVES = [90, 180, 365, 540, 730, 1095, 1460, 2190]
T20I_WEIGHTS = [0.0, 0.25, 0.5, 0.75, 1.0]
EVAL_SEASONS = range(2010, 2025)
LOCKED_SEASONS = (2025, 2026)
# Runs off the bat per ball; the rare 7 (overthrows) is counted as 6
RUNS_SUPPORT = np.arange(7, dtype=np.float64)
EPS = 1e-15


@dataclass(frozen=True, order=True)
class GridPoint:
    half_life_days: int
    t20i_weight: float

    def dbt_vars(self) -> dict[str, float]:
        return {
            "rating_half_life_days": self.half_life_days,
            "rating_t20i_weight": self.t20i_weight,
        }

    def label(self) -> str:
        return f"{self.half_life_days} days, T20I weight {self.t20i_weight:g}"


# --- Scoring rule ----------------------------------------------------------------------


def _logit(p: Array) -> Array:
    p = np.clip(p, EPS, 1 - EPS)
    out: Array = np.log(p / (1 - p))
    return out


def combine_rates(p_bat: Array, p_bowl: Array, bowl_mean: Array) -> Array:
    """Batter's rate moved by the bowler's odds ratio against the average bowler."""
    z = _logit(p_bat) + _logit(p_bowl) - _logit(bowl_mean)
    out: Array = 1 / (1 + np.exp(-z))
    return out


def combine_means(mu_bat: Array, mu_bowl: Array, bowl_mean: Array) -> Array:
    """Batter's runs per ball scaled by the bowler's ratio to the average bowler."""
    out: Array = mu_bat * mu_bowl / bowl_mean
    return out


def tilt(q: Array, mu: Array, support: Array = RUNS_SUPPORT) -> Array:
    """Each row of ``q`` tilted to p_k proportional to q_k * e^(theta*k), with mean ``mu``.

    The tilted mean rises with theta, so bisection always finds it. ``mu`` must lie strictly
    inside the support. Works row by row on (balls x outcomes) arrays.
    """
    with np.errstate(divide="ignore"):
        log_q = np.log(q)

    def probs(theta: Array) -> Array:
        z = log_q + theta[:, None] * support[None, :]
        z -= z.max(axis=1, keepdims=True)
        e = np.exp(z)
        out: Array = e / e.sum(axis=1, keepdims=True)
        return out

    lo, hi = np.full(len(mu), -30.0), np.full(len(mu), 30.0)
    for _ in range(80):
        mid = (lo + hi) / 2
        too_high = probs(mid) @ support > mu
        hi = np.where(too_high, mid, hi)
        lo = np.where(too_high, lo, mid)
    return probs((lo + hi) / 2)


def league_runs_distribution(balls: pd.DataFrame, seasons: Iterable[int]) -> pd.DataFrame:
    """Share of balls going for 0-6 runs, per (season, phase), from earlier seasons only.

    ``balls`` has season, phase and runs_batter. Indexed by (season, phase), one column per
    run value.
    """
    counts = (
        balls.assign(runs=balls["runs_batter"].clip(upper=len(RUNS_SUPPORT) - 1))
        .groupby(["season", "phase", "runs"])
        .size()
        .unstack("runs", fill_value=0)
        .reindex(columns=range(len(RUNS_SUPPORT)), fill_value=0)
    )
    rows = {}
    for season in seasons:
        before = counts[counts.index.get_level_values("season") < season].groupby("phase").sum()
        for phase, row in before.iterrows():
            rows[(season, phase)] = row / row.sum()
    out = pd.DataFrame.from_dict(rows, orient="index")
    out.index = pd.MultiIndex.from_tuples(out.index, names=["season", "phase"])
    return out


def score_balls(balls: pd.DataFrame, league: pd.DataFrame) -> pd.DataFrame:
    """Log-likelihood of each ball's dismissal and runs outcome under the ratings.

    ``balls`` holds the outcomes (runs_batter, batter_dismissed) and ratings (bat_dismissal,
    bowl_wicket, bowl_wicket_mean, bat_runs, bowl_runs, bowl_runs_mean) per ball; ``league``
    is a ``league_runs_distribution``.
    """
    p_out = combine_rates(
        balls["bat_dismissal"].to_numpy(float),
        balls["bowl_wicket"].to_numpy(float),
        balls["bowl_wicket_mean"].to_numpy(float),
    )
    out = balls["batter_dismissed"].to_numpy(bool)
    ll_dismissal = np.log(np.clip(np.where(out, p_out, 1 - p_out), EPS, None))

    mu = combine_means(
        balls["bat_runs"].to_numpy(float),
        balls["bowl_runs"].to_numpy(float),
        balls["bowl_runs_mean"].to_numpy(float),
    )
    keys = pd.MultiIndex.from_arrays([balls["season"], balls["phase"]])
    q = league.reindex(keys).to_numpy(float)
    if np.isnan(q).any():
        raise ValueError("no league runs distribution for some (season, phase)")
    p_runs = tilt(q, mu)
    k = balls["runs_batter"].clip(upper=len(RUNS_SUPPORT) - 1).to_numpy(int)
    ll_runs = np.log(np.clip(p_runs[np.arange(len(k)), k], EPS, None))

    rated = balls["rated"].to_numpy(bool) if "rated" in balls else np.ones(len(balls), bool)
    return pd.DataFrame(
        {
            "match_id": balls["match_id"].to_numpy(),
            "rated": rated,
            "ll_dismissal": ll_dismissal,
            "ll_runs": ll_runs,
        }
    )


RATING_COLUMNS = [
    "bat_dismissal",
    "bowl_wicket",
    "bowl_wicket_mean",
    "bat_runs",
    "bowl_runs",
    "bowl_runs_mean",
]


def _league_inputs(balls: pd.DataFrame, league: pd.DataFrame) -> pd.DataFrame:
    """Rating inputs that say nothing about the players: an average batter (the league's
    earlier-season rates for the phase, ``balls["league_dismissal"]`` for dismissals)
    facing an average bowler (rating equal to its mean, so the bowler term cancels)."""
    keys = pd.MultiIndex.from_arrays([balls["season"], balls["phase"]])
    return pd.DataFrame(
        {
            "bat_dismissal": balls["league_dismissal"].to_numpy(float),
            "bowl_wicket": 0.5,
            "bowl_wicket_mean": 0.5,
            "bat_runs": league.reindex(keys).to_numpy(float) @ RUNS_SUPPORT,
            "bowl_runs": 1.0,
            "bowl_runs_mean": 1.0,
        },
        index=balls.index,
    )


def fall_back_to_league(balls: pd.DataFrame, league: pd.DataFrame) -> pd.DataFrame:
    """Balls missing any rating get the league-only inputs, and ``rated`` says which.

    A rating is missing when its year has no prior: with a short half-life, too few players
    keep ``rating_min_prior_balls`` decayed balls by 1 January to estimate one. Predicting
    those balls from the league is what such a pipeline actually knows, and keeps every
    grid point scored on the same balls.
    """
    rated = balls[RATING_COLUMNS].notna().all(axis=1)
    out = balls.copy()
    out.loc[~rated, RATING_COLUMNS] = _league_inputs(balls[~rated], league)
    out["rated"] = rated
    return out


def score_league_only(balls: pd.DataFrame, league: pd.DataFrame) -> pd.DataFrame:
    """The same scores with no player information: the reference for what ratings add."""
    flat = balls.assign(**_league_inputs(balls, league), rated=False)
    return score_balls(flat, league)


# --- Aggregation and uncertainty -------------------------------------------------------


def match_totals(scored: pd.DataFrame) -> pd.DataFrame:
    """Balls and summed log-likelihoods per match: the unit the bootstrap resamples."""
    return scored.groupby("match_id").agg(
        balls=("ll_dismissal", "size"),
        rated=("rated", "sum"),
        ll_dismissal=("ll_dismissal", "sum"),
        ll_runs=("ll_runs", "sum"),
    )


def summarise(grid: dict[GridPoint, pd.DataFrame]) -> pd.DataFrame:
    """Log loss per ball (lower is better) for each grid point, best first."""
    rows = []
    for point, totals in grid.items():
        balls = totals["balls"].sum()
        dismissal = -totals["ll_dismissal"].sum() / balls
        runs = -totals["ll_runs"].sum() / balls
        rows.append(
            {
                "half_life_days": point.half_life_days,
                "t20i_weight": point.t20i_weight,
                "balls": int(balls),
                "rated_share": totals["rated"].sum() / balls,
                "dismissal": dismissal,
                "runs": runs,
                "total": dismissal + runs,
            }
        )
    return pd.DataFrame(rows).sort_values("total", ignore_index=True)


def _resampled_loss(
    grid: dict[GridPoint, pd.DataFrame], n_resamples: int, seed: int
) -> tuple[list[GridPoint], Array]:
    """Total log loss per ball for every grid point on each bootstrap resample of matches,
    as a (resamples x grid points) array. Every point sees the same resampled matches."""
    points = list(grid)
    first = grid[points[0]]
    balls = first["balls"].to_numpy(float)
    ll = np.column_stack(
        [
            (grid[p]["ll_dismissal"] + grid[p]["ll_runs"]).reindex(first.index).to_numpy(float)
            for p in points
        ]
    )
    rng = np.random.default_rng(seed)
    # how many times each match is drawn in each resample
    counts = rng.multinomial(len(balls), np.full(len(balls), 1 / len(balls)), size=n_resamples)
    loss: Array = -(counts @ ll) / (counts @ balls)[:, None]
    return points, loss


def bootstrap_choice(
    grid: dict[GridPoint, pd.DataFrame], *, n_resamples: int = 1000, seed: int = 0
) -> dict[GridPoint, float]:
    """Share of bootstrap resamples on which each grid point has the lowest log loss."""
    points, loss = _resampled_loss(grid, n_resamples, seed)
    wins = np.bincount(loss.argmin(axis=1), minlength=len(points)) / len(loss)
    return {p: float(s) for p, s in zip(points, wins, strict=True) if s > 0}


def neighbour_differences(
    grid: dict[GridPoint, pd.DataFrame],
    best: GridPoint,
    *,
    n_resamples: int = 1000,
    seed: int = 0,
) -> pd.DataFrame:
    """Paired bootstrap of (neighbour - best) log loss per 1,000 balls, for the grid points
    one step away from ``best`` along each axis."""
    half_lives = sorted({p.half_life_days for p in grid})
    weights = sorted({p.t20i_weight for p in grid})
    h, w = half_lives.index(best.half_life_days), weights.index(best.t20i_weight)
    near = [GridPoint(half_lives[i], best.t20i_weight) for i in (h - 1, h + 1)
            if 0 <= i < len(half_lives)]  # fmt: skip
    near += [GridPoint(best.half_life_days, weights[i]) for i in (w - 1, w + 1)
             if 0 <= i < len(weights)]  # fmt: skip
    points, loss = _resampled_loss(grid, n_resamples, seed)
    full = summarise(grid).set_index(["half_life_days", "t20i_weight"])["total"]
    rows = []
    for p in near:
        d = 1000 * (loss[:, points.index(p)] - loss[:, points.index(best)])
        low, high = np.percentile(d, [2.5, 97.5])
        diff = 1000 * (
            full[(p.half_life_days, p.t20i_weight)] - full[(best.half_life_days, best.t20i_weight)]
        )
        rows.append({"neighbour": p.label(), "diff": diff, "ci_low": low, "ci_high": high})
    return pd.DataFrame(rows)


def edges_hit(best: GridPoint, half_lives: list[int], weights: list[float]) -> list[str]:
    """Which grid edges the chosen point sits on. An edge choice means: extend the grid."""
    out = []
    for name, value, axis in (
        ("half_life_days", best.half_life_days, half_lives),
        ("t20i_weight", best.t20i_weight, weights),
    ):
        if value == min(axis):
            out.append(f"{name} at the low edge ({value:g})")
        elif value == max(axis):
            out.append(f"{name} at the high edge ({value:g})")
    return out


# --- Loading from the warehouse --------------------------------------------------------

EVAL_BALLS_SQL = """
    select match_id, season, match_date, phase, batter_id, bowler_id, runs_batter,
        batter_dismissed
    from fact_delivery
    where competition = 'ipl' and not is_super_over and extras_wides = 0
      and season between {first} and {last}
"""

RATED_BALLS_SQL = """
    with balls as ({eval_balls}),
    long as (select * from int_player_ratings_long),
    priors as (
        select * from int_rating_priors where role = 'any' and skill = 'bowl' and vs = 'all'
    )
    select b.match_id, b.season, b.phase, b.runs_batter, b.batter_dismissed,
        bd.rating as bat_dismissal, br.rating as bat_runs,
        ww.rating as bowl_wicket, wr.rating as bowl_runs,
        pw.prior_mean as bowl_wicket_mean, pr.prior_mean as bowl_runs_mean
    from balls b
    left join long bd on bd.player_id = b.batter_id and bd.match_date = b.match_date
        and bd.phase = b.phase and bd.skill = 'bat' and bd.stat = 'dismissal_rate'
        and bd.vs = 'all'
    left join long br on br.player_id = b.batter_id and br.match_date = b.match_date
        and br.phase = b.phase and br.skill = 'bat' and br.stat = 'runs_per_ball'
        and br.vs = 'all'
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

# League history for the runs baseline: every IPL ball faced up to the last scored season
HISTORY_SQL = """
    select season, phase, runs_batter, batter_dismissed
    from fact_delivery
    where competition = 'ipl' and not is_super_over and extras_wides = 0 and season <= {last}
"""


def _eval_sql(seasons: range) -> str:
    if set(seasons) & set(LOCKED_SEASONS):
        raise ValueError(f"seasons {LOCKED_SEASONS} are locked for the final test")
    return EVAL_BALLS_SQL.format(first=min(seasons), last=max(seasons))


def _query(db: Path, sql: str) -> pd.DataFrame:
    # dbt's connection settings, so the next dbt run can reopen the file
    with duckdb.connect(str(db)) as con:
        out: pd.DataFrame = con.sql(sql).df()
    return out


def load_rated_balls(db: Path, seasons: range = EVAL_SEASONS) -> pd.DataFrame:
    """Every evaluation ball with its batter's and bowler's ratings for that phase, null
    where the pipeline has none (see ``fall_back_to_league``)."""
    eval_balls = _eval_sql(seasons)
    rated = _query(db, RATED_BALLS_SQL.format(eval_balls=eval_balls))
    n = int(_query(db, f"select count(*) as n from ({eval_balls})")["n"].iloc[0])
    if len(rated) != n:
        raise ValueError(f"the rating joins turned {n} evaluation balls into {len(rated)}")
    return rated.sort_values(["match_id"], kind="stable", ignore_index=True)


def league_dismissal_rate(history: pd.DataFrame, seasons: Iterable[int]) -> pd.Series:
    """Dismissals per ball faced, per (season, phase), from earlier seasons only."""
    rows = {}
    for season in seasons:
        before = history[history["season"] < season]
        for phase, rate in before.groupby("phase")["batter_dismissed"].mean().items():
            rows[(season, phase)] = float(rate)
    out = pd.Series(rows, name="league_dismissal")
    out.index = pd.MultiIndex.from_tuples(out.index, names=["season", "phase"])
    return out


# --- Running the grid ------------------------------------------------------------------


def _dbt(args: list[str], *, silver_dir: Path, db: Path, work_dir: Path) -> None:
    result = run_dbt(args, silver_dir=silver_dir, warehouse=db, work_dir=work_dir)
    run = result.result
    failed = (
        [f"{r.node.name}: {r.message}" for r in run if str(r.status) in ("error", "fail")]
        if isinstance(run, RunExecutionResult)
        else []
    )
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed: {result.exception or failed}")


@dataclass
class TuningResult:
    grid: dict[GridPoint, pd.DataFrame]
    league_only: pd.DataFrame
    half_lives: list[int]
    weights: list[float]


def run_grid(
    silver_dir: Path,
    work_dir: Path,
    *,
    half_lives: list[int] = HALF_LIVES,
    weights: list[float] = T20I_WEIGHTS,
    seasons: range = EVAL_SEASONS,
) -> TuningResult:
    """Build a scratch warehouse in ``work_dir``, then rebuild and score the ratings for
    every grid point."""
    db = work_dir / "warehouse.duckdb"
    _dbt(["build"], silver_dir=silver_dir, db=db, work_dir=work_dir)

    history = _query(db, HISTORY_SQL.format(last=max(seasons)))
    league = league_runs_distribution(history, seasons)
    rates = league_dismissal_rate(history, seasons)

    def rated_balls() -> pd.DataFrame:
        balls = load_rated_balls(db, seasons)
        keys = pd.MultiIndex.from_arrays([balls["season"], balls["phase"]])
        return balls.assign(league_dismissal=rates.reindex(keys).to_numpy())

    grid: dict[GridPoint, pd.DataFrame] = {}
    for half_life in half_lives:
        for weight in weights:
            point = GridPoint(half_life, weight)
            selector = ["--select", "int_rating_evidence+"]
            _dbt(["run", *selector, "--vars", json.dumps(point.dbt_vars())], silver_dir=silver_dir,
                 db=db, work_dir=work_dir)  # fmt: skip
            balls = fall_back_to_league(rated_balls(), league)
            grid[point] = match_totals(score_balls(balls, league))
            row = summarise({point: grid[point]}).iloc[0]
            print(f"{point.label()}: {row.total:.5f} ({row.rated_share:.1%} rated)")

    league_only = match_totals(score_league_only(balls, league))
    return TuningResult(grid, league_only, half_lives, weights)


# --- Report ----------------------------------------------------------------------------


def _pivot(table: pd.DataFrame, column: str) -> pd.DataFrame:
    """Excess log loss over the grid's best, per 1,000 balls, half-life by T20I weight."""
    excess = table.assign(excess=1000 * (table[column] - table[column].min()))
    out = excess.pivot(index="half_life_days", columns="t20i_weight", values="excess")
    out.index.name = "half-life (days)"
    out.columns = [f"T20I {w:g}" for w in out.columns]
    return out


def render_markdown(result: TuningResult, *, n_resamples: int = 1000) -> str:
    table = summarise(result.grid)
    top = table.iloc[0]
    best = GridPoint(int(top.half_life_days), float(top.t20i_weight))
    edges = edges_hit(best, result.half_lives, result.weights)
    share = bootstrap_choice(result.grid, n_resamples=n_resamples)
    near = neighbour_differences(result.grid, best, n_resamples=n_resamples)
    base = summarise({best: result.league_only}).iloc[0]

    lines = [
        "# Ratings tuning: half-life and T20I weight",
        "",
        "Generated by `make tune-ratings`; every number below comes from that run. Do not edit",
        "by hand. Method: `cricket_value/ratings/tuning.py` and DECISIONS.md section 7.",
        "",
        f"**Scored:** {int(top.balls):,} balls faced in IPL {min(EVAL_SEASONS)}-"
        f"{max(EVAL_SEASONS)} (no super overs, no wides),",
        "each predicted from its batter's and bowler's as-of ratings for that phase.",
        "2025-2026 are locked and never read.",
        "",
        "**Criterion:** log loss per ball, in nats (lower is better): the negative",
        "log-likelihood of whether the batter was out (log-odds combination of batter and",
        "bowler) plus how many runs he scored (the league's 0-6 distribution for the phase,",
        "tilted to the combined mean).",
        "",
        "## Chosen values",
        "",
        f"- **Half-life:** {best.half_life_days} days",
        f"- **T20I weight:** {best.t20i_weight:g}",
        f"- **Log loss per ball:** {top.total:.5f} (dismissal {top.dismissal:.5f}, "
        f"runs {top.runs:.5f})",
        f"- **League only (no player ratings):** {base.total:.5f} (dismissal "
        f"{base.dismissal:.5f}, runs {base.runs:.5f}), so the ratings save "
        f"{1000 * (base.total - top.total):.1f} nats per 1,000 balls",
        "- **Grid edges:** "
        + ("; ".join(edges) + ". Extend the grid." if edges else "none, the optimum is interior"),
        f"- **Bootstrap ({n_resamples:,} resamples of matches), share of resamples each point "
        "wins:** "
        + ", ".join(
            f"{p.label()} {s:.0%}" for p, s in sorted(share.items(), key=lambda kv: -kv[1])[:5]
        ),
        "",
        "## How flat is it near the optimum?",
        "",
        "Neighbour minus chosen, in nats per 1,000 balls, with a paired bootstrap 95% interval",
        "over matches. Positive = the neighbour is worse.",
        "",
        "| Neighbour | Difference | 95% interval |",
        "|---|---|---|",
    ]
    for r in near.itertuples(index=False):
        lines.append(f"| {r.neighbour} | {r.diff:+.3f} | [{r.ci_low:+.3f}, {r.ci_high:+.3f}] |")

    for title, column in (
        ("Total", "total"),
        ("Dismissal component", "dismissal"),
        ("Runs component", "runs"),
    ):
        lines += [
            "",
            f"## {title}: excess log loss over the grid's best, per 1,000 balls",
            "",
            _pivot(table, column).round(2).to_markdown(),
        ]

    coverage = table.pivot(index="half_life_days", columns="t20i_weight", values="rated_share")
    coverage.index.name = "half-life (days)"
    coverage.columns = [f"T20I {w:g}" for w in coverage.columns]
    lines += [
        "",
        "## Share of balls with both players rated",
        "",
        "With a short half-life, too few players keep 60 decayed balls by 1 January to estimate",
        "that year's priors, so some years have no ratings. Those balls are scored as",
        "league-only, which is part of why short half-lives score badly.",
        "",
        (100 * coverage).round(1).to_markdown(),
    ]

    lines += [
        "",
        "## Ten best grid points (log loss per ball)",
        "",
        "| Half-life (days) | T20I weight | Dismissal | Runs | Total |",
        "|---|---|---|---|---|",
    ]
    for r in table.head(10).itertuples(index=False):
        lines.append(
            f"| {r.half_life_days} | {r.t20i_weight:g} | {r.dismissal:.5f} | {r.runs:.5f} "
            f"| {r.total:.5f} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--silver-dir", type=Path, default=REPO / "data" / "silver")
    parser.add_argument("--report", type=Path, default=REPO / "reports" / "ratings_tuning.md")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        result = run_grid(args.silver_dir.resolve(), Path(tmp))
    args.report.write_text(render_markdown(result))
    print(summarise(result.grid).head(10).to_string(index=False))
    print(f"wrote {args.report}")


if __name__ == "__main__":
    main()
