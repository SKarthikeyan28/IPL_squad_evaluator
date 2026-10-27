"""Elo baseline: tune on the past, score on the next season, compare with simple baselines.

Protocol (DECISIONS.md section 8):

- **Rolling origin.** For each test season N in 2015-2024, Elo's parameters are chosen by
  log loss on seasons 2009 to N-1 only, then Elo predicts season N. 2008 is burn-in:
  every team starts level, so its forecasts say little.
- **Locked seasons.** 2025 and 2026 are never scored here. They are kept for one final
  evaluation at the end of the project.
- **Baselines** use the same rule: their one number is estimated from earlier seasons.
  - ``coin_flip``: every match 50/50.
  - ``home_rate``: the home side wins with the home-win rate of earlier seasons;
    50/50 at neutral grounds.
  - ``toss_rate``: the toss winner wins with the toss-winner win rate of earlier seasons.
- **Scoring:** log loss and Brier score (lower is better), accuracy second. Differences
  between models come with paired bootstrap 95% intervals over matches.

No-result matches are excluded from scoring. Ties decided by a super over count as a
win for the super-over winner (the official result).

Run with ``make elo``; writes ``reports/elo_baseline.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from cricket_value.models.elo import EloParams, run_elo
from cricket_value.models.metrics import accuracy, brier, log_loss, paired_bootstrap
from cricket_value.reference.home import match_home_status
from cricket_value.reference.reference import Reference, load_reference

REPO = Path(__file__).resolve().parents[2]

DEFAULT_GRID = [
    EloParams(k=k, home_advantage=h, season_regression=r)
    for k in (2, 4, 6, 8, 12, 16, 20, 24, 32, 40)
    for h in (0, 15, 30, 45, 60, 90)
    for r in (0.0, 0.2, 1 / 3, 0.5, 0.75)
]
FIRST_SCORED_SEASON = 2009
TEST_SEASONS = list(range(2015, 2025))
LOCKED_SEASONS = (2025, 2026)
MODELS = ["coin_flip", "home_rate", "toss_rate", "elo"]


def games_from_silver(matches: pd.DataFrame, ref: Reference | None = None) -> pd.DataFrame:
    """One row per IPL match, teams as franchise ids, from team A's (team1's) side."""
    ref = ref if ref is not None else load_reference(REPO / "config" / "reference")
    ipl = matches[matches["competition"] == "ipl"]
    home = match_home_status(ipl, ref)
    home_a = home.merge(
        ipl[["match_id", "team1"]], left_on=["match_id", "team"], right_on=["match_id", "team1"]
    )
    franchise = ref.team_aliases.set_index("team_name")["franchise_id"]

    g = ipl[["match_id", "season", "match_date"]].copy()
    g["franchise_a"] = ipl["team1"].map(franchise)
    g["franchise_b"] = ipl["team2"].map(franchise)
    g["winner"] = ipl["winner"].map(franchise).astype(object).where(ipl["winner"].notna(), None)
    g["home_a"] = g["match_id"].map(home_a.set_index("match_id")["home_status"])
    g["toss_winner_is_a"] = (ipl["toss_winner"] == ipl["team1"]).astype(bool)
    if g[["franchise_a", "franchise_b", "home_a"]].isna().any().any():
        raise ValueError("unmapped IPL team names: run `make reference-audit`")
    g["season"] = g["season"].astype("int64")
    return g.sort_values(["match_date", "match_id"], ignore_index=True)


def tune(
    games: pd.DataFrame,
    *,
    test_season: int,
    grid: list[EloParams],
    first_scored_season: int = FIRST_SCORED_SEASON,
) -> EloParams:
    """The grid point with the lowest log loss on seasons before ``test_season``.

    Only games before the test season are passed to Elo, so nothing from that season
    or later can influence the choice. Ties go to the earlier grid point.
    """
    train = games[games["season"] < test_season]
    best, best_loss = grid[0], np.inf
    for params in grid:
        preds, _ = run_elo(train, params)
        scored = preds[(preds["season"] >= first_scored_season) & preds["outcome_a"].notna()]
        loss = log_loss(scored["outcome_a"].to_numpy(), scored["p_a"].to_numpy())
        if loss < best_loss:
            best, best_loss = params, loss
    return best


def rate_baselines(games: pd.DataFrame, test_season: int) -> pd.DataFrame:
    """Home-rate and toss-rate forecasts for one test season, estimated from earlier ones."""
    past = games[(games["season"] < test_season) & games["winner"].notna()]
    a_won = past["winner"] == past["franchise_a"]

    decided_home = past["home_a"] != "neutral"
    home_side_won = np.where(past["home_a"] == "home", a_won, ~a_won)[decided_home.to_numpy()]
    home_rate = float(home_side_won.mean()) if len(home_side_won) else 0.5

    toss_side_won = np.where(past["toss_winner_is_a"], a_won, ~a_won)
    toss_rate = float(toss_side_won.mean()) if len(toss_side_won) else 0.5

    test = games[games["season"] == test_season]
    sign = test["home_a"].map({"home": 1.0, "away": -1.0, "neutral": 0.0}).to_numpy()
    return pd.DataFrame(
        {
            "match_id": test["match_id"].to_numpy(),
            "p_home_rate": 0.5 + sign * (home_rate - 0.5),
            "p_toss_rate": np.where(test["toss_winner_is_a"], toss_rate, 1 - toss_rate),
        }
    )


@dataclass
class EvalResult:
    predictions: pd.DataFrame
    per_season: pd.DataFrame
    pooled: pd.DataFrame
    comparisons: pd.DataFrame
    params: dict[int, EloParams] = field(default_factory=dict)


def evaluate(
    games: pd.DataFrame,
    *,
    test_seasons: list[int],
    grid: list[EloParams],
    first_scored_season: int = FIRST_SCORED_SEASON,
    n_resamples: int = 2000,
    seed: int = 0,
) -> EvalResult:
    parts, params = [], {}
    for season in test_seasons:
        chosen = tune(games, test_season=season, grid=grid, first_scored_season=first_scored_season)
        params[season] = chosen
        # Elo is online: running it to the end of the test season still predicts each
        # test match from earlier matches only
        preds, _ = run_elo(games[games["season"] <= season], chosen)
        preds = preds[preds["season"] == season].merge(rate_baselines(games, season), on="match_id")
        parts.append(preds.rename(columns={"p_a": "p_elo"}))

    pred = pd.concat(parts, ignore_index=True)
    pred = pred[pred["outcome_a"].notna()].reset_index(drop=True)
    pred["p_coin_flip"] = 0.5

    def scores(df: pd.DataFrame) -> list[dict[str, object]]:
        y = df["outcome_a"].to_numpy(dtype=float)
        out = []
        for model in MODELS:
            p = df[f"p_{model}"].to_numpy(dtype=float)
            out.append(
                {
                    "model": model,
                    "n": len(df),
                    "log_loss": log_loss(y, p),
                    "brier": brier(y, p),
                    "accuracy": accuracy(y, p),
                }
            )
        return out

    per_season = pd.DataFrame(
        [{"season": s, **row} for s, df in pred.groupby("season") for row in scores(df)]
    )
    pooled = pd.DataFrame(scores(pred))

    y = pred["outcome_a"].to_numpy(dtype=float)
    comparisons = []
    for baseline in ["coin_flip", "home_rate", "toss_rate"]:
        for metric in ("log_loss", "brier"):
            r = paired_bootstrap(
                y,
                pred["p_elo"].to_numpy(dtype=float),
                pred[f"p_{baseline}"].to_numpy(dtype=float),
                metric=metric,
                n_resamples=n_resamples,
                seed=seed,
            )
            comparisons.append(
                {
                    "vs": baseline,
                    "metric": metric,
                    "elo_minus_baseline": r.diff,
                    "ci_low": r.low,
                    "ci_high": r.high,
                }
            )
    return EvalResult(pred, per_season, pooled, pd.DataFrame(comparisons), params)


def render_markdown(
    result: EvalResult, final: EloParams, ratings: dict[str, float], ref: Reference
) -> str:
    names = ref.franchises.set_index("franchise_id")["franchise_name"]
    lines = [
        "# Elo baseline",
        "",
        "Generated by `make elo`; every number below comes from that run. Do not edit by hand.",
        "",
        "**Protocol:** for each test season N (2015-2024), Elo's K, home bonus and season",
        "pull-back are chosen by log loss on seasons 2009 to N-1 only; baselines are estimated",
        "from earlier seasons too. 2025-2026 are locked and not scored. No-results excluded.",
        "Intervals are paired bootstrap 95% intervals over matches (2,000 resamples).",
        "",
        "## Pooled over test seasons 2015-2024",
        "",
        "Lower is better for log loss and Brier score. A coin flip scores 0.693 and 0.250.",
        "",
        "| Model | Matches | Log loss | Brier | Accuracy |",
        "|---|---|---|---|---|",
    ]
    for r in result.pooled.itertuples(index=False):
        lines.append(f"| {r.model} | {r.n} | {r.log_loss:.4f} | {r.brier:.4f} | {r.accuracy:.3f} |")

    lines += [
        "",
        "## Is Elo better than each baseline?",
        "",
        "Negative = Elo's loss is lower. Only an interval entirely below zero is a real gain.",
        "",
        "| Elo vs | Metric | Elo minus baseline | 95% interval | Verdict |",
        "|---|---|---|---|---|",
    ]
    for c in result.comparisons.to_dict("records"):
        low, high = float(c["ci_low"]), float(c["ci_high"])
        verdict = "Elo better" if high < 0 else ("Elo worse" if low > 0 else "no clear difference")
        lines.append(
            f"| {c['vs']} | {c['metric']} | {float(c['elo_minus_baseline']):+.4f} "
            f"| [{low:+.4f}, {high:+.4f}] | {verdict} |"
        )

    lines += ["", "## Per season (log loss)", ""]
    table = result.per_season.pivot(index="season", columns="model", values="log_loss")[MODELS]
    n = result.per_season.groupby("season")["n"].first()
    table.insert(0, "matches", n)
    lines.append(table.round(4).to_markdown())

    lines += [
        "",
        "## Parameters chosen for each test season",
        "",
        "| Test season | K | Home bonus (points) | Season pull-back |",
        "|---|---|---|---|",
    ]
    for season, p in result.params.items():
        lines.append(f"| {season} | {p.k:g} | {p.home_advantage:g} | {p.season_regression:.2f} |")

    lines += [
        "",
        f"## Current ratings (after the last match; parameters tuned on 2009-2024: K={final.k:g}, "
        f"home bonus={final.home_advantage:g}, pull-back={final.season_regression:.2f})",
        "",
        "Includes the locked 2025-2026 matches as rating updates only; they are not scored.",
        "",
        "| Franchise | Rating |",
        "|---|---|",
    ]
    current = set(ref.franchises.loc[ref.franchises["last_season"].isna(), "franchise_id"])
    for fid, rating in sorted(ratings.items(), key=lambda kv: -kv[1]):
        if fid in current:
            lines.append(f"| {names[fid]} | {rating:.0f} |")
    return "\n".join(lines) + "\n"


def build_elo_baseline(
    silver_dir: Path, config_dir: Path, models_dir: Path, reports_dir: Path
) -> EvalResult:
    """Evaluate, save test-season predictions and write the report. Used by ``make elo``
    and the Dagster asset."""
    matches = pd.read_parquet(silver_dir / "matches")
    matches["season"] = matches["season"].astype("int64")
    ref = load_reference(config_dir)
    games = games_from_silver(matches, ref)

    result = evaluate(games, test_seasons=TEST_SEASONS, grid=DEFAULT_GRID)
    final = tune(games, test_season=LOCKED_SEASONS[0], grid=DEFAULT_GRID)
    _, ratings = run_elo(games, final)

    (models_dir / "elo").mkdir(parents=True, exist_ok=True)
    result.predictions.to_parquet(models_dir / "elo" / "test_predictions.parquet", index=False)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "elo_baseline.md").write_text(render_markdown(result, final, ratings, ref))
    return result


def main() -> None:
    result = build_elo_baseline(
        REPO / "data" / "silver",
        REPO / "config" / "reference",
        REPO / "data" / "models",
        REPO / "reports",
    )
    print(result.pooled.to_string(index=False))
    print(result.comparisons.to_string(index=False))
    print("wrote reports/elo_baseline.md")


if __name__ == "__main__":
    main()
