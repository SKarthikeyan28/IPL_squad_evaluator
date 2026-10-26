# Project Brief: IPL Squad Value and Auction Picker

## One-line summary

A production-style data platform that ingests T20 ball-by-ball data into a layered, tested warehouse, builds point-in-time player ratings, and serves a calibrated win model, player valuations and an IPL auction squad optimiser.

## Why this project exists

I watch cricket and wanted to answer a question fans argue about every auction: do IPL franchises overpay for big names, and which players are undervalued?

The project is a portfolio piece for **data engineering internship** applications. The main thing it must show is data engineering done properly: reproducible ingestion, a layered data model, data quality checks, orchestration, lineage and point-in-time correctness. The ML is the use case that the data platform serves. Rigour and honest evaluation matter more than headline accuracy.

## About the builder

- Final-year NUS Computer Science student
- Comfortable with: Python, Pandas, NumPy, Numba, FastAPI, PostgreSQL, Docker, GitHub Actions, pytest, cvxpy (used for mean-variance optimisation), heuristic optimisation with weighted cost functions
- Newer to: dbt, Dagster, DuckDB, scikit-learn modelling workflow, gradient boosting, probability calibration, SHAP, integer programming solvers
- Prefers test-first development and clean package structure (OOP, typed interfaces)

Pitch explanations at that level. Explain data modelling, ML and statistics choices as you go, because I will need to defend every decision in interviews.

---

## Ground rules

1. **No fabricated results.** Never invent metrics, accuracy figures or findings. Every number in the README or write-up must come from code that runs in the repo.
2. **Raw data is immutable.** Store downloaded files exactly as received, with checksums and download timestamps. Every downstream table must be rebuildable from raw files.
3. **Every pipeline step is idempotent.** Re-running any step for the same inputs gives the same output and creates no duplicates.
4. **Point-in-time correctness is non-negotiable.** Any feature used to predict a match may only use data from matches that finished before it started. Enforce this with a test.
5. **Validate over time, never with random splits.** Train on earlier seasons and test on later ones.
6. **Beat simple baselines before claiming anything.** Every model is compared with the baselines below.
7. **Be clear about what the model can't tell us.** Player swap effects are what the model predicts, not causal effects. The write-up must say so.
8. **Verify cricket rules, don't assume them.** IPL purse size, squad size limits, overseas limits, retention and Right to Match (RTM) rules, and the Impact Player rule change between seasons. Look them up for each season and keep them in versioned config with a source link for each value.

---

## Decisions already made

| Question | Decision |
|---|---|
| Seasons in scope | All IPL seasons from 2008. Add an `impact_player_era` flag from 2023, and record the Impact Player substitution as its own event |
| Data used for player ratings | IPL plus T20 internationals. T20I deliveries are down-weighted, with the weight tuned on validation seasons |
| Bowling style and role data | Scrape player profile pages for in-scope players, then check by hand. Every value records its source, scrape date and whether it was checked by hand. Scraping must respect each site's terms and robots.txt |
| Auction prices | Start from the best Kaggle dataset, check it against official results, and fill missing seasons. Record the source of each row |
| Demo | FastAPI backend with a small frontend |

---

## Data sources

| Source | What it gives | Notes |
|---|---|---|
| Cricsheet (cricsheet.org) | Ball-by-ball JSON for IPL and T20Is, match metadata (venue, toss, result, playing XIs), people register with stable IDs | Primary source. Download incrementally: only new or changed files |
| Player attributes | Role, batting hand, bowling style (pace or spin, arm) | Scraped then checked by hand. Changes over time (for example a role change) are stored as SCD Type 2 rows |
| IPL auction history | Player, season, final price, team, base price, retained or bought, RTM | Kaggle as the base, checked against official results, gaps filled. Produce a coverage report per season |

---

## Architecture

```
            Cricsheet zip/JSON      scraped profiles      Kaggle + official auction data
                    │                     │                         │
                    v                     v                         v
  RAW (bronze)  immutable files + manifest (checksum, source URL, fetched_at)
                    │
                    v
  STAGING (silver)  parsed, typed, deduplicated tables · contracts + quality tests
     matches · innings · deliveries · players · match_players · player_attributes (SCD2)
     · auctions · team_aliases · venue_aliases
                    │
                    v
  MARTS (gold)  fact_delivery · dim_player · dim_team · dim_venue · dim_season_rules
     · player_ratings_asof (per player per match date) · squad_features (per match)
                    │
       ┌────────────┼────────────────────┬────────────────────┐
       v            v                    v                    v
  win model     player value       price model         squad optimiser
  (calibrated)  + swap simulator                       + backtest
       │
       v
  FastAPI service + small frontend
```

All layers are orchestrated as Dagster assets, so lineage, partitions and reruns come built in.

## Tech stack

- Python 3.12, managed with uv
- **Storage:** Parquet files partitioned by season, queried with DuckDB
- **Transformations:** dbt with the dbt-duckdb adapter for staging and mart models. Python only for JSON parsing and ML
- **Orchestration:** Dagster software-defined assets, partitioned by season, with asset checks for data quality
- **Data quality:** dbt tests (unique, not_null, relationships, accepted_values) plus custom tests. pandera for Python-side dataframes
- **ML:** scikit-learn, LightGBM, SHAP, MLflow (local) for experiment tracking
- **Optimiser:** PuLP or OR-Tools
- **Serving:** FastAPI plus a small frontend
- **Engineering:** pytest, ruff, mypy, pre-commit, GitHub Actions, Docker Compose

Package layout:

```
cricket_value/
  ingest/        # downloaders, manifest, JSON parsing to staging Parquet
  orchestration/ # Dagster assets, partitions, schedules, asset checks
  ratings/       # point-in-time rating logic (if not expressible in dbt)
  models/        # baselines, win model, calibration
  value/         # player value, swap simulator
  auction/       # price model, optimiser, backtest
  api/           # FastAPI service
frontend/        # small UI for the swap simulator and player profiles
dbt/             # staging and mart models, tests, docs
config/          # season rules (with sources), model settings
data/raw/        # immutable raw files (git-ignored) + manifest
tests/
notebooks/       # exploration only; no logic lives here
reports/         # write-up, figures, data quality reports
```

---

## Phases

### Phase 1: Ingestion and the raw and staging layers

**Build**
- Incremental downloader for Cricsheet IPL and T20I archives. Write a manifest recording file, checksum, source URL and fetched_at. Skip files that haven't changed.
- Parser that turns nested JSON into staging tables: `matches`, `innings`, `deliveries`, `players`, `match_players`. Handle Cricsheet schema version differences explicitly.
- Edge cases: no-results and abandoned matches, rain-shortened matches and DLS targets, super overs (kept separate from the main innings), Impact Player substitutions, name changes (use Cricsheet IDs), team and venue renames (alias tables mapping to one canonical ID).
- Data contracts for each staging table: column types, keys, allowed values. Rows that fail go to a quarantine table with a reason. They are never silently dropped.
- Reconciliation checks: delivery-level runs sum to the innings totals, there are 11 players per side (12 with Impact Player), and the wicket count is ≤ 10.
- Dagster assets partitioned by season, so one season can be backfilled or rebuilt alone.
- Team Elo rating as the first baseline.

**Done when**
- `make rebuild` builds every table from raw files and gives the same row counts and checksums every time
- Running ingestion twice creates no duplicates
- Tests cover parsing, each edge case and schema version differences
- A data quality report shows the row count, the quarantined count and the reason for each season
- A short exploration notebook covers matches per season, toss effect, venue effects and home advantage

### Phase 2: Marts and point-in-time player ratings

**Build**
- dbt models for the star schema: `fact_delivery` plus player, team, venue and season-rules dimensions. Generate dbt docs so the lineage graph is visible.
- Player attributes scraper, a manual check workflow (a CSV of overrides kept under version control) and an SCD Type 2 `dim_player` table.
- Batting ratings: runs per ball and dismissal rate by phase (powerplay, middle overs, death), split by pace vs spin.
- Bowling ratings: economy, wicket rate and dot-ball percentage by phase.
- Time decay for older matches. Down-weight T20I balls.
- Shrink small samples toward a role-based average (empirical Bayes). Explain the choice.
- `player_ratings_asof`: one row per player per match date, built with an as-of join (DuckDB `ASOF JOIN`) or an equivalent windowed model.

**Done when**
- A test proves no rating uses a match on or after the date it is used for
- dbt tests pass on every mart, and the lineage graph is in the README
- Sanity checks pass: the top-rated players in each phase look plausible to a fan, and small-sample players are not at the extremes

### Phase 3: Squad features and win probability model

**Build**
- A `squad_features` mart built from each playing XI: top-order batting strength, middle-order and finishing strength, powerplay and death bowling quality, number of spin and pace options, batting depth and all-rounder balance.
- Model inputs are the differences between the two teams, plus venue, toss result and decision, home advantage and the Impact Player era flag. Use the XI announced before the match. The Impact Player substitute is treated as part of the squad, not of the starting XI.
- Baselines: the home team always wins, the toss winner always wins, and Elo.
- Models: logistic regression, then LightGBM.
- Rolling time-based validation, for example training on seasons up to N and testing on N+1, moving forward each season.
- Metrics: log loss and Brier score are the main ones; accuracy is secondary. Report confidence intervals, because one IPL season has only about 70 matches and one season is a noisy test.
- Calibration: reliability curves before and after calibration (Platt or isotonic, fitted on a separate calibration split). Report expected calibration error.
- Log every run in MLflow with the git commit and a hash of the data snapshot used.

**Done when**
- A results table compares every model with every baseline on the same test seasons
- Calibration plots are saved in `reports/`
- The conclusion is honest, even if the gain over Elo is small

### Phase 4: Player value, swap simulator and API

**Build**
- SHAP values for individual match predictions and for the model as a whole.
- Player value: the change in predicted win probability when a player replaces a replacement-level player in their role, averaged over a set of typical XIs and opponents. Define "replacement level" clearly (for example, a typical player available at or near base price).
- Swap simulator: pick a team's XI, swap one player, rebuild the squad features, and show the new win probability.
- FastAPI endpoints with typed request and response schemas, for player profiles, swap simulation and predictions. They read from the gold marts, not from raw data.
- A small frontend for the swap simulator and player profiles.

**Done when**
- `docker compose up` runs the API and the frontend against a built warehouse
- The API has tests
- The write-up explains that swap effects are model estimates, not causal effects

### Phase 5: Auction data, price model, optimiser and backtest

**Build**
- Auction ingestion: load the Kaggle data, reconcile it with official results, fill the gaps, and record a source for each row. Match auction player names to Cricsheet IDs, and send uncertain matches to manual review.
- Price model: predict auction price from player value, ratings, age, role, overseas status and recent form. The data is small and noisy, so report the error honestly and compare it with a baseline (the median price by role).
- Value for money: player value compared with the predicted or actual price.
- Squad optimiser (integer programming): maximise total expected value subject to the season's purse, squad size limits, overseas limits and role requirements, all read from `dim_season_rules`.
- Backtest: for past auctions, check whether players the model rated as undervalued did better the following season than players it rated as overvalued. Use only data available before each auction.

**Done when**
- The optimiser returns a valid squad under a past season's rules and purse
- Backtest results are reported, including if they show no edge

### Phase 6: Write-up and polish

- README: the question, an architecture diagram, the data model and lineage, data quality approach, results table, limits, and how to run it
- A short engineering write-up covering the design decisions, the trade-offs and what would change at a larger scale (for example, cloud storage, a warehouse such as BigQuery or Snowflake, and scheduled runs)
- A short write-up for a cricket fan with no stats background: what makes a player undervalued, with three or four concrete findings and charts
- CI that runs lint, type checks, unit tests, and the dbt build and tests on a small fixture dataset; Docker Compose for the full stack

---

## Scope control

- Phases 1 and 2 are the data engineering core and must be finished properly first. Phases 1 to 3 together are the minimum complete project.
- Do not start the Phase 5 price model until the Phase 3 win model beats Elo, or until it is understood why it doesn't.
- Out of scope: multi-agent bidding simulation, live in-match prediction, betting odds, scraping that breaks a site's terms, and cloud deployment (describe it in the write-up instead).