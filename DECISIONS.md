# Design Decisions

Settled in a design review on 2026-10-26. These decisions refine [Project_brief.md](Project_brief.md); where the two disagree, this file wins. The reasoning behind them, for interviews, is in [interview_prep.md](interview_prep.md).

## Contents

1. [Scope and schedule](#1-scope-and-schedule)
2. [Raw layer and ingestion](#2-raw-layer-and-ingestion)
3. [Storage, partitioning and reproducibility](#3-storage-partitioning-and-reproducibility)
4. [Data quality](#4-data-quality)
5. [Reference data: teams, venues, home games, season rules](#5-reference-data)
6. [Player attributes](#6-player-attributes)
7. [Point-in-time ratings](#7-point-in-time-ratings)
8. [Win model](#8-win-model)
9. [Player value and swap simulator](#9-player-value-and-swap-simulator)
10. [Auction data, price model, optimiser and backtest](#10-auction-data-price-model-optimiser-and-backtest)
11. [API and frontend](#11-api-and-frontend)
12. [CI and experiment tracking](#12-ci-and-experiment-tracking)

---

## 1. Scope and schedule

- **By Sun 1 Nov 2026:**
  - All of Phase 1.
  - The dbt star schema.
  - `player_ratings_asof` split by over phase (not yet by pace/spin), with empirical Bayes shrinkage and the point-in-time test passing.
- **Week 2, before Phase 3 starts:** the attribute pipeline, SCD2 `dim_player`, and pace/spin rating splits.
- **Daily plan for week 1:**
  - Mon–Tue: ingest and parse.
  - Wed: contracts, quarantine and reconciliation.
  - Thu: Dagster and Elo.
  - Fri–Sat: dbt marts and ratings.
  - Sun: point-in-time test and data quality report.
- **Minimum project:** Phase 3. Phases 4–5 are stretch goals.
- **README:** written as each phase is finished, so the repo can be shown at any phase boundary.
- **Frontend:** no work on it before Phase 4.

## 2. Raw layer and ingestion

- **Zips:** each Cricsheet zip is stored as `data/raw/cricsheet/{archive}/{fetched_at}.zip`, but only when its SHA-256 differs from the last download.
- **Manifest:** one row per JSON file inside the zip, with `match_id`, `sha256`, `source_zip` and `first_seen_at`.
- **Parsing:** only files whose hash hasn't been seen before are parsed.
- **Corrections:** a corrected match becomes a new version. Silver keeps the latest version of each `match_id`, and the manifest keeps the full history.
- **Parser:** builds pandas DataFrames, then writes Parquet through pyarrow. Each Cricsheet schema version is handled explicitly.
- **Canonical season:** the year of a match's first date, not Cricsheet's `info.season` label. Cricsheet labels IPL 2008 as `"2007/08"`, 2010 as `"2009/10"` and 2020 as `"2020/21"`. The original label is kept as `season_label`.
- **Staging tables:** `matches`, `innings`, `deliveries`, `match_players` and `players` as in the brief, plus three more:
  - `wickets`: a delivery can have two wickets.
  - `replacements`: Impact Player and concussion subs, recorded as events.
  - `player_name_variants`: from the Cricsheet names register.
- **Partitions:** match tables are hive-partitioned by season. The register tables are not partitioned.
- **Parse failures vs quarantine:** a file that cannot be read at all goes to `_parse_failures.parquet` and doesn't stop the build. Possible reasons:
  - an unknown schema version,
  - malformed JSON,
  - a person missing from the registry,
  - a checksum mismatch.

  Rows that parse but fail a *data contract* go to quarantine instead (Wed).
- **Impact Player reason:** Cricsheet records Impact Player subs with the reason `impact_player`, which the format docs don't list. The real data is what counts.
- **Conditional requests:** the downloader sends `If-Modified-Since` using the stored `Last-Modified` value, so an unchanged source costs one 304 response. A body whose SHA-256 is already stored is also skipped.
- **T20Is:** men's only, from 2005, and only matches where both sides are Full Members. This is one config switch. Opponent-strength adjustment goes in the write-up as future work.
  - **Full Members:** listed in `config/reference/full_member_nations.csv`, judged *as of the match date* (Ireland and Afghanistan became Full Members on 22 June 2017). `fact_delivery.is_full_member_t20i` flags the qualifying deliveries, and ratings filter on it.
  - **Size of the cut:** only **31% of T20I deliveries** (258,166 of 819,957) are from Full Member against Full Member matches, because most T20Is are now played between associate nations.
  - **Known source gap:** Cricsheet **withholds all men's matches involving Afghanistan** (163 T20Is; see cricsheet.org/withheld-matches). Afghan players therefore have IPL data but no T20I data, so their ratings rest on fewer balls and shrink harder toward the role prior. The write-up must say so. The IPL archive withholds nothing (all 1,243 matches are present).

## 3. Storage, partitioning and reproducibility

- **Silver:** written by Python as hive-partitioned Parquet (`season=YYYY/`).
- **Gold:** dbt reads silver as external sources and builds marts into a single `warehouse.duckdb` file.
- **API access:** the API opens `warehouse.duckdb` read-only.
- **Partitions:** only bronze and silver are Dagster assets partitioned by season. Gold marts and ratings are **not** partitioned; they are rebuilt in full by one dbt build through `dagster-dbt`.
- **Season builds:** silver is built in two steps.
  - A **match index** maps the latest version of each match file to its season, read from the file's date.
  - **`build_season`** then parses, checks and writes one season without reading any other.
  - `make silver` loops over the seasons, and Dagster runs one partition or a range, so both use the same code.
  - Quarantine, violations and parse failures are season-partitioned too. A season with no matches writes empty files, so stale rows can't survive.
- **Dagster:**
  - **Assets:** `raw/<source>` (4) → `silver/match_index` → `silver/<table>` (6 tables in one multi-asset, partitioned by season) and `silver/players` and `silver/player_name_variants` → `reports/data_quality`.
  - **Partitions:** yearly time-window partitions from 2005, which add each new year automatically.
  - **Backfills:** single-run backfill policy, so a full 22-season rebuild is **one run** (~7 s) and needs no daemon.
  - **Checks:** a `contracts` check on each silver table and a `reconciliation` check on `silver/matches`, both WARN severity. Quarantine already protects the data, so a failed check means "look at this", not "the pipeline broke".
  - **Schedule:** a weekly ingest schedule, off by default.
  - **Assets hold no logic:** they call the same functions as the `make` targets.
- **Rebuild check (`make rebuild`, ~17 s):**
  1. Re-hash every raw file against the manifest, offline. Any altered file stops the build.
  2. Delete `data/silver` and `data/models`.
  3. Rebuild silver, the registers, the quality report, the reference audit and the Elo baseline, using the same functions Dagster runs.
  4. Fingerprint all 22 outputs.
  - **The fingerprint:** row count, columns, and SHA-256 over the *sorted per-row hashes*. That makes it independent of row order and of how rows are split into files, while duplicate rows still count. Parquet bytes are never compared, and Markdown reports are hashed byte for byte.
  - **Baselines:** fingerprints are stored per raw snapshot in `data/build/fingerprints/<snapshot_id>.json`. The first build of a snapshot stores them, later builds must match exactly or the command fails and names each output that changed, and new raw data starts a new baseline. `make rebuild ARGS=--update-baseline` accepts a deliberate change.
  - **Real data:** three consecutive rebuilds matched. The regenerated reports are byte-identical to the committed ones.
  - **Tests:** a test builds the fixtures twice into separate folders and requires identical fingerprints, so CI checks determinism without the real data.

- **Warehouse (dbt + DuckDB, `make dbt` → `data/warehouse.duckdb`):**
  - **Project layout:** the project lives in `dbt/`. Its sources read the silver Parquet in place (`read_parquet` with hive partitioning), and seeds are the CSVs in `config/reference/` themselves, so there's one copy of every reference fact, read by both Python and dbt.
  - **Layers:**
    - **Staging** (`stg_silver__*`): thin views.
    - **Intermediate:** `int_match_home_status` (the home rule), `int_team_names` (team name → `team_id`) and `int_venue_names` (venue name → `venue_id`).
    - **Marts:** tables with **enforced contracts**, where every column's type is declared and a build that returns anything else fails.
  - **Star schema:** `fact_delivery` (one row per ball) and `fact_team_match` (one row per team per match) share the dimensions `dim_player`, `dim_team`, `dim_venue` and `dim_season`, which makes a small fact constellation.
    - **Team keys:** an IPL `team_id` is the franchise, so renames share an id. A national side's is `nat_<slug>`.
    - **Venue keys:** curated grounds keep their ids. Other names get `v_<slug>`, because T20I grounds aren't curated yet, so two spellings of one T20I ground stay separate.
    - **Derived flags in `fact_delivery`:** `phase`, `runs_conceded` (bat + wides + no-balls), `bowler_wicket` (only dismissals credited to the bowler), `batter_dismissed` (the striker was out) and `is_full_member_t20i`.
  - **`dim_player`:** falls back to the latest scorecard name, with `in_register = false`, if the register ever lags a new match. On the real data all 5,166 players are in the register.
  - **Tests:** 118 dbt steps, made up of not-null, unique, relationship and accepted-values tests on the marts, plus four custom tests:
    - every silver delivery appears exactly once in the fact table;
    - Impact Player substitutions occur only in rule seasons;
    - home status is symmetric;
    - each match has two team rows.
  - **CI:** `tests/warehouse` builds the warehouse from fixture data with `dbt build` through `cricket_value.warehouse.dbt.run_dbt`, and asserts that the dbt home rule equals the Python rule. Both exist until the Python callers read the warehouse.
  - **Consistency check:** the warehouse's home win rate (53.11% of 915 matches) is identical to the notebook's.

## 4. Data quality

- **Two severities:**
  - **`error`:** the data is unusable. The **whole match** is quarantined, meaning removed from every silver table and written to `_quarantine/<table>.parquet` with a `quarantine_rules` column. One broken delivery would make every total and rating built on that match wrong, so the match is the unit.
  - **`warn`:** the data is odd but usable. It's kept and logged.
- **Contracts:** pandera schemas for rules within a row, plus pandas joins for rules across tables (the match exists, the innings exists, the team plays in the match).
  - **Rule names:** every rule has one, and the same name appears in tests, the violations log and the report.
  - **Accepted values:** Cricsheet's documented values, plus values seen in real data that the docs omit (`impact_player`, `retired out`, `retired not out`). Each addition is commented in the code.
- **Warnings found in real data** (all kept):
  - 11 T20Is whose header says `overs_per_side = 50`, though no over past the 20th was bowled.
  - One super-over wide that credits runs to the batter.
- **Reconciliation:** Cricsheet has no innings totals, so ball-by-ball runs are checked against the two independent numbers it does record, the second-innings **target** and the **winning margin**. Other checks: starting XI = 11, at most 10 dismissals (retired hurt / retired not out don't count), and at most 120 legal balls unless Cricsheet flags miscounted overs.
  - All reconciliation checks are `warn`, because real cricket triggers some of them: Serbia and Costa Rica each really fielded 10 players.
  - The real run flags 7 matches: five 10-player XIs, one target mismatch (1457218) and one margin mismatch (1495657).
- **Violations log:** `_violations.parquet` has one row per failing row and rule, with `match_id`, `season`, `table`, `rule`, `severity`, `column` and `failure_case`. Whole-row rules are logged once per row, identified by the row's key.
- **Parse failures** (files that cannot be read at all) stay separate, in `_parse_failures.parquet`.
- **Quality report:** `make quality-report` writes `reports/data_quality.md` from those files. It covers each season and competition: matches, quarantined, deliveries, errors and warnings, plus every violation, with the raw snapshot ID at the top.
- **Run identifier:** the raw `snapshot_id`, not a timestamp, so rebuilding the same data gives byte-identical logs.
- **Dagster:** asset checks will wrap these same functions on Thursday.

## 5. Reference data

All reference facts live in `config/reference/` as version-controlled CSVs. Every row has a `source_url`, and `load_reference` rejects the files if any key is duplicated, any ID doesn't resolve, or any source is missing.

- **Franchises (`franchises.csv`, `team_aliases.csv`):** a canonical team is the **franchise ownership entity**. 19 Cricsheet team names map to 15 franchises.
  - **Renames of the same franchise:** Delhi Daredevils → Delhi Capitals (December 2018), Kings XI Punjab → Punjab Kings (17 February 2021), Royal Challengers Bangalore → Bengaluru (2024), and Rising Pune Supergiants → Supergiant (2017).
  - **Separate franchises:** Deccan Chargers and Sunrisers Hyderabad (Deccan was expelled and SRH is a new franchise), and Gujarat Lions and Gujarat Titans. Pune Warriors, Kochi Tuskers, Rising Pune Supergiant and Gujarat Lions are defunct.
- **Venues (`venues.csv`, `venue_aliases.csv`):** 60 IPL venue names map to 37 grounds. Cricsheet's venue register covers only 272 venues worldwide and missed most IPL names, so the aliases are curated.
  - **Renames that keep a `venue_id`:** Feroz Shah Kotla → Arun Jaitley Stadium (12 September 2019), Subrata Roy Sahara Stadium → MCA Stadium Pune, and Sheikh Zayed Stadium / Zayed Cricket Stadium.
  - **Rebuild with a new `venue_id`:** **Narendra Modi Stadium** replaced Sardar Patel Stadium, which was demolished in 2015. It is a separate venue because a rebuilt ground has new pitches and dimensions.
  - **T20I venues:** not curated yet. They aren't needed for IPL home advantage.
- **Home venues (`home_venues.csv`):** each franchise's grounds for each range of seasons, with a role of `primary`, `secondary` or `relocated`. Each row is checked against the Wikipedia season page's venue table and "(H)" markers.
  - **`league_only`:** a row marked `league_only` counts only for league matches. In 2010 Deccan played its home league games at DY Patil, which then hosted the semi-finals as a neutral venue.
- **Neutral periods (`neutral_periods.csv`):** 2009 (South Africa), the 2014 UAE leg (16–30 April), 2020 (UAE) and 2021 (no team at its home ground, then the UAE).
  - **2022 is a project decision:** Wikipedia doesn't say "no home advantage", but every team got the same number of matches at each of the four Maharashtra venues.
- **Home status rule (`match_home_status.sql`, run on DuckDB):**
  - Any match in a neutral period is neutral.
  - Otherwise a team is at home if the venue is on its franchise's home list for that season. Exactly one team at home gives home/away; none or both gives neutral.
  - Playoffs follow the same rule, so a playoff at a team's ground is a home game.
  - The rule is written in SQL so it can become a dbt model unchanged.
- **Audit (`make reference-audit` → `reports/reference_audit.md`):** compares the sourced home lists with where teams actually played. It flags a ground where one franchise played every league match but which isn't listed, a listed ground that no match used, and unmapped names.
  - **Real data:** no findings. Home games per franchise come to 7–8 per normal season.
- **Season rules (purse, squad and overseas limits, retention, RTM, Impact Player):** still to be researched for Phase 5, kept in `config/` with a source for every value. Anything not confirmed is marked unconfirmed, never guessed.

## 6. Player attributes

- **Pre-fill sources:** Wikidata and DBpedia, which are open endpoints, so no scraping is needed. They are linked through Cricsheet register `key_cricinfo` → Wikidata ESPNcricinfo ID → DBpedia. *(The Wikidata property ID still has to be verified.)*
  - **Batting hand and bowling style:** from DBpedia `battingSide` and `bowlingSide`. Bowling style is mapped to `{pace, spin} × {left, right arm}` with a tested lookup table.
  - **Date of birth:** from Wikidata.
  - **Nationality:** from Wikidata "country for sport", with the T20I team as a fallback.
- **Cricinfo:** not scraped. It blocks automated access, which conflicts with the terms rule in the brief.
- **Role:** derived **from the match data** (share of balls bowled versus faced, and keeping records), not from text sources.
- **Overseas status:** derived from nationality using that season's rules.
- **Overrides:** a version-controlled CSV fills gaps and corrects errors. Every value records `source`, `retrieved_at` and `hand_checked`.
- **History:** changes over time are stored as SCD Type 2 rows in `dim_player`.

## 7. Point-in-time ratings

- **Point-in-time rule:** a match on date D may only use matches dated **strictly before D**, so same-day matches are excluded. The test asserts `max(source_match_date) < match_date` for every row.
- **Phases:** overs 1–6, 7–15 and 16–20.
- **Shrinkage (empirical Bayes):**
  - **Dismissal rate per ball:** beta-binomial, with a prior for each role and phase.
  - **Runs per ball:** normal-normal.
  - **Prior strength:** estimated by method-of-moments on players with large samples.
- **Time decay:** exponential, computed in dbt SQL with the cumulative-sum identity `Σ wᵢxᵢ = e^(−λt) · Σ e^(λtᵢ)xᵢ`. Days are counted from a reference date to avoid overflow. Ratings are joined to matches with DuckDB `ASOF JOIN`.
- **Python fallback:** the `ratings/` module is used only if the SQL becomes unreadable.
- **Tuning:** the decay rate and T20I weight are tuned on **next-ball log-likelihood** (dismissal and runs outcomes) over the development IPL seasons. They are not tuned on the win model.

## 8. Win model

- **Prediction moment:** just after the toss, when both XIs are known.
- **What the exploration notebook found** (`notebooks/01_exploration.ipynb`, 1,234 decided IPL matches, 95% Wilson intervals):
  - **Toss winner wins:** 51.4% [48.6, 54.2], so no clear effect.
  - **Toss winners fielding first:** 66% overall, and about 80% every season since 2016.
  - **Side batting second wins:** 54.5% [51.7, 57.2], a modest real edge.
  - **Home side wins:** 53.1% [49.9, 56.3] over 915 matches with a home side. That's borderline, and consistent with Elo's tuned home bonus.
  - **Scoring:** the average first-innings total rose from about 171 (2022) to about 196 (2026), starting with the 2023 Impact Player rule.
- **Features that follow from these findings:**
  - **Use `bats_second`, not `won_toss`.** Who chases is known at the toss and carries the measured edge. Who won the toss doesn't.
  - **Venue effects must be estimated within an era,** or with season as a covariate. Raw venue averages are confounded by scoring inflation: the high average at Narendra Modi Stadium is partly the 2023+ era.
  - **Home advantage is a single shared effect,** not a separate one for each team. Per-franchise home rates mix home advantage with team strength and are far too noisy, at n ≈ 7–108 per franchise.
- **Notebook rules:**
  - **No logic in notebooks.** All logic is in tested `cricket_value/analysis/explore.py`, and chart styling is in `charts.py`.
  - **Committed with outputs.** The notebook is checked in with its outputs, and `make notebook` re-executes it and refreshes `reports/figures/`.
  - **Summary table is computed,** so the notebook can't drift from the data.
- **Starting XI:** `info.players` minus anyone who came in as a `match` replacement.
  - The Impact Player actually used is stored as an event but is **never a model input**.
  - The `impact_player_era` flag captures the rule change.
  - A leakage test enforces this.
- **Batting slots:** come from each player's **expected batting position**, their decay-weighted point-in-time average position. Debutants get their role's typical position. The actual batting order of the match is never read, and a test checks it.
- **Low-sample features:** `n_low_sample_players` and `xi_rating_uncertainty` (summed posterior variance).
- **Outcome labels:**
  - **No-results and abandoned matches:** excluded from training.
  - **Ties:** use the official winner, including super overs, plus a `decided_by_super_over` flag.
  - **DLS matches:** kept with a `reduced_overs` flag, plus a sensitivity check that drops them.
- **Team orientation:** each match is trained in both orientations, with features negated and the label flipped. Predictions are made symmetric: p = ½(p(A beats B) + 1 − p(B beats A)).
- **Elo:**
  - Win/loss only, no margin of victory.
  - Every team starts at 1500, including new franchises.
  - A home bonus applies only when `is_home` says so.
  - Each team is pulled one third of the way back toward 1500 at every season start.
  - K and the pull-back fraction are tuned on development seasons.
  - IPL matches only.
- **Baselines:** home team always wins, toss winner always wins, and Elo.
  - **Probabilistic baselines:** a hard "always wins" pick has infinite log loss when it's wrong, so the baselines are expressed as probabilities estimated from earlier seasons:
    - `coin_flip` (0.5);
    - `home_rate` (the home-win rate, and 0.5 at neutral grounds);
    - `toss_rate` (the toss-winner win rate).
  - **Elo grid:** K ∈ {2, 4, 6, 8, 12, 16, 20, 24, 32, 40}, home bonus ∈ {0, 15, 30, 45, 60, 90} points, and pull-back ∈ {0, 0.2, ⅓, 0.5, 0.75}. Elo is tuned by log loss on 2009 to N−1 for each test season N, with 2008 as burn-in. K was first offered only down to 8, and the tuner picked the smallest value, so the grid was extended. The values chosen now sit inside the grid (K = 4–12).
  - **Accuracy:** a 0.5 forecast earns half credit.
- **Elo result (`make elo` → `reports/elo_baseline.md`, 633 test matches, 2015–2024):** Elo does **not** beat a coin flip.
  - **Log loss:** Elo 0.6963 against 0.6931 for the coin flip. Elo − coin is +0.0032, with a 95% interval of [−0.0082, +0.0144].
  - **Other baselines:** none of the simple baselines beats the coin flip either.
  - **What the tuner chose:** a small K (≈4–8, so past results barely move ratings) and a 30–45 point home bonus, which means about 54–56% for the home side.
  - **Implication for the scope rule:** "beat Elo before the price model" is a weak bar on its own. The win model must beat the **coin flip and `home_rate`** by a paired interval that excludes zero, or the write-up has to explain why it can't.
- **Models:** logistic regression, then LightGBM.
- **LightGBM tuning:** monotone constraints on strength-difference features. Hyperparameters are chosen once from a fixed grid of about 20 settings by rolling development log loss. The number of trees is fixed, with no early stopping.
- **Validation:**
  - **Development:** rolling-origin tests on 2015–2024.
  - **Tuning:** inside each training window only (nested).
  - **Final test:** **2025–2026 are locked** and evaluated once, at the end.
- **Calibration:** **Platt scaling only**, fit on the last season of each training window. Isotonic is rejected because it overfits about 70 points.
  - Logistic regression is reported both uncalibrated and calibrated.
  - ECE uses 10 equal-frequency bins.
- **Metrics:** log loss and Brier score are primary, with accuracy secondary.
  - 95% intervals come from a **paired bootstrap over matches**, including the model-minus-Elo difference.
  - A gain counts only if its interval excludes 0.
  - Results are reported pooled and for each season.

## 9. Player value and swap simulator

- **Replacement level:** for each role and season, the mean point-in-time rating of fringe squad players, meaning the bottom 30% by appearances among players who played at least once that season. It uses Cricsheet only. In Phase 5 it is checked against the ratings of players bought at base price.
- **Value of player P in role R:**
  1. Take every real XI from the previous season.
  2. Put P in place of that XI's player in role R closest to P's expected batting position.
  3. Play it against every opponent XI from that season, at a neutral venue, with the toss averaged.
  4. Value is the mean Δwin probability compared with the same XI holding a replacement-level player. The spread across XIs is reported too.
- **Write-up:** must say that swap effects are model estimates, not causal effects.

## 10. Auction data, price model, optimiser and backtest

- **Name matching:**
  - Uses rapidfuzz against Cricsheet `names.csv` variants, limited to players active within ±1 season.
  - Auto-accepts only a score of at least 95 that is clearly ahead of the runner-up.
  - Everything else goes to the version-controlled `auction_name_review.csv`, and decisions made by hand are reused on reruns.
  - The coverage report shows auto-matched, hand-matched and unmatched counts for each season.
- **Price model:**
  - Target: **`log(price / purse)`**, with the raw currency kept alongside it, covering the USD era and the INR era.
  - Data: **open-auction purchases only**. Retentions and RTMs are kept in the data but excluded from training.
  - Features: include `is_mega_auction` and base price.
  - Models: ridge regression as the main model, and LightGBM only if it beats ridge on later auctions.
  - Validation: train on earlier auctions and test on later ones.
  - Metric: MAE on the log scale with bootstrap intervals, compared with the median-by-role baseline.
- **Optimiser (PuLP/CBC):**
  - Variables: `x_i` means buy player i, and `y_i ≤ x_i` means player i is in the first-choice XI.
  - Objective: maximise `Σ vᵢyᵢ + β·Σ vᵢ(xᵢ − yᵢ)`, with β around 0.1–0.2, tuned and reported.
  - XI constraints: exactly 11 players, at most 4 overseas, at least 1 wicketkeeper, at least 5 bowling options.
  - Squad constraints: purse, squad size limits and overseas limit, all from `dim_season_rules`.
  - Starting state: the team's actual position after retentions.
  - Prices: **predicted**, not actual.
- **Backtest:**
  - Before each auction, rank bought players by value relative to predicted price.
  - Each player's outcome is next-season realised value multiplied by matches played, and **players who never played count as 0**.
  - Metrics: Spearman correlation and the top-versus-bottom quintile gap in contribution per crore, both with bootstrap intervals.
  - Only development-window auctions are backtested until the final run.

## 11. API and frontend

- **API:** FastAPI, versioned under `/v1`:
  - `GET /players/{id}` returns the profile and attributes with their sources.
  - `GET /players/{id}/ratings?asof=` returns ratings as of a date.
  - `POST /predict` takes two XIs, a venue and the toss.
  - `POST /simulate/swap`
  - `GET /auctions/{season}/value`
  - `POST /optimise`
- **Snapshot header:** every response carries `X-Data-Snapshot`.
- **Data and models:** DuckDB is opened read-only, and models are loaded once at startup from MLflow by run ID.
- **Frontend:** Vite, React and TypeScript, with TanStack Query, and Observable Plot for charts (visx if lower-level control is needed).
  - API types are generated from FastAPI's OpenAPI schema with `openapi-typescript`.
  - It is served by nginx in Docker Compose.

## 12. CI and experiment tracking

- **Fixtures:** about 25 real Cricsheet match files in `tests/fixtures/cricsheet/`, with an attribution file (ODC-BY) and a list naming the edge case each covers:
  - an old schema version, a no-result, a DLS match, a super over,
  - an Impact Player substitution, a concussion sub,
  - a renamed team, a renamed venue, a T20I.
- **CI build:** runs the full Dagster and dbt build on these fixtures, plus lint, type checks and tests.
- **Data snapshot ID:** SHA-256 over the sorted `(match_id, sha256)` pairs in the manifest, plus hashes of the config files and overrides CSV.
- **MLflow:** each run logs the snapshot ID with the git commit.
