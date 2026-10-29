# Interview Prep: Design Decisions to Defend

## Contents

1. [What the project is about (the 60-second pitch)](#1-what-the-project-is-about)
2. [Star schema and dbt](#2-star-schema-and-dbt)
3. [Other schema designs, and why star was chosen](#3-other-schema-designs-and-why-star-was-chosen)
4. [Empirical Bayes shrinkage](#4-empirical-bayes-shrinkage)
5. [Parquet and DuckDB, not Spark](#5-parquet-and-duckdb-not-spark)
6. [Partitioning: season partitions for bronze/silver only](#6-partitioning-season-partitions-for-bronzesilver-only)
7. [Data engineering concepts used in this project](#7-data-engineering-concepts-used-in-this-project)
8. [Where pandas is used](#8-where-pandas-is-used)
9. [The raw layer: incremental, idempotent downloads](#9-the-raw-layer-incremental-idempotent-downloads)
10. [The parser and staging (silver) tables](#10-the-parser-and-staging-silver-tables)
11. [Data contracts, quarantine and reconciliation](#11-data-contracts-quarantine-and-reconciliation)
12. [Orchestration with Dagster](#12-orchestration-with-dagster)
13. [Reference data: franchises, venues and home advantage](#13-reference-data-franchises-venues-and-home-advantage)
14. [The Elo baseline, and a negative result](#14-the-elo-baseline-and-a-negative-result)
15. [Reproducible rebuilds: `make rebuild`](#15-reproducible-rebuilds-make-rebuild)
16. [Exploration: the toss, chasing, venues and home advantage](#16-exploration-the-toss-chasing-venues-and-home-advantage)
17. [The dbt warehouse: star schema, contracts and tests](#17-the-dbt-warehouse-star-schema-contracts-and-tests)
18. [Kinds of data warehouse, and where DuckDB fits](#18-kinds-of-data-warehouse-and-where-duckdb-fits)
19. [Point-in-time player ratings](#19-point-in-time-player-ratings)
20. [Tuning the ratings: decay and T20I weight](#20-tuning-the-ratings-decay-and-t20i-weight)
21. [Player attributes: SPARQL, overrides and SCD Type 2](#21-player-attributes-sparql-overrides-and-scd-type-2)
22. [Pace/spin batting ratings and a feature that barely helped](#22-pacespin-batting-ratings-and-a-feature-that-barely-helped)

---

## 1. What the project is about

**The question:** every IPL auction, fans argue that franchises overpay for big names. Do they, and who is undervalued?

**How it's answered, in four steps:**
1. **Rate every player using only what was known at the time.** Ratings come from every ball they faced or bowled, but only from matches before the date being rated.
2. **Turn ratings into value.** A win probability model takes two playing XIs and predicts the winner. Swapping one player into an XI and seeing how much the win probability moves gives that player's **value**.
3. **Model the price.** A price model predicts what a player sells for at auction.
4. **Find who is undervalued.** These are players with high value compared with their price. An integer-programming optimiser picks the best squad a team could have built within the auction rules. A backtest checks whether the "undervalued" players really did better the following season.

**The framing for a data engineering interview:** the ML is the use case. The real product is the platform underneath: immutable raw data, a layered and tested warehouse, orchestration with lineage, point-in-time correctness, and reproducible builds.

---

## 2. Star schema and dbt

**Star schema.** Analytics tables are arranged around one central fact table:
- **Fact table:** one row per event at a fixed **grain**. Here that's `fact_delivery`, one row per ball. It holds measures (runs, extras, wicket yes/no) plus foreign keys.
- **Dimension tables:** descriptive context for each thing involved: `dim_player`, `dim_team`, `dim_venue`, `dim_season_rules`.

```
            dim_player
                │
dim_venue ── fact_delivery ── dim_team
                │
          dim_season_rules
```

**dbt** is the build tool for the SQL layers:
- **Models:** each table is a `SELECT` in its own file, and models refer to each other with `ref()`.
- **Build order and lineage:** dbt works out the dependency graph (DAG) from those references, builds tables in the right order, and draws the lineage graph.
- **Tests:** `unique`, `not_null`, `relationships` (foreign keys) and `accepted_values` run on every build.
- **Docs and contracts:** dbt generates documentation from the models and enforces model contracts (column names and types).
- **One-line pitch:** dbt brings software engineering practice to SQL: version control, tests, docs, modular code, and ELT inside the warehouse.

---

## 3. Other schema designs, and why star was chosen

| Design | What it is | Strength | Weakness |
|---|---|---|---|
| **3NF / normalised (Inmon)** | Every fact stored once across many small related tables, as in an application database | No redundancy; best for transactional writes (OLTP) | Analytical queries need many joins; hard for analysts to read |
| **Star (Kimball)** | One fact table with denormalised dimensions around it | Few joins, easy to understand, fast for analytical queries (OLAP) | Some redundancy in dimensions |
| **Snowflake** | A star whose dimensions are normalised further (for example `dim_player` → `dim_country`) | Less redundancy in large dimensions | More joins, and the gain is little when dimensions are small |
| **Galaxy / fact constellation** | Several fact tables sharing *conformed* dimensions | Several processes analysed against shared dimensions | More to design and govern |
| **One Big Table (OBT)** | Everything flattened into one wide table | The simplest queries; works well with columnar engines | Duplicates dimension data; awkward for slowly changing attributes; brittle when the schema changes |
| **Data Vault 2.0** | Hubs (business keys), links (relationships) and satellites (attributes with history) | Auditable, keeps full history, flexible with many changing sources | Heavy; needs a presentation layer (usually a star) on top for analysis |

**Why star here:**
- **There's a natural fact at a fixed grain.** Ball-by-ball cricket data is a textbook fact table, since every question (strike rate, economy, phase splits) is an aggregate over deliveries.
- **Dimensions are small and descriptive.** The player, team, venue and season-rules tables each have hundreds or thousands of rows, so normalising them into a snowflake saves nothing.
- **SCD Type 2 fits naturally.** Player role or style changes over time become versioned `dim_player` rows with `valid_from` and `valid_to`. The fact joins to the version that was valid on the match date, which supports point-in-time correctness directly.
- **It's what the downstream code needs.** Ratings, squad features and the API all query "deliveries filtered by player, phase and date", which is one join from the fact table.
- **Why not the others:** 3NF stays in the staging layer, where cleaned source tables are close to normalised. OBT would duplicate player attributes onto every ball and break SCD2 history. Data Vault is designed for dozens of changing enterprise sources, which is far more than three sources need.
- **It may become a galaxy.** `fact_auction` (one row per player per auction) would be a second fact table sharing `dim_player` and `dim_team`, which makes it a small fact constellation. That's worth mentioning in an interview.

---

## 4. Empirical Bayes shrinkage

**The problem:** a debutant scores 18 off 6 balls, a strike rate of 300. Taken at face value, that's the best batter in the league, ahead of someone with a strike rate of 140 across 3,000 balls. Small samples are mostly noise.

**The fix:** each rating is a weighted average of the player's own record and their role's typical value, and the weight grows with the evidence:

```
rating = (n × player_rate + k × role_average) / (n + k)
```

- **Small samples:** with 6 balls, the rating stays near the role average.
- **Large samples:** with 3,000 balls, the rating is almost exactly the player's own record.
- **`k`:** sets how many balls it takes for the player's own data to dominate.

**Why it's called "empirical Bayes":**
- **Bayes:** start from a prior (the role average) and update it with evidence (the player's balls).
- **Empirical:** the prior and `k` are estimated *from the data*. Method-of-moments on large-sample players measures how much true skill varies between players, compared with ball-to-ball noise.

**The models used:**
- **Dismissal rate per ball:** beta-binomial. A Beta prior for each role and phase, updated with dismissals out of balls.
- **Runs per ball:** normal-normal, where the player's mean is shrunk toward the group mean.

**What it's for:**
- **Better inputs.** Without it, small-sample flukes would dominate the win model features and the "undervalued" list, and the model would recommend buying the 6-ball wonder.
- **Debutants are handled.** A player with no data gets exactly the role prior, so no special case is needed.
- **Uncertainty comes free.** The posterior variance becomes the `xi_rating_uncertainty` feature.

**Interview analogy:** it's regularisation for averages, the same idea as ridge regression shrinking coefficients toward zero.

---

## 5. Parquet and DuckDB, not Spark

**The two aren't alternatives.** Parquet is a **file format** and Spark is a **compute engine**, and Spark usually reads and writes Parquet. The real choice is **DuckDB versus Spark**, and Parquet is the storage format either way.

**Why Parquet:**
- **Columnar.** A query reads only the columns it needs.
- **Small and typed.** Files are compressed and keep a typed schema.
- **Hive partitioning.** Folders like `season=2024/` let queries skip whole seasons.
- **Open.** DuckDB, Spark, pandas, BigQuery and Snowflake all read it.

**Why DuckDB rather than Spark:**
- **Scale.** 1–2M deliveries is a few hundred MB. DuckDB is an embedded, single-node columnar engine that handles it in seconds on a laptop.
- **Overhead.** Spark distributes work across a cluster for terabytes of data. At this size, its JVM startup, task scheduling and data shuffling make it *slower*, and it needs infrastructure to keep running.
- **Judgement.** Choosing Spark for 200 MB is a classic over-engineering red flag. "Right tool for the scale, and here's when I'd switch" is the stronger answer.
- **When to switch.** At hundreds of GB or more (every T20 league, ball-tracking data), move compute to Spark or a cloud warehouse such as BigQuery or Snowflake. The Parquet files and dbt models carry over largely unchanged, because dbt has adapters for those engines. That portability is part of the design.

---

## 6. Partitioning: season partitions for bronze/silver only

**Decision**
- Bronze (raw files) and silver (staging tables) are Dagster assets partitioned by season.
- Gold marts and `player_ratings_asof` are not partitioned. They are rebuilt in full as one dbt build run through `dagster-dbt`.

**Why**
- Point-in-time ratings are cumulative. Season N's ratings depend on every match before it, including earlier seasons and T20Is.
- If marts were partitioned by season, each partition would depend on every earlier one. Rebuilding 2015 would force 2016 onwards to rebuild as well, so you'd get the overhead of partitions without being able to backfill one season on its own.
- Silver is different. Parsing one season's JSON doesn't depend on any other season, so season partitions there give real independent backfills and reruns.
- The data is small: about 1,200 IPL matches plus a few thousand T20Is, roughly 1–2M deliveries. DuckDB rebuilds all the marts in seconds, so incremental marts would add complexity and bug risk for no measurable gain.

**What would change at scale**
- With far more data (every T20 league, ball-tracking data), switch to incremental marts. Rating state at the end of season N becomes a checkpoint that season N+1 builds on, which gives a chain of partitions in time order instead of independent ones.
- Exponential time decay helps here. The decayed sum at time t equals the sum at an earlier time multiplied by `exp(-λΔt)`, plus the new deliveries, so state can be carried forward without re-reading history.

**Likely follow-up questions**
- *"Why not partition everything?"* Partitions only pay off when partitions are independent. Here they aren't, so the dependency chain would remove the benefit.
- *"How do you know the full rebuild is correct?"* `make rebuild` is checked by row counts plus a content hash of each table's sorted rows. File bytes aren't compared, because Parquet files differ in bytes across writer versions.

---

## 7. Data engineering concepts used in this project

Each concept has a one-line definition and where it shows up in this repo.

### Architecture
- **Medallion architecture (bronze / silver / gold).** Raw data as received, then cleaned and typed, then business-ready. *Here:* `data/raw` → staging Parquet → dbt marts.
- **ELT vs ETL.** ELT loads first and transforms inside the warehouse; ETL transforms before loading. *Here:* mostly ELT (dbt runs SQL inside DuckDB). Python handles only the JSON flattening, which SQL does poorly.
- **Dimensional modelling (Kimball).** Facts plus dimensions, with the **grain** declared first. *Here:* `fact_delivery` has a grain of one row per ball.
- **Lakehouse-style storage.** Open file formats on disk, queried by an engine. *Here:* Parquet plus DuckDB.

### Ingestion
- **Incremental loading.** Process only what's new or changed. *Here:* SHA-256 checksums for each match file in the manifest.
- **Change detection.** Spot changed records by comparing hashes. *Here:* a corrected Cricsheet file gets a new hash, which creates a new version.
- **Immutable raw layer and replayability.** Raw data is never edited, so every table can be rebuilt from it. *Here:* timestamped zips plus the manifest, and `make rebuild`.
- **Idempotency.** Running a step twice gives the same result with no duplicates. *Here:* hash-based skipping, partition overwrite (not append), and deterministic keys.
- **Schema evolution.** Sources change format over time. *Here:* parsers that know each Cricsheet schema version explicitly, with tests for each.
- **Ethical and legal sourcing.** Respect licences, terms and robots.txt. *Here:* ODC-BY attribution for Cricsheet, open Wikidata/DBpedia endpoints, and no Cricinfo scraping.

### Data quality
- **Data contracts.** Agreed column types, keys and allowed values for a table. *Here:* pandera schemas (Python) and dbt model contracts (SQL).
- **Quarantine / dead-letter tables.** Bad rows are set aside with a reason, never silently dropped. *Here:* `quarantine_{table}`.
- **Reconciliation.** Check that totals agree across levels. *Here:* deliveries sum to innings totals, 11 players per side, at most 10 wickets.
- **Data quality tests.** *Here:* dbt `unique`, `not_null`, `relationships` and `accepted_values`, plus Dagster asset checks.
- **Data quality reporting.** *Here:* a report for each season with row counts, quarantined counts and reasons.

### Modelling and history
- **Slowly Changing Dimensions (SCD) Type 2.** Keep history by adding a new row with `valid_from`/`valid_to` instead of overwriting. *Here:* role and style changes in `dim_player`. Know Types 0, 1, 2 and 3 for interviews.
- **Surrogate vs natural keys.** A natural key comes from the source (the Cricsheet player ID). A surrogate key is generated by the warehouse (one per SCD2 version). *Here:* both, and you should be able to explain why SCD2 needs a surrogate key.
- **Entity resolution and master data.** Map messy names to one canonical entity. *Here:* team and venue alias tables, and fuzzy matching of auction names with a human review queue.
- **Reference data in version-controlled config.** *Here:* season rules with a source link for every value.

### Time and correctness
- **Point-in-time correctness / no leakage.** A feature may use only data available at prediction time. *Here:* matches dated strictly earlier, enforced by a test.
- **As-of join.** Join each row to the latest record at or before its timestamp. *Here:* DuckDB `ASOF JOIN` for `player_ratings_asof`.
- **Bitemporality (to mention as an awareness point).** Event time (when the match happened) versus record time (when Cricsheet published or corrected the file). The manifest's `first_seen_at` tracks record time.

### Orchestration and operations
- **DAG and software-defined assets.** Pipelines are declared as the data assets they produce, not as tasks. *Here:* Dagster assets.
- **Partitions and backfills.** Rebuild a slice without touching the rest. *Here:* season partitions in bronze and silver.
- **Lineage.** What feeds what, all the way from raw to the API. *Here:* the Dagster asset graph and dbt docs.
- **Observability.** Asset checks and metadata (row counts, quarantine counts) attached to each materialisation.
- **Reproducibility and data versioning.** Tie every result to the exact code and data. *Here:* a data snapshot ID (hash of the manifest and configs) logged with the git commit in MLflow, and returned in the API's `X-Data-Snapshot` header.
- **CI for data.** Lint, type checks, unit tests and a full dbt build on a fixture dataset for every push. *Here:* GitHub Actions with about 25 real Cricsheet match files.
- **Containerisation.** *Here:* Docker Compose for the API, the frontend and the warehouse.

### Storage and performance
- **Columnar storage and compression.** *Here:* Parquet.
- **Partition pruning.** Skip folders that a query's filter excludes. *Here:* `season=YYYY/` folders.
- **Scaling up vs scaling out.** One strong machine versus a cluster, and when each is right. *Here:* DuckDB now; Spark, BigQuery or Snowflake at larger scale.

---

## 8. Where pandas is used

The heavy table-to-table transforms are in SQL (dbt), by design. pandas handles the Python edges of the pipeline.

| Where | What pandas does | Skills shown |
|---|---|---|
| **Cricsheet JSON parser** (`ingest/`) | Flattens nested match JSON into `matches`, `innings`, `deliveries` and `match_players` DataFrames, then writes Parquet through pyarrow | `json_normalize`, explode, dtype casting, nullable dtypes, `to_parquet` with partitions |
| **pandera validation** (`ingest/`) | Validates each DataFrame against its contract, and splits failing rows into the quarantine DataFrame | Boolean masks, `concat`, schema checks |
| **Auction ingestion** (`auction/`) | Cleans Kaggle CSVs: currency strings like "₹2.4 Cr" and "$950,000", team name aliases, and merging with official results | String parsing, `merge` with `indicator=True` for reconciliation, `groupby` coverage reports |
| **Name matching** (`auction/`) | Builds candidate tables of auction names against Cricsheet name variants, and applies the hand-review CSV | `merge`, `apply` with rapidfuzz, conflict resolution |
| **Attribute pre-fill** (`ingest/`) | Turns Wikidata/DBpedia SPARQL results into a DataFrame and combines them with the overrides CSV | `combine_first`, deduplication, SCD2 interval building |
| **ML training sets** (`models/`, `value/`) | Loads marts from DuckDB (`.df()`), builds the matrix of both match orientations, and splits the rolling folds | Vectorised feature differences, time-based splits |
| **Bootstrap and evaluation** (`models/`) | Paired bootstrap over matches, and results tables | `groupby`, `sample`, `pivot_table` |
| **Ratings tuning** (`ratings/tuning.py`) | Scores 226k balls per grid point, aggregates log-likelihood per match for the bootstrap, and pivots the grid into the report | `groupby`/`agg` with named aggregations, `MultiIndex` `reindex` lookups, `pivot`, `to_markdown` |
| **Exploration notebook** (`notebooks/`) | Matches per season, toss effect, venue and home effects | `groupby`/`agg`, plotting |

**How to say it in an interview:** "I used pandas where Python is the right tool: parsing nested JSON, validation, messy CSV cleaning, entity resolution and ML feature preparation. Set-based transforms between tables went to SQL in dbt, because they're easier to test, document and trace there."

---

## 9. The raw layer: incremental, idempotent downloads

Code: `cricket_value/ingest/download.py` and `cricket_value/ingest/manifest.py`.

**What happens on each run**
1. **Conditional request.** If the last download had a `Last-Modified` header, send `If-Modified-Since`. A `304 Not Modified` means stop, and costs no download at all.
2. **Checksum check.** Otherwise hash the body (SHA-256). If it matches the latest stored download, stop, because nothing changed even if the server didn't say so.
3. **Immutable storage.** Store the bytes unchanged as `data/raw/<source>/<fetched_at>.zip`. The write goes to a `.part` temp file and is then renamed (an *atomic write*), so a crash never leaves a half-written file. Existing files are never overwritten.
4. **Member-level manifest.** For a zip, hash each match JSON *inside* it, and record only `(member, sha256)` pairs not seen before. A new match adds one row. A Cricsheet correction to an old match adds a *second* row for that match with a new hash, so the history is kept.

**Why hash members and not just the zip?** The zip's hash changes whenever *any* match is added, so it can only say "something changed". Member hashes say exactly which matches are new or corrected, which is what the parser needs to process only those (incremental processing).

**Idempotency, verified:** running `make ingest` twice in a row on real data gives `not_modified` for every source, no new files, no new manifest rows, and the same `snapshot_id`.

**Snapshot ID:** a SHA-256 over the sorted list of the *latest* version of every match file, plus the latest hash of plain-file sources such as the people register. Because it's sorted and only covers latest versions, row order and fetches that changed nothing don't affect it. Any new or corrected match does change it.

**Testability:** HTTP goes through an injected `httpx.Client`, so tests use `httpx.MockTransport` as a fake server and never touch the network. The clock is injected too, so timestamps in the tests are deterministic. This is dependency injection; it's worth naming it in an interview.

**`verify()`:** re-hashes every stored file against the manifest, which catches anyone or anything editing the raw layer. It's the check behind the "raw data is immutable" rule.

**Known edge case (honest limitation):** member rows are written before the archive row. If the process crashes between the two, the next run downloads the zip again and stores it a second time. The duplicate is harmless (no member rows are duplicated) but wastes a little disk. The other write order would be worse: a crash could leave an archive recorded whose members never get recorded.

**Likely follow-up questions**
- *"Why CSV for the manifest and not a database table?"* It's small (about 5k rows), easy to read and diff, and written sorted so diffs stay stable. At scale it would be a table in the warehouse or a catalog such as Iceberg, Delta or Hive metastore.
- *"Is the data reproducible from a fresh clone?"* The code and config are, but Cricsheet only serves the *current* zip, so a fresh clone downloads today's data. Exact reproduction needs the stored raw files, which is why runs log the `snapshot_id`. At scale, raw files would live in versioned object storage (S3 with versioning).

---

## 10. The parser and staging (silver) tables

Code: `cricket_value/ingest/parse.py` (one match → rows), `schemas.py` (Arrow types) and `silver.py` (batch build).

**The flow:** for each match, find the latest version in the manifest. Read it *straight from the stored zip* without extracting it, re-check its SHA-256, parse it into rows, and write hive-partitioned Parquet with `season=YYYY/` folders.

**Tables and grain.** Always state the grain first in an interview:

| Table | One row per |
|---|---|
| `matches` | match |
| `innings` | innings, including super-over innings |
| `deliveries` | ball bowled, including wides and no-balls |
| `wickets` | dismissal; a ball can have two |
| `replacements` | substitution event (Impact Player, concussion) |
| `match_players` | player listed for a team in a match |
| `players`, `player_name_variants` | person / alternate name (from the registers) |

**Profile the data before writing the parser.** Profiling all 4,889 files first turned up things neither the docs nor assumptions predicted:
- **Season labels:** `"2007/08"` means IPL 2008, so season comes from the first match date.
- **Impact Player reason:** subs use `impact_player`, which the docs don't list.
- **Rare events:**
  - two wickets on one ball,
  - bowl-outs,
  - `Awarded` results,
  - teams listing 10 or 13 players,
  - matches played over two dates.

*Interview line:* "I profiled the data before writing the parser, because the documentation and the data disagreed."

**Handling schema versions explicitly:**
- `SUPPORTED_VERSIONS` is an allow-list, so an unknown future version *fails loudly* instead of being half-parsed.
- `actual_delivery` (added in 1.2.0) is derived for older files. A test removes the field from every real fixture and checks that the derived values match Cricsheet's exactly.
- `registry.venues` (added in 1.3.0) is used when present and left null otherwise.

**Parse failures vs data contracts.** These are two separate kinds of failure:
- **Can't read the file** (unknown version, bad JSON, unregistered person, checksum mismatch): the file goes to `_parse_failures.parquet`, and the build continues.
- **Readable but implausible** (a team with 10 players, runs that don't add up): the parser keeps the rows, and the *contract checks* decide. That's why the 10-player Serbia match parses fine. The parser's job is to reshape data, not to judge it.

**Explicit Arrow schemas.** Without declared types, pandas guesses them per batch. A column like `win_by_runs` holds ints in one season and turns into floats in another once a null appears, because NumPy ints can't be null. The parser builds rows into Arrow tables with declared types, then uses Arrow-backed pandas dtypes (`pd.ArrowDtype`) that keep nullable integers as integers. Register IDs are read as strings, so `key_cricinfo` stays `"253802"` and never becomes `253802.0`.

**Idempotent writes.** Each table is written to a temp directory and swapped in only when it's complete:
- A rebuild **replaces** the table instead of appending, so there are no duplicates and stale season folders disappear.
- A crash part-way leaves the previous version intact.
- Rows are sorted before writing, so the output is deterministic.
- *Verified on real data:* hashing every table's contents before and after a rebuild gave identical hashes.

**Engine-neutral files.** pandas embeds its own metadata in Parquet files, and pandas couldn't read some of it back for list columns. It's stripped, so DuckDB, dbt and Spark all see plain Arrow types.

**Real fixtures in tests.** 15 real Cricsheet files, each chosen for a specific edge case, are committed with ODC-BY attribution. Tests use real data shapes instead of hand-written JSON that might not match reality.

**Numbers (real run, 2026-10-26):** 4,889 matches and 1,115,689 deliveries, 0 parse failures, a 3.6 s full rebuild, and 5.2 MB of delivery Parquet.

**Likely follow-up questions**
- *"Why parse in Python and not SQL?"* Nested JSON with optional fields that vary by version is clearer and easier to test in Python. Everything after staging is SQL.
- *"Why rebuild everything rather than only new files?"* At 3.6 s, a full rebuild is simpler and always correct. Season-level incremental runs arrive with Dagster partitions, and the manifest already records which files are new.
- *"How would this scale?"* Parsing is per-file and independent, so it parallelises cleanly across processes or Spark tasks. The Parquet layout stays the same.

---

## 11. Data contracts, quarantine and reconciliation

Code: `cricket_value/quality/contracts.py`, `reconciliation.py` and `report.py`. Output: `data/silver/_violations.parquet`, `data/silver/_quarantine/` and `reports/data_quality.md`.

**Three kinds of check, in order:**

| Check | Question | Example | On failure |
|---|---|---|---|
| **Parse** | Can the file be read at all? | unknown schema version | file goes to `_parse_failures` |
| **Contract** | Is each row valid on its own, and do the keys join? | `runs_total = runs_batter + runs_extras`; wicket kind in the accepted list; no duplicate ball keys; every innings belongs to an existing match | `error` quarantines the match, `warn` flags it |
| **Reconciliation** | Do the numbers agree *across* tables? | second-innings target = first-innings runs + 1; "won by 35 runs" equals the score difference; 11 players per side | flagged (`warn`), kept |

**What is a data contract?** It's an explicit, versioned promise about a table's shape and content: column types, keys, allowed values and rules between columns. Producers must keep it, and consumers can rely on it. Here, types are enforced by the Arrow schemas, and values and rules by pandera. Every rule has a **name**, and the same name appears in the tests, the violations log and the report, so a failure can be traced from report to code in one search.

**Why quarantine the whole match and not just the bad row?** Dropping one bad delivery would leave a match whose innings total, ratings and features are silently wrong, which is worse than missing the match entirely. Quarantining the whole match keeps every table internally consistent. The match isn't lost: the rows sit in `_quarantine` with the rules they broke, and the raw file is untouched, so a fixed rule or a corrected Cricsheet file brings it back on the next rebuild.

**Why two severities?**
- A wrong header field (`overs_per_side = 50` on a T20I where no over past the 20th was bowled) doesn't make the balls wrong. Throwing away 11 valid matches over a field nobody uses would be over-zealous.
- **`error` = unusable, `warn` = suspicious but usable.**
- This is how tools like dbt (`severity: warn`) and Great Expectations ("mostly") handle it.

**"Never silently dropped":** every row is accounted for in exactly one place: silver, quarantine or parse failures. Every warning is logged with its match ID.

**Calibrate rules against real data before trusting them.** Each check was prototyped on all 4,889 matches *before* it was written as a rule:
- **Kept as warnings:** a check that fires on hundreds of valid matches is a wrong check. The real numbers are tiny: 5 ten-player XIs, 1 target mismatch, 1 margin mismatch. Those are genuine source anomalies worth flagging, not bugs in the checks.
- **A bug the test suite didn't catch:** the warning count was 20 when profiling had found 1 such ball. pandera reports a failed whole-row rule once *per column*, so the count was multiplied by the number of columns. A test now pins "one violation per failing row".

**Reconciliation without totals.** The brief said "delivery runs sum to innings totals", but Cricsheet doesn't record innings totals. The ball-by-ball runs are instead tied to the two independent numbers it *does* record, the chase target and the winning margin. That's the same idea as tying a ledger to a bank statement.

**Documentation versus reality (again).** Cricsheet's documented wicket kinds omit `retired out` and `retired not out`, both valid under the Laws and both present in the data. Accepted values are the documented list plus observed values. Each addition is commented in code, so nothing is silently allowed.

**Determinism.** Violations are sorted, and the run is identified by the raw `snapshot_id`, not a timestamp. Rebuilding the same raw data gives identical logs, so idempotency covers the quality outputs too.

**Real run (2026-10-26):** 4,889 matches loaded, 0 quarantined, 0 unparseable, and 19 warnings:
- 11 T20Is with `overs_per_side = 50`
- 1 wide crediting runs to the batter
- 5 ten-player XIs
- 1 target mismatch
- 1 margin mismatch

**Likely follow-up questions**
- *"Why pandera and not Great Expectations?"* pandera is code-first, typed, and lives next to the pandas code it checks, with no separate config store. Great Expectations suits teams that want expectation suites and generated docs managed outside the code. The marts get dbt tests, which are the SQL-side equivalent.
- *"What happens when a quarantined match should actually be allowed?"* Change the rule, with a code review and a test, then rebuild. The raw file never changed, so the match comes back automatically. Rows are never hand-edited.
- *"How would you alert on this in production?"* A Dagster asset check fails the run on any `error` and records warning counts as metadata. In production, a jump in warnings or quarantines compared with the previous run would trigger an alert. That's the next step.

---

## 12. Orchestration with Dagster

Code: `cricket_value/orchestration/` (`assets.py`, `partitions.py`, `resources.py`, `definitions.py`). UI: `make dagster`, then open http://localhost:3000.

**What an orchestrator does:** it runs pipeline steps in dependency order, on a schedule or on demand. It records every run, shows lineage, retries failures, and lets you rebuild one slice of the data (a *backfill*). Without one, you have a pile of scripts and a README saying what order to run them in.

**Why Dagster, and what "software-defined assets" means.** Airflow is built around **tasks** ("run this function, then that one"). Dagster is built around the **data assets** the steps produce ("this table exists and is built from those tables"). The dependency graph *is* the lineage graph, so asking "what feeds `silver/matches`?" or "what goes stale if `raw/cricsheet_ipl` changes?" is the same question the scheduler already answers.

**The asset graph:**
```
raw/cricsheet_ipl ─┐
raw/cricsheet_t20i_male ─┴─> silver/match_index ─> silver/{matches, innings, deliveries,
                                                    wickets, replacements, match_players}
                                                    (partitioned by season) ─> reports/data_quality
raw/cricsheet_people ─> silver/players
raw/cricsheet_names  ─> silver/player_name_variants
```

**Making silver truly partitionable** needed a code change, not just Dagster config. The old build parsed everything and *then* split it by season, so "rebuild 2017" still read every file. Now there are two steps:
1. **Match index:** a cheap pass mapping each match file to its season.
2. **Season build:** parses, checks and writes *only* that season's files.

A test proves that rebuilding 2017 changes 2017's files and leaves every other season's files byte-identical. That's the property that makes partitions worth having.

**Assets hold no logic.** Each asset calls the same function as the `make` target (`build_season`, `build_match_index` and so on), so Dagster adds scheduling, partitions, lineage and checks, but no logic of its own. You can test the logic without Dagster, and switching orchestrator wouldn't mean rewriting it.

**Design choices to be ready to defend:**
- **Yearly time-window partitions** from 2005, rather than a fixed list: a new season appears on 1 January with no config change. `end_offset=1` includes the season still in progress.
- **Single-run backfill policy.** Normally each partition is a separate run, which needs a background daemon to queue them. A full rebuild here takes seconds, so the asset processes every season in the selected range in **one run**: all 22 seasons in about 7 s. If one season took minutes, I'd switch to one run per partition so they run in parallel.
- **Six tables in one multi-asset.** All six tables come out of parsing the same files, so they're materialised together and can't be selected separately. Splitting them would mean parsing every file six times.
- **Checks are WARN, not ERROR.** Bad matches are already quarantined, so the data written is correct. A failed `contracts` check means "a match was quarantined, look at it", and a failed `reconciliation` check means "a source anomaly was flagged". Neither should stop downstream assets. An ERROR (blocking) check would suit a rule whose failure means the data written is wrong.
- **Resources for paths and HTTP.** Tests point `ProjectPaths` at a temp directory, which is dependency injection again, the same idea as the downloader's injected HTTP client.
- **Weekly ingest schedule, off by default,** so a fresh clone never starts downloading on its own.

**Things that went wrong, and what they taught me:**
- **`from __future__ import annotations` breaks Dagster.** Dagster reads type hints at runtime to inject `context` and resources, and that import turns hints into plain strings. It's removed from `assets.py`, with a comment explaining why.
- **An empty-season test found a real bug.** On an empty frame, the reconciliation limits came out as booleans and the comparison crashed. Building a partition for a season with no matches (such as next year) would have failed in production.
- **Reports must sort explicitly.** Moving to season partitions changed the order rows were read in, and the report's row order changed with it. Sorting explicitly made it deterministic again.

**Dagster ↔ Airflow: the same ideas under different names**

Airflow is the most common orchestrator in job ads, so be ready to translate. The underlying ideas are the same; what differs is whether the unit is a *task* (Airflow) or the *data asset* a task produces (Dagster).

| Concept | Airflow | Dagster | In this project |
|---|---|---|---|
| A pipeline | **DAG** (a Python file defining tasks and their order) | **Asset graph**, plus **jobs** that select part of it | `ingest`, `silver_by_season` and `data_quality_report` jobs |
| A unit of work | **Task**, an instance of an **Operator** (`PythonOperator`, `BashOperator`, …) or a `@task` function (TaskFlow API) | **Asset** (`@asset`, `@multi_asset`) or, lower level, an **op** | `silver_season` multi-asset |
| Dependencies | Declared between tasks: `extract >> transform` | Declared between data: `deps=[...]` or function inputs | `deps=[MATCH_INDEX_KEY]` |
| Passing data between steps | **XCom** (small values through the metadata database) | **IO managers**, or each asset reads and writes its own storage | Assets read and write Parquet themselves |
| Data-aware scheduling | **Datasets** (Airflow 2.4+, renamed **Assets** in Airflow 3) | Native: the graph *is* assets; **automation conditions** and sensors | Not used yet (manual or scheduled) |
| Time slice of a run | **Logical date** (formerly `execution_date`) and the data interval | **Partition key** (time window, static, dynamic or multi-dimensional) | Yearly season partitions `2005`…`2026` |
| Rebuilding history | `airflow dags backfill` / clearing task instances | **Backfill** over a partition range, or a single-run backfill policy | All 22 seasons in one ~7 s run |
| Waiting for something external | **Sensor** (e.g. `FileSensor`, `ExternalTaskSensor`) | **Sensor** (`@sensor`, asset sensors) | Not needed yet |
| Timed runs | `schedule="0 6 * * 1"` on the DAG | **ScheduleDefinition** on a job | `weekly_ingest`, off by default |
| Data quality | Extra tasks (SQL check operators, Great Expectations) or callbacks | **Asset checks** attached to the asset, shown in the UI | `contracts` and `reconciliation` checks |
| External systems and config | **Connections**, **Hooks**, **Variables** | **Resources** (typed config classes, swappable in tests) | `ProjectPaths`, `CricsheetHttp` |
| Limiting concurrency | **Pools**, `max_active_runs` | **Concurrency pools** and run queue limits | Not needed |
| Where tasks execute | **Executors**: Local, Celery, Kubernetes | **Executors** plus **run launchers** (in process, multiprocess, Docker, Kubernetes) | In process, locally |
| Always-on services | **Scheduler**, webserver, metadata DB (Postgres/MySQL) | **Daemon** (schedules, sensors, backfills), webserver, instance storage (SQLite locally, Postgres in production) | `make dagster` with SQLite in `.dagster/` |
| Lineage | Through Datasets/Assets or OpenLineage integration | Built in: the asset graph | UI shows raw → silver → report |
| Testing locally | Harder: needs a scheduler context, or `dag.test()` | `dg.materialize([...])` inside pytest | `tests/orchestration/test_dagster.py` |
| dbt integration | Run dbt as a task (`BashOperator`, Cosmos) | `dagster-dbt`: every dbt model becomes an asset | Coming with the dbt marts |

**One-sentence translation:** a Dagster asset is an Airflow task that also declares the data it produces. Once outputs are declared, lineage, staleness, partitioned backfills and per-table checks follow naturally, which in Airflow you build on top.

**When I'd pick Airflow instead:** a team already running it, workflows that are mostly "call this API, then run that job" rather than building data tables, or a need for its very large library of provider operators.

**Likely follow-up questions**
- *"Dagster or Airflow?"* Airflow is the industry standard and very good for task scheduling. Dagster's asset model gives lineage, partition-aware backfills and data quality checks as built-in concepts, which matches what this project is meant to show. In an interview I'd say I can map either one onto the other: an asset is roughly a task plus a declared output.
- *"How does it know what's stale?"* Each materialisation is recorded. When an upstream asset is newer than a downstream one, the UI marks the downstream as stale. Automation conditions can then rebuild it automatically, which is the next step after a manual or scheduled ingest.
- *"What changes in production?"* Run storage moves from local SQLite in `.dagster/` to Postgres, and runs execute in containers or Kubernetes. Raw and silver move to object storage (S3 or GCS), and the schedule plus an automation condition replace manual backfills.

---

## 13. Reference data: franchises, venues and home advantage

Code: `cricket_value/reference/`. Data: `config/reference/*.csv`. Report: `reports/reference_audit.md`.

**What reference data is:** slowly changing facts that the source data doesn't contain but the analysis needs, such as "Delhi Daredevils and Delhi Capitals are the same franchise" or "Indore was a Kings XI home ground in 2017". In industry this is called **master data** or **reference data**, and it's usually maintained by people. That makes it the riskiest data in the pipeline, because nobody's code produces it.

**How it's kept honest:**
1. **Every row has a source link.** The loader rejects a row without one. "I looked it up" is not a source.
2. **It's validated like data:** unique keys, every ID resolving (an alias must point to a real franchise), no overlapping season ranges, allowed values for `role`.
3. **It's audited against the match data.** The sources say which grounds were home grounds, and the data shows where teams actually played. In a normal season the home side plays every match at its ground, so the data can *corroborate* the sources, but not replace them. The audit lists any disagreement for a person to resolve. The audit never edits the reference data, because the source stays the authority.

**Rename or a new franchise?** This decides whether a team's history (and later its Elo rating) carries over.
- **Same franchise, new name:** Daredevils → Capitals, Kings XI → Punjab Kings, Bangalore → Bengaluru, Supergiants → Supergiant. History continues.
- **Different franchise in the same city:** Deccan Chargers → Sunrisers Hyderabad. Deccan was *expelled*, and SRH is a new franchise with new owners, so history does **not** carry over.
- *Interview line:* "Identity is the ownership entity, not the city or the name."

**Rename or a new venue?** The same question for grounds. Feroz Shah Kotla → Arun Jaitley Stadium is a rename, so it keeps the same `venue_id`. Sardar Patel Stadium → Narendra Modi Stadium was a *demolition and rebuild*, so it gets a new `venue_id`, because pitch behaviour and boundary sizes changed. That matters for venue effects later.

**What the data taught me while building this.** I first planned to drop DY Patil from Deccan's 2010 home list, because semi-finals were played there. The audit flagged that Deccan had played *both* league matches at DY Patil. The source confirmed Mumbai was a Deccan home city that year, because Hyderabad hosted no matches. So a whole-season rule couldn't express "home for league games, neutral for the semi-final at the same ground". The fix was a `league_only` flag on that row, which gives the right answer for both kinds of match. The audit made me revisit an assumption, which is exactly what it's for.

**Neutral seasons.**
- **Taken from the sources:** 2009 in South Africa, the 2014 UAE leg (16–30 April), 2020 in the UAE, and 2021, where the season page says no team played at its home ground.
- **2022 is a judgement call:** the page doesn't say "no home advantage", but every team was given the same number of matches at each of the four Maharashtra venues. It's documented as a project decision. A sensitivity check in the win model can test whether treating it differently changes anything.

**Why the rule is SQL on DuckDB, not pandas.** The brief says transformations belong in dbt. Writing `match_home_status.sql` now means it becomes a dbt model without being rewritten, and this is the first use of DuckDB in the project.

**Likely follow-up questions**
- *"Why not scrape the venue tables?"* There are only about 40 rows, which change once a season. Hand-curating with sources and auditing against data is more reliable than a scraper that breaks when the page layout changes. At larger scale, such as every T20 league worldwide, I'd scrape or license the data and keep the same audit.
- *"How would you handle a fact the sources disagree on?"* Record both sources, choose one with a written reason in the `note` column, and add a sensitivity check if it affects a model.
- *"Is home advantage even real in the IPL?"* That's what the exploration notebook and the Elo home bonus will measure. The point of this layer is to make that measurement trustworthy.

---

## 14. The Elo baseline, and a negative result

Code: `cricket_value/models/elo.py`, `elo_baseline.py`, `metrics.py`. Report: `reports/elo_baseline.md` (`make elo`, or the `models/elo_baseline` Dagster asset).

**What Elo is.** Every team has a rating. The rating gap gives a win probability:

```
p(A wins) = 1 / (1 + 10^(−(rating_A − rating_B + home bonus) / 400))
```

After the match, both ratings move by `K × (result − p)`. An upset moves them a lot, and an expected result barely moves them. Here there are 3 parameters:
- **K:** how fast ratings react.
- **Home bonus:** in rating points.
- **Season pull-back:** how far ratings shrink back to average each new season, since squads change a lot at auctions.

**Why it's the first baseline:** it's the simplest model that uses *team strength*. Any player-level model has to beat it, or it isn't adding anything.

**The evaluation protocol, which matters more than the model:**
- **Rolling-origin validation.** For each test season N from 2015 to 2024, the parameters are tuned on 2009 to N−1 *only*, then season N is predicted. That's how it would have worked in real time.
- **The leakage test proves it:** rewrite every result from 2013 onwards, and the parameters chosen for 2013 don't change.
- **Locked seasons.** 2025–2026 are never scored, so they stay a clean test for the end of the project.
- **Point-in-time.** Elo is processed in date order, and each forecast uses ratings from *earlier* matches only. A team never plays twice in one day, so this satisfies the "strictly earlier date" rule.

**Why log loss and Brier score, not accuracy.**
- **Proper scoring rules:** both are *proper*, meaning they are minimised by forecasting the true probability. They reward being calibrated, not just picking the right side.
- **The problem with accuracy:** it treats a 51% forecast and a 99% forecast the same.
- **Reference points:** a coin flip scores log loss ln 2 = 0.693 and Brier 0.25.
- **Why the baselines are probabilities:** "home team always wins" as a hard pick has *infinite* log loss when it's wrong, so it's expressed as a probability estimated from earlier seasons.

**Paired bootstrap, and why "paired".** Both models face the same matches, so resample *matches* and recompute (Elo − baseline) on each resample. Easy and hard matches affect both models equally and largely cancel, which gives a much narrower interval than comparing two separate intervals. A gain only counts if the whole 95% interval is below zero.

**The result, stated honestly:** across 633 test matches, **Elo doesn't beat a coin flip.**
- **Log loss:** 0.6963 against 0.6931. The difference is +0.0032, with a 95% interval of [−0.0082, +0.0144].
- **The other baselines:** the home-rate and toss-rate baselines don't beat the coin flip either.
- **What the tuner is saying:** it chose a tiny K (about 4–8), meaning "past results barely predict the next match", plus a home bonus of about 30–45 points, which is about 54–56% for the home side.

**Why the result is plausible:**
- **Squads reset.** IPL squads change heavily every year through auctions, and the playing XI changes match to match.
- **T20 is high-variance.** One over can swing a game.
- **Even rosters.** A salary cap and an auction keep teams close in strength.
- **Implication:** team-level history is a weak signal. Any edge has to come from *who is actually playing*, which is exactly what the player-rating model in Phases 2–3 is for. This negative result is the motivation for the rest of the project.

**A trap I caught:** the first grid offered K only down to 8, and the tuner chose 8 almost every season. **A parameter stuck at the edge of the grid means the grid is wrong, not that the edge is the answer.** After extending K down to 2, the choices sit inside the grid (4–12), and the conclusion held.

**Why tuned Elo can do *worse* than a coin flip out of sample.** Tuning picks the parameters that best fit the earlier seasons, including their noise. When the true signal is tiny, the noise that was fitted costs a little out of sample. That's overfitting with only 3 parameters, and it shows why out-of-sample scoring is non-negotiable.

**Likely follow-up questions**
- *"Isn't a model that loses to a coin flip a failure?"* It's a *measured* result with an interval, and it changes the plan: the scope rule "beat Elo before the price model" is now a weak bar, so the win model must beat the coin flip and home-rate baselines by an interval that excludes zero, or explain why not. Reporting it is the rigour the project is meant to show.
- *"Why not margin-of-victory Elo?"* That was a deliberate early choice: win/loss only. Margins in T20 depend on whether a team batted first (runs) or chased (wickets), so they aren't directly comparable. It's a reasonable extension to test later, against the same locked protocol.
- *"How many matches would you need to detect a real 1–2% edge?"* Many more than 633. The interval is about ±0.01 in log loss, roughly the size of a real but small edge. That's why the brief insists on intervals rather than single numbers.

---

## 15. Reproducible rebuilds: `make rebuild`

Code: `cricket_value/build/rebuild.py`, `fingerprint.py`. Command: `make rebuild`.

**The claim it proves:** every derived table can be deleted and rebuilt from the raw files, and comes out *identical*. That's the practical meaning of "raw is the source of truth" and "every step is idempotent".

**What it does:**
1. **Verifies the raw layer first.** Every raw file is re-hashed against the manifest, offline. If raw data has changed, nothing downstream can be trusted, so the build stops.
2. **Deletes all derived data,** so nothing left over from an earlier build can make the result look reproducible when it isn't.
3. **Rebuilds everything:** silver, the registers, the quality report, the reference audit and the Elo baseline. It uses the *same functions* Dagster runs, so this checks the real pipeline, not a copy.
4. **Fingerprints the 22 outputs** and compares them with the last build of the *same raw snapshot*.

**Why not compare file checksums?** Parquet files with identical contents can differ in bytes, depending on the writer version and how rows are grouped. The brief's "same checksums" has to mean the same *content*. So each output's fingerprint is:
- **row count;**
- **column names;**
- **SHA-256 of the sorted per-row hashes.** Sorting makes it independent of row order and file layout. Duplicates still count, and any changed value, added row or renamed column changes it.

The reports are deterministic Markdown, so they're hashed byte for byte.

**Why fingerprints are kept per raw snapshot.** When Cricsheet publishes new matches, the outputs *should* change. Comparing against "the last build" would raise a false alarm. Keying the stored fingerprints by `snapshot_id` asks the right question: *for the same input, is the output the same?* New raw data starts a new baseline, and `--update-baseline` records a deliberate code change, such as a parser fix.

**Result on real data:** three consecutive full rebuilds took about 17 s each and matched. The regenerated reports are byte-identical to the versions committed to git.

**In CI:** a test builds the fixture data twice into separate folders and requires identical fingerprints. That checks determinism on every push without needing the real data.

**A bug the tests nearly hid.** pytest's default `norecursedirs` skips any folder named `build`. My new tests in `tests/build/` passed when I ran that folder directly, but `make check` silently skipped all 11 of them. I caught it because the total test count didn't go up. The folder is now `tests/rebuild`, and `norecursedirs` is set explicitly so this can't recur. *Lesson:* check that new tests actually ran, not just that the suite is green.

**Likely follow-up questions**
- *"What makes a pipeline non-deterministic?"* Unordered output (set or dict iteration, `groupby` without sorting, parallel writes), timestamps or run IDs written into the data, unseeded randomness (here the bootstrap is seeded), floating-point sums in a different order, and unpinned library versions (here `uv.lock` pins them).
- *"How would you do this at scale?"* Compare table-level checksums in the warehouse (e.g. `HASH_AGG` in Snowflake, or summing row hashes in BigQuery), run it on a sample or per partition, and keep data versions with a table format like Iceberg or Delta so two snapshots can be compared directly.

---

## 16. Exploration: the toss, chasing, venues and home advantage

Notebook: `notebooks/01_exploration.ipynb` (`make notebook`). Logic: `cricket_value/analysis/explore.py`. Figures: `reports/figures/`.

**"No logic in notebooks."** Notebooks are hard to test, review and reuse, and their cells can run out of order. Every number here comes from a tested function. The notebook only calls those functions and draws charts. It's committed with outputs so a reviewer can read it on GitHub, and `make notebook` re-executes it from a clean kernel.

**Wilson intervals, not ±1.96·SE.** With 60–74 matches a season, a "normal approximation" interval can spill past 0% or 100%, and it behaves badly near extremes. The Wilson score interval stays inside [0, 1] and has better coverage for small n. *Rule used throughout:* a result counts as an effect only if its 95% interval excludes 50%.

**Findings (1,234 decided IPL matches):**

| Question | Rate | 95% interval | Verdict |
|---|---|---|---|
| Toss winner chooses to field | 66.4% | 63.7–68.9% | above 50% |
| Toss winner wins | 51.4% | 48.6–54.2% | no clear effect |
| Side batting second wins | 54.5% | 51.7–57.2% | above 50% |
| Home side wins (915 matches with a home side) | 53.1% | 49.9–56.3% | no clear effect |

**How to talk about them:**
- **The toss is a red herring; the choice it enables isn't.** Captains have learned to bowl first (about 80% since 2016), and the chasing side does win slightly more often. So the model uses **who bats second** (known at the toss), not who won the toss.
- **Home advantage is small,** borderline at 53%. That agrees with Elo's tuned home bonus of about 54–56%. Together with the Elo result, it says team-level signals are weak, which is the case for player-level ratings.
- **Confounding, explained with a real example.** Narendra Modi Stadium has the highest average first-innings total, about 185. But scoring jumped from about 171 to about 196 after the 2023 Impact Player rule, and most of that ground's matches are from 2023 onwards. The raw venue average mixes the ground with the era, so venue effects must be estimated *with season in the model*.
- **Per-team home rates mix home advantage with strength.** CSK wins about 66% at home, but CSK wins a lot everywhere. Separating the two needs home-versus-away comparisons or a model with team strength, which is what Elo's home bonus does.

**Charts that respect the noise.** Every rate is drawn as a point with its 95% interval, a 50% reference line, and the all-seasons rate as a shaded band. A bar chart of 19 season rates would invite reading noise as trends.

**Bugs found while building this:**
- **Hive partition columns collide in joins.** Silver `innings` carries its own `season` partition column, so merging it with `matches` produced `season_x` and `season_y`, and the notebook crashed. The unit test's hand-made frame lacked that column. Fix: make test frames match the real table's shape, reproduce the failure in the test first, then fix the code.
- **pandas 3 stores missing strings as NaN.** NaN is truthy, so `"win" if winner else ...` silently labelled no-results as wins in a test helper. Use `notna()`, never truthiness, on pandas values.

**Likely follow-up questions**
- *"Is 54.5% for chasing a big edge?"* It's modest: about 4–5 points above a coin flip. It's real because the interval excludes 50%, and it's the kind of small, honest feature a calibrated model should use. It won't dominate on its own.
- *"Why not test home advantage per season?"* You can, but each season's interval spans about 25 points. Testing 15 seasons separately invites false positives (multiple comparisons). The pooled estimate answers the question with the data available.
- *"What would you do next with venues?"* Model first-innings totals with season and venue together (fixed or random effects), so each ground's effect is measured against its own era.

---

## 17. The dbt warehouse: star schema, contracts and tests

Code: `dbt/` (models, tests, macros), `cricket_value/warehouse/dbt.py`, `tests/warehouse/`. Commands: `make dbt`, `make dbt-docs`.

**Layering, and why each layer exists:**
- **Sources:** silver Parquet, read *in place* by DuckDB's `read_parquet` with hive partitioning. No copy is made, and `season` comes from the folder name.
- **Seeds:** the CSVs in `config/reference/` *are* the seeds (`seed-paths: ["../config/reference"]`), so there's one copy of every reference fact, read by both Python and dbt.
- **Staging (`stg_*`, views):** one model per source, with only type fixes. Everything downstream depends on staging, never on raw sources, so a source change is fixed in one place.
- **Intermediate (`int_*`, views):** reusable business logic: the home/away rule, and the mapping of names to `team_id` and `venue_id`.
- **Marts (tables with enforced contracts):** the star schema the ratings, models and API read.

**The star schema, with grains:**

| Table | Grain | Keys |
|---|---|---|
| `fact_delivery` | one row per ball (wides and no-balls included) | `delivery_key`; foreign keys to player ×3, team ×2, venue, season |
| `fact_team_match` | one row per team per match | `team_match_key`; foreign keys to team ×2, venue, season |
| `dim_player` | person | `player_id` (Cricsheet register) |
| `dim_team` | franchise or nation | `team_id` |
| `dim_venue` | ground | `venue_id` |
| `dim_season` | competition season | `season_key` |

Two fact tables sharing *conformed* dimensions makes this a small **fact constellation (galaxy)**. That's the answer to "star or galaxy?".

**Data contracts in dbt.** `contract: enforced` makes every mart column's type part of its interface. If a model returns an extra column, a missing one, or the wrong type, the build fails *before* the table is replaced. That's the SQL-side version of the pandera contracts on silver.

**Tests, generic and singular:**
- **Generic** (in YAML): `not_null`, `unique`, `relationships` (every foreign key resolves to its dimension) and `accepted_values`. These are the referential integrity the database can't enforce.
- **Singular** (a SQL file that returns failing rows):
  - every silver ball appears exactly once in `fact_delivery`, which catches join fan-out or loss;
  - Impact Player substitutions occur only in rule seasons, so sourced reference data and match data agree;
  - home status is symmetric (home ↔ away, or neutral ↔ neutral);
  - each match has exactly two team rows.

**Derived measures with cricket definitions,** worth knowing in an interview:
- **`runs_conceded`** = runs off the bat + wides + no-balls. Byes and leg-byes aren't the bowler's fault.
- **`bowler_wicket`** = bowled, caught, caught and bowled, lbw, stumped or hit wicket. Run-outs and retirements aren't credited to the bowler.
- **`batter_dismissed`** = the *striker* was out. A non-striker run out doesn't count against the striker.
- **`phase`** = overs 1–6 powerplay, 7–15 middle, 16–20 death.

**Making dbt testable from Python.** Paths reach dbt through the environment variables `CV_SILVER_DIR` and `CV_WAREHOUSE`, read in `profiles.yml` and the source definition. `run_dbt()` sets them and invokes `dbtRunner`, so a pytest test builds the whole warehouse from 15 fixture matches in a temp folder. CI therefore runs dbt without the real data.

**One rule in two places, guarded by a test.** The home/away rule is now a dbt model, but the Elo baseline and the notebook still call the Python/DuckDB version. Until they read the warehouse, a test asserts both give identical results. Duplication with a guard is acceptable as a migration step; silent duplication isn't.

**Things the data and tools taught me:**
- **Cricsheet withholds Afghanistan's men's matches.** I found it because the Full Member check showed 0 Afghanistan T20Is. That's a source gap, not a bug, so it's documented, and the ratings must say Afghan players have no T20I data.
- **Most T20Is are now between associate nations:** only 31% of T20I balls are Full Member against Full Member. The Full Member filter is a big modelling choice, not a footnote.
- **Don't let a lagging register break the build.** `dim_player` falls back to the scorecard name and flags it, instead of failing the whole build on one missing name.
- **DuckDB allows one configuration per file per process.** dbt's adapter keeps the warehouse open in-process, so a test opening it read-only fails. The tests connect with the same settings dbt uses.

**Likely follow-up questions**
- *"Why are staging models views and marts tables?"* Views cost nothing to build and always reflect silver. Marts are read many times by ratings, models and the API, so they're materialised once per build.
- *"How would this run in production?"* The same models on Snowflake or BigQuery, with only the adapter changed. Sources become external or staged tables, and marts could become incremental on season partitions.
- *"Where's the lineage graph?"* `make dbt-docs` serves dbt's docs site with the full DAG, from sources and seeds to marts, and it goes into the README in Phase 6.

---

## 18. Kinds of data warehouse, and where DuckDB fits

**dbt isn't a database.** It stores no data and runs no queries itself. It turns `SELECT` files into `CREATE TABLE/VIEW` statements and sends them to a database through an **adapter** (`dbt-duckdb`, `dbt-snowflake`, `dbt-bigquery`, ...). That's why the engine can change without rewriting the models.

**Main kinds of warehouse:**

| Kind | Examples | How it works | Good for |
|---|---|---|---|
| **Embedded / in-process OLAP** | DuckDB, chDB | A library inside your program; the database is a file. No server. | Single-machine analytics up to roughly hundreds of GB; local dev; CI |
| **Cloud warehouse, storage separate from compute** | Snowflake, BigQuery, Databricks SQL | Data sits in object storage; compute clusters spin up per query and scale independently. Pay per query or per second. | Shared company warehouse, TB–PB, many concurrent users |
| **Cloud MPP, compute and storage together** | Amazon Redshift (classic), Azure Synapse dedicated pools | A cluster of nodes, each storing a slice of the data | Steady heavy workloads; older AWS/Azure setups |
| **Lakehouse** | Apache Iceberg / Delta Lake / Hudi tables queried by Spark, Trino, Snowflake or DuckDB | Open table formats on top of Parquet in S3/GCS add transactions, schema evolution and time travel | One copy of data read by many engines; avoiding vendor lock-in |
| **Real-time OLAP** | ClickHouse, Apache Druid, Apache Pinot | Columnar servers built for very fast aggregations on fresh, streaming data | User-facing dashboards, sub-second queries on event streams |
| **Row-store OLTP used as a warehouse** | PostgreSQL, MySQL | Row-oriented, built for transactions | Small analytics next to an app; struggles with big scans |

**OLTP vs OLAP** is the underlying split. OLTP (Postgres behind an app) is row-oriented and handles many small reads and writes. OLAP is columnar and handles few large scans and aggregations, like "average strike rate by phase over 15 seasons". A warehouse is an OLAP system.

**Why DuckDB here:**
- **Scale:** a few hundred MB of deliveries. Any server or cloud warehouse would be infrastructure with nothing to do.
- **Zero ops and cost:** `pip install`, one `warehouse.duckdb` file, no account or credit card. Anyone cloning the repo can run `make dbt`.
- **Reads Parquet in place:** silver is never copied (§17).
- **CI-friendly:** pytest builds the whole warehouse from fixtures in a temp folder, which would be slow and need secrets on Snowflake.
- **SQL features the project uses:** `ASOF JOIN` for point-in-time ratings, window functions, hive partitioning.
- **Portable:** same dbt models, different adapter, if it ever has to scale (§5).

**DuckDB's limits, so you can name them first:**
- **One writer at a time,** and no server, so it can't be a shared company warehouse with many users writing. Fine here: dbt writes, the API only reads.
- **One machine:** no spreading work across a cluster.
- **No access control or roles** beyond file permissions.

**Likely follow-up questions**
- *"Isn't DuckDB just SQLite?"* Same deployment model (embedded, one file), opposite design: SQLite is a row-store for transactions, DuckDB is columnar and vectorised for analytics.
- *"What would you pick at a company?"* Usually whatever is already there, most often Snowflake or BigQuery, since the warehouse is shared infrastructure. For new builds with lots of raw files, a lakehouse on Iceberg keeps the data open to several engines.
- *"What's MotherDuck?"* A hosted DuckDB service that adds sharing and cloud storage, one path to scale without changing engines.

---

## 19. Point-in-time player ratings

Code: `dbt/models/intermediate/ratings/`, `dbt/macros/ratings.sql`, `dbt/models/marts/player_ratings_asof.sql`, `tests/warehouse/test_ratings.py`.

**The one rule:** a rating used for a match on date D may only use matches from *before* D. If it used the match itself, the win model would be trained on information it couldn't have had at the toss. That's **leakage**, and it makes a model look far better in backtests than it will ever be live.

**Pipeline, one idea per model:**
1. **Evidence:** per player, day, skill (bat or bowl), phase and stat, the day's weighted sums:
   - `n`: balls;
   - `x`: outcomes;
   - `x2`: squared outcomes, for the variance;
   - `n2`: squared weights.
2. **Cumulative:** running totals of each series, with every term multiplied by `e^(λ·day)`.
3. **As-of:** for each (player, D), DuckDB's `ASOF JOIN ... AND D > evidence_date` picks the last running total *strictly before* D. Multiplying it by `e^(−λ·D)` decays the whole history to D in one step.
4. **Priors and shrinkage:** `rating = (x + k·m) / (n + k)`.

**Why the cumulative trick:** the decay weight for a ball on day `t` used on day `D` is `e^(−λ(D − t)) = e^(−λD) · e^(λt)`. The `e^(λt)` part doesn't depend on D, so it can be summed once, as a running total. Without the trick, every (player, date) pair would need its own sum over the whole history, which is quadratic. With it, the work is one window function plus one join.
- **Overflow:** days are counted from 2005-01-01, so `e^(λt)` stays small. With the tuned 1,095-day half-life it's about `e^5`.

**Point in time for priors too.** It's easy to make the ratings point in time but estimate the role average from *all* data, which leaks the future through the prior. Here, each calendar year's priors use only evidence from before 1 January of that year. A test checks both.

**How the point-in-time test proves it:** build the warehouse, then rebuild it after changing every ball from 2023-03-31 onwards (+4 runs a ball, all wickets removed). Every rating dated on or before 2023-03-31 must be *identical*. The test also checks that later ratings *do* change, so it can't pass trivially. This kind of test is called **perturbation** or **metamorphic** testing. It checks behaviour, not how the code was written, so it would catch a leak from any source: a wrong join, the priors, the roles or the decay.

**Method of moments, in one breath:**
- The spread of players' observed rates = the true spread of skill + sampling noise.
- The noise is known from the binomial or normal model, so `true spread = observed spread − noise`.
- `k` = noise per ball ÷ true spread: how many balls of a player's own data it takes to count as much as the prior.

**A bug worth telling:** with decayed weights, a rate's sampling variance is `ball_var · Σw²/(Σw)²`. That's the "effective sample size" correction, `n_eff = (Σw)²/Σw²`. Using the naive `ball_var/Σw` overstated the noise, so the true spread came out at or below zero, and every dismissal prior hit the 5,000-ball cap. Tracking `Σw²` (decayed at `2λ`) fixed it: dismissal priors are now worth about 230–720 balls at the tuned 1,095-day half-life.

**Findings:**
- **Wicket rate is mostly noise.** Bowlers' per-ball wicket rates barely differ beyond chance, so their `k` often reaches the cap. Economy and dot rate carry the bowling signal. This matches the common analytics view that wickets in T20 are high-variance.
- **Role priors matter for tailenders.** Bowlers batting at the death average 1.21 runs per ball, against 1.76 for batters. Shrinking a 10-ball tailender toward the overall average would rate them as a decent batter.
- **Burn-in:** priors need history, so ratings start in 2009–2010. IPL 2008–2009 are evidence only, and the win model starts at 2010.
- **Sanity check:** players under 30 balls make up 75–100% of the extremes of raw rates, but only 4–54% of the extremes of ratings, about their base rate or below. The top of each list is the players a fan would name: Bumrah and Narine for death economy, Boult and Bhuvneshwar for powerplay dot balls.

**Where pandas is used:** only in the tests, to rewrite silver Parquet for the perturbation test and to compare the two builds with `pd.testing.assert_frame_equal`. The ratings themselves are pure SQL.

**Likely follow-up questions**
- *"Why `ASOF JOIN` and not a window over every match?"* An as-of join is the standard tool for "latest value before time t" (finance uses it for prices at trade time). It's one join instead of a correlated subquery.
- *"Why are T20Is down-weighted?"* Full Member T20Is are a different, often stronger, standard than the IPL, and they're there to fill gaps in IPL history. The weight was tuned on next-ball log-likelihood rather than picked by hand, and came out at 0.5 (section 20).
- *"Why 1 January for priors, not the day before each match?"* Priors estimated daily would cost a full method-of-moments pass per date for almost no change. Yearly priors are still strictly point in time.
- *"What's the weakness?"* Method-of-moments `k` is noisy from year to year, and roles are a coarse share of balls. A hierarchical Bayesian model fitted jointly would do both better, at much higher cost.

---

## 20. Tuning the ratings: decay and T20I weight

Code: `cricket_value/ratings/tuning.py`, `tests/ratings/test_tuning.py`, `tests/warehouse/test_ratings_tuning.py`. Report: `reports/ratings_tuning.md` (`make tune-ratings`).

**The two knobs:**
- **Half-life:** how fast old evidence fades. A ball from 1,095 days ago counts half as much as one from today.
- **T20I weight:** how much a Full Member T20I ball counts, compared with an IPL ball.

Both were guesses (730 days and 0.5) until this step.

**What log-likelihood is.** A model gives each possible outcome a probability. The *likelihood* is the probability it gave to what actually happened. Multiplied over many balls, that number gets tiny, so you take logs and add them up instead. **Log loss** is the negative average: lower is better, and it's measured in *nats* (natural-log units).
- **Proper scoring:** like the Elo report's log loss, it's a proper scoring rule. It's minimised by forecasting the true probabilities, so it rewards calibrated probabilities, not just "right side of 50%".
- **The penalty for confidence:** saying 1% for something that happens costs a lot (−ln 0.01 ≈ 4.6). Saying 50% costs 0.69.

**What gets predicted.** Every ball faced in IPL 2010–2024 (226,193 balls) gets two predictions, from that day's ratings of the batter and the bowler for that phase:
1. **Out or not.** The batter's dismissal rate is the base, adjusted on the log-odds scale by how much better or worse than average the bowler is: `logit p = logit p_bat + logit p_bowl − logit(average bowler)`. This is the "log5" idea from baseball: an average bowler leaves the batter's rate unchanged.
2. **How many runs (0–6).** The predicted mean is `batter's runs per ball × bowler's runs per ball ÷ average bowler's`.

**Why not just MSE on runs?** Runs per ball are lumpy: 0, 1, 4 and 6 are common, and 3 and 5 are rare. A Gaussian likelihood (which is exactly MSE) puts probability on 2.7 runs and treats a 6 as a huge error, so it scores the model on outliers.
- **The approach instead:** a real probability for each run value. Start from the league's 0–6 distribution for that phase, from earlier seasons only. **Exponentially tilt** it: multiply each `q_k` by `e^(θk)` and renormalise, with θ found by bisection so the mean matches the prediction.
- **Why tilting:** it's the distribution closest to the league's (in KL divergence) with the right mean. It keeps the league's shape, with 3s and 5s rare, and moves only the level.
- **A bonus:** now both parts are log-probabilities of discrete outcomes, so they add into one criterion, the joint log-likelihood of (out?, runs). A Gaussian density and a probability would be in different units.

**Why tune on next-ball prediction, not on the win model?**
- **Separation of concerns.** The ratings' job is to describe players. If they were tuned to whatever helps the win model, any win-model change would move the ratings, and the ratings would be fitted to one consumer, when the value model and the price model use them too.
- **More data points.** There are about 226,000 balls, against about 900 development matches. With so few matches, the win model's log loss is too noisy to tell 730 days from 1,095. The Elo baseline showed how wide match-level intervals are.
- **Less overfitting, and no double use.** The win model is tuned on development seasons too. Tuning the ratings on it as well would spend the same matches twice and make its scores look better than they'd be live.

**Why 2025–2026 stay locked.** They are the final test, evaluated once at the end. If their outcomes picked the half-life, that test would no longer be out of sample, and its score would be optimistic. The loader raises an error if asked for those seasons.
- **One subtlety:** the dbt build does include 2025–26 balls as *evidence*. But every rating used for a 2010–2024 ball is dated before 2025, by the point-in-time rule, so the locked seasons can't affect any score.

**Point in time again.** Each ball is scored with ratings dated strictly before its match, so every prediction is genuinely out of sample, with no train/test split to get wrong. The league distribution used for runs comes from *earlier* seasons, and a unit test checks that.

**Results:**
- **Chosen:** half-life 1,095 days (3 years) and T20I weight 0.5. Both are **interior**: there are grid points on both sides of each, so the grid didn't need extending.
- **The surface is flat near the optimum.** The four neighbours are 0.05–0.07 nats per 1,000 balls worse, and every paired bootstrap interval includes 0. The chosen point wins 64% of bootstrap resamples. I'd say: "anything from 2 to 6 years and 0.25–0.75 is as good as anything else; 3 years and 0.5 is the centre of that region."
- **What the data does rule out:**
  - half-lives of a year or less;
  - ignoring T20Is entirely (weight 0 is clearly worse);
  - counting a T20I ball the same as an IPL ball (weight 1 is slightly worse).
- **The ratings add little per ball:** 5.6 nats per 1,000 balls over predicting every ball from league averages, about 0.4%. That's expected, because one ball is mostly luck. Almost all of the gain is in runs, not dismissals, which matches section 19's finding that wicket rate is mostly noise.

**The edge-of-grid lesson (from Elo).** When the Elo tuner's best K was the smallest value offered, the right conclusion was that the grid was wrong, not that the edge was the answer. So the report checks whether the choice is on an edge and says "extend the grid" if it is. Here it isn't. One edge is natural: a T20I weight below 0 means nothing, so a choice of 0 would have been a real answer, not a grid problem.

**A finding: short half-lives break the priors.** The rule "a player needs 60 balls to help estimate a prior" counts *decayed* balls. On 1 January, eight months after an IPL, with a 90-day half-life nobody has 60, so no priors and no ratings exist.
- **How it's handled:** unrated balls are scored as league-only, which is honestly what that pipeline knows, and every grid point is scored on the same balls. The report shows the coverage of each point.
- **Why it doesn't change the answer:** the chosen point and its four neighbours rate 100% of balls.
- **What I'd change if short half-lives mattered:** the floor should count effective balls, not decayed ones.

**Where pandas is used:** the scoring and aggregation. Per grid point, it joins about 226k balls to their ratings (via DuckDB `.df()`), looks up each ball's league distribution with a `MultiIndex` `reindex`, and sums log-likelihoods per match with `groupby().agg()`. Grid tables are made with `pivot` and `to_markdown`. The heavy maths (tilting, bootstrap) is vectorised numpy on the same frames.

**Likely follow-up questions**
- *"Why bootstrap over matches, not balls?"* Balls in a match share players, pitch and conditions, so they aren't independent. Resampling balls would make the intervals too narrow.
- *"Isn't a 0.07 nats per 1,000 balls difference meaningless?"* Yes, that's the point of reporting it. The choice is the centre of a flat region, so the win model won't be sensitive to it.
- *"Why grid search and not an optimiser?"* There are two parameters, each run is a 3-second dbt rebuild, and a grid shows the whole surface: how flat it is, and where it breaks. A gradient method would give one number and hide that.
- *"Isn't tuning on 2010–2024 and then using the ratings for the win model on the same seasons leakage?"* The two parameters are global, and they're chosen by a different target (next ball, not match result). Strictly, a rolling tune (only seasons before each test season) would be cleaner, like Elo's. Given how flat the surface is, the choice would barely move. That's a reasonable follow-up if the win model's gains are marginal.
- *"Why is the bowler measured against the average bowler, not the average batter?"* The two stats aren't the same event: run outs count against the batter but not the bowler, and bowlers are charged wides and no-balls. Comparing each rating with its own mean keeps the adjustment unbiased.

---

## 21. Player attributes: SPARQL, overrides and SCD Type 2

Code: `cricket_value/attributes/` (`fetch.py`, `styles.py`, `silver.py`, `audit.py`), `dbt/models/intermediate/int_player_attributes.sql`, `dbt/models/marts/dim_player_history.sql`, `config/reference/player_attribute_overrides.csv`. Commands: `make attributes`, `make attributes-audit`.

**Why these attributes:** Phase 3 needs "number of spin and pace options" and left/right-hand balance, and Phase 5 needs nationality for overseas limits. None of that is in the ball-by-ball data.

**Why Wikidata and DBpedia, not scraping Cricinfo:** Cricinfo blocks automated access, and the brief requires respecting each site's terms. Wikidata and DBpedia are open SPARQL endpoints made for machine access, and Wikipedia has a documented API. The link between them is an ID, not a name match:
- Cricsheet's register has each player's ESPNcricinfo ID;
- Wikidata stores the same ID as property P2697;
- that leads to the Wikipedia article, and DBpedia's resource has the same title.

Joining on IDs avoids the classic name-matching bugs ("MS Dhoni" vs "Mahendra Singh Dhoni").

**SPARQL in one breath:** a query language for graph data. A query is a pattern of (subject, predicate, object) triples, such as `?item wdt:P2697 "253802"` ("the item whose Cricinfo ID is 253802"), and the endpoint returns every match. `OPTIONAL` is a left join. Batching 400 IDs per query keeps it to about 13 requests.

**Making an API idempotent: canonical snapshots.** Endpoints return rows in any order, so two identical answers could hash differently and look like a change. Sorting and de-duplicating rows before hashing makes "same answers" mean "same bytes". A refetch then stores nothing, the same rule as the Cricsheet downloader.

**A third source for the gaps only.** DBpedia is an occasional extraction of Wikipedia, so some famous pages (Harbhajan, Morkel) have no infobox data. Wikipedia's own API returns the top section of 50 pages per request, so one query round covers the 866 pages with a gap. Asking only about gaps keeps requests few, and Wikipedia fills only fields still empty, with the filled fields recorded, so provenance stays visible. Redirects and renamed pages are followed and mapped back to the title that was requested.

**Mapping messy text:** DBpedia copies Wikipedia infoboxes, so one style has many spellings ("Right-arm off break", "Right arm offbreak", a link to "Off_spin"). The mapper normalises the text, then reads keywords. It's tested on real spellings, and anything ambiguous becomes null rather than a guess. Domain knowledge fills gaps safely: leg spin is right-arm by definition, because the left-arm version has its own name.

**The overrides workflow (the human in the loop):** automatic sources have gaps (DBpedia never extracted Harbhajan Singh's infobox) and errors. A version-controlled CSV holds hand-checked corrections, one field per row, each with a source URL, date and `hand_checked` flag.
- It's in git, so every correction is reviewable and attributable.
- A dbt test rejects typos (an unknown field, player or value) instead of silently ignoring them.
- The audit report ranks gaps by the balls behind them, so checking effort goes where it matters.

**SCD Type 2 (slowly changing dimension):** keep every version of a dimension row, with `valid_from` and `valid_to`, instead of overwriting it.
- **Type 1:** overwrite, so the history is lost.
- **Type 2:** add a row per change, so history is kept.
- **Type 3:** keep a "previous value" column, so only one step of history.

Type 2 is what lets a fact be joined to the version that was true at the time.

**Why not dbt's built-in `snapshot`:** a dbt snapshot records history by comparing the source with the warehouse each time it runs. The history then lives *only in the warehouse*: delete it and rebuild, and it's gone. Here, every raw snapshot is kept, so `dim_player_history` is recomputed from raw files on every build. That keeps the project's rule that everything is rebuildable from raw. The cost is storing raw snapshots, which only happens when answers change.

**A bug worth telling.** My infobox regex let `\s*` after `=` match a newline, so an empty field (`| batting =`) captured the *next line* as its value. The mapper turned `| bowling = Right-arm fast` into a batting hand of "right", which was a silent wrong value, not an error. I only saw it because one player's `role` came back as `| club1 = Uttar Pradesh`. The lesson: parsers need a test for the empty case, and a sample of the *output* should be read, not just the success count. Now it has one, and fixing it changed 19 batting hands, 9 dates of birth and about 225 countries.

**Verifying a hand-check against the source.** For players with no Wikidata link, a Wikipedia search can return a lookalike. A page is trusted only if its text contains the player's own ESPNcricinfo ID, which is unambiguous. This also exposed a false positive: a *list* page matched because it contains hundreds of IDs, so list pages are excluded.

**Limitations to say first:**
- The sources give *current* values, so history starts at the first fetch. A player who changed country (Corey Anderson, New Zealand to the USA) would carry their current country back in time.
- Bowling style and batting hand almost never change, so for them the current version is safe for past matches.

**Where pandas is used:** silver parsing (`silver.py`) and the audit's coverage and queue (`audit.py`). The mapping and versioning logic is in plain Python and dbt SQL.

**Likely follow-up questions**
- *"Why is the SCD2 table separate from `dim_player`?"* Facts join to `dim_player` by `player_id`, which must stay unique. History is a separate table keyed by `player_version_key`, and `dim_player` shows the current version.
- *"How do you know the mapping is right?"* It's tested on real spellings; 98.7% of fetched styles map fully; the audit lists every gap; and ambiguity becomes null and is queued for a hand check.
- *"Why not just scrape Cricinfo?"* It blocks automated access and the brief says to respect each site's terms. These three sources are open and documented for machine use, so the pipeline is polite and legal by design.
- *"How did you decide a dual-style bowler is pace or spin?"* The first-listed style in the infobox, unless the text says "occasional". It is a judgement, recorded in the row's note. If it matters for the model, derive the primary style from the balls the player actually bowled.
- *"What if Wikidata is down?"* The fetch fails loudly, and silver is rebuilt from the snapshots already stored, so builds never depend on the network.

---

## 22. Pace/spin batting ratings and a feature that barely helped

Code: `dbt/macros/ratings.sql`, `dbt/models/intermediate/ratings/` (the `vs` column), `cricket_value/ratings/split_eval.py`, `tests/warehouse/test_ratings.py`. Command: `make split-eval`, which writes `reports/pacespin_split.md`.

**The idea:** batters differ against pace and spin, and the squad features need "how good is this XI against a spin attack?". So each batting series (runs and dismissals, per phase) gets a copy restricted to balls faced against pace or against spin bowlers. The series gain one dimension, `vs ∈ {all, pace, spin}`, and everything downstream (cumulative sums, priors, shrinkage) carries it.

**A modelling subtlety: the split costs data.** A batter faces far fewer balls of spin than of everything, so a split rating is noisier even after shrinkage. A split is only worth having if it predicts the next ball better. So the question was answered by measurement, not assumed.

**How it is measured:** the same next-ball log-likelihood used to tune the half-life (§20). Both scores use the *same* bowler rating and scoring rule; the only difference is the batter's rating, against all bowlers or against this bowler's kind. A paired bootstrap over matches gives an interval for the difference. This is **ablation**: change one thing, hold the rest fixed.

**What went wrong, in order (worth telling as a story):**
1. **The planned prior was bad.** I planned to shrink a split rating toward the role's average against that kind of bowler. It scored *worse* than the unsplit rating. The reason: a batter's rating against spin is mostly *his overall skill* plus a small deviation. Shrinking toward the role average throws away what his other balls tell us. The fix is **hierarchical shrinkage**: shrink the split rating toward his own overall rating (scaled by how the role scores against that bowler type).
2. **A coverage bug hid behind a good-looking number.** My first result was "the split helps, −0.22, significant". But 10% of balls were silently dropped for a missing rating. Spin at the death is so rare that fewer than 20 batters ever had 60 balls of it, so that series never estimated a prior and had no ratings in any year. When I added a rule for sparse series to borrow the unsplit prior strength, the dropped balls went to 0% and the result became **−0.11, not significant**. The earlier number came from a non-random 90% of the balls.
3. **The lesson:** always report how many rows each comparison dropped, and re-run the comparison after fixing coverage.

**The honest conclusion:**
- Overall, the split is **not distinguishable from no split** (−0.11, interval [−0.29, +0.07]).
- Against **pace** it helps (−0.50, [−0.72, −0.28]).
- Against **spin** it hurts (+0.57, [+0.18, +0.96]).

I kept the columns, because Phase 3 may use them, but the write-up must not claim pace/spin ratings improve predictions in general. This kind of mixed or null result is normal. Saying so plainly, with the interval, is the point of the exercise.

**Where pandas is used:** the evaluation (`split_eval.py`) uses pandas for the per-ball scoring and the bootstrap, and the tests use it to rewrite the fixture's attributes. The ratings themselves are SQL.

**Likely follow-up questions**
- *"Why does the split hurt against spin?"* I don't know yet. Fewer balls per batter is the obvious suspect, and it is testable: look at whether the loss is concentrated in batters with few spin balls.
- *"Why own-rating shrinkage instead of the role average?"* A hierarchy: a batter's skill against spin is a *deviation from his overall skill*, so his overall rating is the right centre. The role average is the right centre only when we know nothing about him.
- *"Isn't picking the prior strength on the seasons you score overfitting?"* A little, and the report says so. The strengths from ×1 to ×8 are within noise, so the choice barely matters. A stricter version tunes on earlier seasons and scores on later ones.
- *"How do you know the split balls partition the unsplit ones?"* A test: with every bowler labelled, balls against pace plus balls against spin equal the balls faced, for every batter, date, phase and stat.

---

## 23. Squad features: turning an XI into numbers

**The idea in one sentence:** before the model sees a match, each side's eleven players are collapsed into a short row of numbers (how strong are the openers, the middle order, the finishers, the bowling in each phase, how many pace and spin options), using only what was known that morning.

**Who counts as the XI.** The players listed as the starting eleven. The Impact Player is a substitute announced later and used in only some matches. If I counted them, the features would depend on a decision made *during* the match. They are stored as events and never used as a model input; instead the model has an `impact_player_era` flag that captures the rule change.

**Batting slots without peeking.** To say "the top three batters" I need to know who bats where, but the order for *that* match is only known after it starts. So each player gets an **expected batting position**: a decayed average of where they batted in *earlier* IPL matches.
- It is shrunk toward their role's typical position, `(Σw·position + k·default) / (Σw + k)`, with `k = 2`. This is the same shrinkage idea as the ratings: one innings as an opener shouldn't make someone an opener forever, and a debutant has no history, so they get the role's typical position.
- `k = 2` is a judgement, not a tuned value, and I say so if asked.
- Players are sorted by expected position and numbered 1 to 11. Top = slots 1–3, middle = 4–6, finishers = 5–8. Slots 5 and 6 are in both middle and finish on purpose: they face both phases.

**Bowling.** The five XI players with the highest share of balls bowled form the attack, averaged per phase. This avoids needing the real bowling order, which is also only known afterwards.

**The testing story (worth telling).** I wrote a leakage test first: rewrite every ball from a cutoff date, then check nothing before the cutoff changed. My first version swapped striker and non-striker. To check the test could fail, I deliberately introduced a leak (`>=` instead of `>` in the history join). The test still passed. The reason: swapping the openers only changes slots 1 and 2, and both go into the same "top 1–3 average", so the features couldn't change. I replaced it with a rewrite that *reverses* the batting order of each innings, and the deliberate leak was then caught. The lesson: a test that has never been seen to fail proves nothing, so break the code on purpose.

**Honest limits:**
- **Nulls in 2008–2009.** There is no history to build priors from in 2008, and none for death overs in 2009. A dbt test makes sure there are none from 2010. The models use 2015 onwards.
- **Current labels.** Pace or spin comes from the *current* player attributes. Bowling kind almost never changes, so this is safe, but it is not strictly as of the match date.
- **No signal in single features, as a first look.** Comparing the two sides inside each match (2015–2024, 633 decided matches), the side with the better single feature won 49–53% of the time, every interval including 50%. That matches the Elo result. If the models also find nothing, the write-up says so.

**Likely follow-up questions**
- *"Why not use the actual batting order?"* It is only known once the match is under way, so using it would leak the future into a pre-match prediction.
- *"How do you know there's no leakage?"* A test rewrites the outcomes and batting order of everything from a cutoff and checks all earlier features are identical, and I broke the code on purpose to prove the test fails when it should.
- *"Why is the Impact Player excluded?"* They are chosen during the match; including them would use information that doesn't exist at the toss.
- *"Why differences between teams rather than raw features?"* The model's inputs are team A minus team B, so one model covers both orientations and predictions can be made symmetric (next step, not built yet).
- *"Is k = 2 right?"* It's a judgement. The position only decides which group a player falls into, and the features are group averages, so the model is not very sensitive to it; I haven't tuned it.

---

## 24. Match features: one row per team, differences and symmetry

**The idea:** the model answers "does this team win this match?" from the *difference* between the two XIs (my openers minus theirs, my death bowling minus theirs) plus what is known at the toss.

**Why one row per team, not per match.** Each match gives two rows, one from each side. In one, every difference is `A − B` and the label is "A won"; in the other, every difference is negated and the label flips. Training on both means the model can't learn "team 1 tends to win" (an accident of how the data lists teams), and predictions can be made symmetric.

**Antisymmetric vs symmetric.** Differences, `home_edge` (+1 home, −1 away, 0 neutral) and `chase_edge` (+1 batting second) flip sign with the orientation. Flags like "Impact Player era" and "decided by super over" are the same for both sides. A test checks every column of the two rows of every match is an exact mirror image.

**Home advantage is one shared effect.** A separate home effect per franchise would mix "plays well at home" with "is a good team", and each franchise has only 7–108 home matches. One shared effect (`home_edge`) is estimable.

**Chase, not toss.** Who won the toss isn't a feature: the toss itself is a coin flip, and the measured edge belongs to *who bats second* (54.5% in the exploration), which is known right after the toss.

**A bug the tests caught.** `win_method = 'D/L'` is null for ordinary matches (no win method), so the flag came out null instead of false. The `not_null` test on the column failed on 12 rows. SQL three-valued logic: null compared to anything is null, so wrap it in `coalesce(..., false)`.

**Limits:** no venue features yet (they must be estimated within an era, because scoring inflation after 2023 confounds raw venue averages), and no Elo input yet. Both come with the models.

**Likely follow-up questions**
- *"Why not one row per match?"* Then "team A" is arbitrary, and a model can pick up that arbitrariness. Two mirrored rows remove it.
- *"Aren't the two rows duplicates, so your test set is double counted?"* They are the same match, so metrics are computed per match (one orientation, or the symmetric average), and bootstrap resamples *matches*, never rows.
- *"How are ties handled?"* The official winner, which for IPL ties is the super over; a flag records it.
