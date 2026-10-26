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
