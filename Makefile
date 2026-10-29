.PHONY: install ingest verify silver quality-report reference-audit elo rebuild notebook attributes attributes-audit dbt dbt-docs lineage tune-ratings dagster test lint typecheck check

install:
	uv sync

ingest:
	uv run python -m cricket_value.ingest

verify:
	uv run python -m cricket_value.ingest --verify

silver:
	uv run python -m cricket_value.ingest.silver

quality-report:
	uv run python -m cricket_value.quality.report

reference-audit:
	uv run python -m cricket_value.reference.audit

elo:
	uv run python -m cricket_value.models.elo_baseline

# Rebuild every derived output from raw files and check it matches the last build of the
# same raw snapshot. Accept a deliberate change with: make rebuild ARGS=--update-baseline
rebuild:
	uv run python -m cricket_value.build.rebuild $(ARGS)

# Fetch player attributes from Wikidata and DBpedia into data/raw (a snapshot is stored
# only when the answers changed), then stage silver player_attributes. Needs make silver.
attributes:
	uv run python -m cricket_value.attributes

# Coverage of player attributes and the hand-check queue -> reports/player_attributes.md
attributes-audit:
	uv run python -m cricket_value.attributes.audit

# Build the warehouse (data/warehouse.duckdb): seeds, staging views, marts, then every
# dbt test and contract. Reads silver in place, so run `make silver` first.
DBT_FLAGS = --project-dir dbt --profiles-dir dbt
dbt:
	uv run dbt build $(DBT_FLAGS)

# Tune the ratings' half-life and T20I weight on next-ball log-likelihood (IPL 2010-2024)
# in a scratch warehouse; writes reports/ratings_tuning.md. Reads silver, like `make dbt`.
tune-ratings:
	uv run python -m cricket_value.ratings.tuning $(ARGS)

# Lineage graph and column docs at http://localhost:8080
dbt-docs:
	uv run dbt docs generate $(DBT_FLAGS) && uv run dbt docs serve $(DBT_FLAGS)

# Redraw the dbt lineage graph in README.md from the dbt project (no data needed)
lineage:
	uv run python -m cricket_value.warehouse.lineage

# Re-run the exploration notebook in place and refresh reports/figures/
notebook:
	cd notebooks && uv run jupyter nbconvert --to notebook --execute --inplace 01_exploration.ipynb

# Dagster UI on http://localhost:3000. Run history is kept in .dagster/ (git-ignored).
dagster:
	mkdir -p .dagster && touch .dagster/dagster.yaml
	DAGSTER_HOME=$(CURDIR)/.dagster uv run dagster dev -m cricket_value.orchestration.definitions

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test
