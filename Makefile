.PHONY: install ingest verify silver quality-report reference-audit elo dagster test lint typecheck check

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
