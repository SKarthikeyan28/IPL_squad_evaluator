.PHONY: install ingest verify silver test lint typecheck check

install:
	uv sync

ingest:
	uv run python -m cricket_value.ingest

verify:
	uv run python -m cricket_value.ingest --verify

silver:
	uv run python -m cricket_value.ingest.silver

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test
