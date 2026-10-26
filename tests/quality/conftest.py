from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from cricket_value.ingest.parse import parse_match
from cricket_value.ingest.silver import to_frames

FIXTURES = Path(__file__).parents[1] / "fixtures" / "cricsheet"

Tables = dict[str, pd.DataFrame]


@pytest.fixture
def tables() -> Tables:
    """Staging frames for all 15 real fixture matches, as build_silver produces them."""
    parsed = [
        parse_match(
            json.loads(p.read_text()),
            match_id=p.stem,
            competition=p.parent.name,
            file_sha256="f" * 64,
        )
        for p in sorted(FIXTURES.glob("*/*.json"))
    ]
    return to_frames(parsed)


def edit(df: pd.DataFrame, mask: pd.Series, column: str, value: object) -> pd.DataFrame:
    """Copy of ``df`` with ``column`` set to ``value`` on the first row matching ``mask``."""
    out = df.copy()
    row = mask[mask].index[0]
    out[column] = out[column].where(out.index != row, value)
    return out
