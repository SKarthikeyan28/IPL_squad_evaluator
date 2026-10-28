from __future__ import annotations

from pathlib import Path

import pytest

from cricket_value.warehouse.dbt import REPO
from cricket_value.warehouse.lineage import (
    END,
    START,
    manifest_lineage,
    mermaid,
    parse_manifest,
    replace_block,
)


def node(kind: str, fqn: list[str], deps: list[str] | None = None) -> dict[str, object]:
    out: dict[str, object] = {"resource_type": kind, "fqn": ["cricket_value", *fqn]}
    if deps is not None:
        out["depends_on"] = {"nodes": deps}
    return out


MANIFEST = {
    "sources": {"source.cricket_value.silver.matches": {"name": "matches"}},
    "nodes": {
        "seed.cricket_value.venues": node("seed", ["venues"]),
        "model.cricket_value.stg_matches": node(
            "model", ["staging", "stg_matches"], ["source.cricket_value.silver.matches"]
        ),
        "model.cricket_value.int_rating_x": node(
            "model",
            ["intermediate", "ratings", "int_rating_x"],
            ["model.cricket_value.stg_matches"],
        ),
        "model.cricket_value.dim_venue": node(
            "model",
            ["marts", "dim_venue"],
            ["model.cricket_value.int_rating_x", "seed.cricket_value.venues"],
        ),
        "test.cricket_value.not_null_x": node(
            "test", ["not_null_x"], ["model.cricket_value.dim_venue"]
        ),
    },
}


def test_lineage_groups_models_by_layer_and_collapses_seeds() -> None:
    layers, edges = manifest_lineage(MANIFEST)

    assert layers == {
        "silver": ["silver.matches"],
        "seeds": ["reference seeds"],
        "staging": ["stg_matches"],
        "ratings": ["int_rating_x"],
        "marts": ["dim_venue"],
    }
    assert edges == [
        ("int_rating_x", "dim_venue"),
        ("reference seeds", "dim_venue"),
        ("silver.matches", "stg_matches"),
        ("stg_matches", "int_rating_x"),
    ]  # tests are not lineage


def test_mermaid_is_a_left_to_right_flowchart_with_one_subgraph_per_layer() -> None:
    chart = mermaid(MANIFEST)

    assert chart.startswith("```mermaid\nflowchart LR\n")
    assert chart.count("subgraph ") == 5
    assert "stg_matches --> int_rating_x" in chart
    assert chart.endswith("```")


def test_replace_block_swaps_only_the_marked_section() -> None:
    text = f"intro\n{START}\nold\n{END}\noutro\n"

    assert replace_block(text, "new") == f"intro\n{START}\nnew\n{END}\noutro\n"
    assert replace_block(f"{START}\n{END}", "new") == f"{START}\nnew\n{END}"  # empty block
    with pytest.raises(ValueError, match="markers"):
        replace_block("no markers here", "new")


def test_readme_lineage_matches_the_dbt_project(tmp_path: Path) -> None:
    """Fails when a model is added or rewired without `make lineage`."""
    chart = mermaid(parse_manifest(tmp_path))
    readme = (REPO / "README.md").read_text()

    assert replace_block(readme, chart) == readme
