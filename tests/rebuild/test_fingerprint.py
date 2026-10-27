from __future__ import annotations

from pathlib import Path

import pandas as pd

from cricket_value.build.fingerprint import compare, fingerprint_frame, fingerprint_path


def frame() -> pd.DataFrame:
    return pd.DataFrame({"id": ["a", "b", "c"], "runs": [1, 4, 6], "tags": [["x"], [], ["y", "z"]]})


def test_row_order_does_not_change_the_fingerprint() -> None:
    df = frame()

    assert fingerprint_frame(df) == fingerprint_frame(df.iloc[[2, 0, 1]])


def test_any_changed_value_changes_it() -> None:
    df = frame()
    changed = df.assign(runs=[1, 4, 7])

    assert fingerprint_frame(df)["sha256"] != fingerprint_frame(changed)["sha256"]


def test_duplicate_rows_count() -> None:
    df = frame()
    doubled = pd.concat([df, df.head(1)], ignore_index=True)

    assert fingerprint_frame(doubled)["rows"] == 4
    assert fingerprint_frame(doubled)["sha256"] != fingerprint_frame(df)["sha256"]


def test_renamed_column_changes_it() -> None:
    df = frame()

    assert (
        fingerprint_frame(df)["sha256"]
        != fingerprint_frame(df.rename(columns={"runs": "r"}))["sha256"]
    )


def test_paths_parquet_dirs_and_text_files(tmp_path: Path) -> None:
    (tmp_path / "t" / "season=2020").mkdir(parents=True)
    frame().to_parquet(tmp_path / "t" / "season=2020" / "part-0.parquet")
    (tmp_path / "r.md").write_text("# report\n")

    table = fingerprint_path(tmp_path / "t")
    text = fingerprint_path(tmp_path / "r.md")

    assert table["rows"] == 3
    columns = table["columns"]
    assert isinstance(columns, list)
    assert "season" in columns  # the partition folder is part of the data
    assert text["rows"] is None and len(str(text["sha256"])) == 64


def test_compare_names_what_changed() -> None:
    old = {
        "matches": {"rows": 10, "columns": ["a"], "sha256": "x"},
        "gone": {"rows": 1, "columns": [], "sha256": "y"},
    }
    new = {
        "matches": {"rows": 11, "columns": ["a"], "sha256": "z"},
        "added": {"rows": 1, "columns": [], "sha256": "w"},
    }

    assert compare(old, new) == [
        "added: new output",
        "gone: missing from this build",
        "matches: rows 10 -> 11, content changed",
    ]
