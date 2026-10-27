"""Fingerprints of build outputs: row count, columns, and a hash of the contents.

The content hash does not depend on row order or on how rows are split into files: each
row is hashed, the row hashes are sorted, and the sorted list is hashed. Duplicate rows
still count, and any changed value, added or removed row, or renamed column changes it.

Parquet files themselves are not compared byte for byte, because the same data can be
written as different bytes (writer version, row groups). The *contents* are what must
be reproducible.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

Fingerprint = dict[str, object]


def fingerprint_frame(df: pd.DataFrame) -> Fingerprint:
    columns = [str(c) for c in df.columns]
    # Text form first: a stable representation for every type, including list columns
    text = df.astype(str)
    row_hashes = np.sort(pd.util.hash_pandas_object(text, index=False).to_numpy())
    digest = hashlib.sha256("\x1f".join(columns).encode())
    digest.update(row_hashes.tobytes())
    return {"rows": len(df), "columns": columns, "sha256": digest.hexdigest()}


def fingerprint_path(path: Path) -> Fingerprint:
    """A Parquet file or a directory of Parquet files (hive partitions included), or any
    other file, which is hashed byte for byte (reports are deterministic text)."""
    if path.is_dir() or path.suffix == ".parquet":
        df = pd.read_parquet(path)
        return fingerprint_frame(df[sorted(df.columns)] if path.is_dir() else df)
    return {"rows": None, "columns": [], "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def compare(old: dict[str, Fingerprint], new: dict[str, Fingerprint]) -> list[str]:
    """Human-readable differences between two builds, one line per output."""
    out = []
    for name in sorted(set(old) | set(new)):
        if name not in old:
            out.append(f"{name}: new output")
        elif name not in new:
            out.append(f"{name}: missing from this build")
        elif old[name] != new[name]:
            parts = []
            if old[name]["rows"] != new[name]["rows"]:
                parts.append(f"rows {old[name]['rows']} -> {new[name]['rows']}")
            if old[name]["columns"] != new[name]["columns"]:
                parts.append("columns changed")
            if old[name]["sha256"] != new[name]["sha256"]:
                parts.append("content changed")
            out.append(f"{name}: {', '.join(parts)}")
    return out
