"""Rebuild every derived output from the raw files, and prove the result is reproducible.

``make rebuild``:

1. Re-hashes every raw file against the manifest (offline). Any altered or missing file
   stops the rebuild: derived data is only as trustworthy as the raw layer.
2. Deletes all derived data (``data/silver``, ``data/models``), so nothing from an
   earlier build can survive.
3. Rebuilds silver (all seasons), the registers and the player attributes, then the data
   quality report, the reference audit and the Elo baseline: the same functions Dagster
   runs.
4. Fingerprints every output (rows, columns, content hash) and compares them with the
   fingerprints stored for the same raw snapshot in ``data/build/fingerprints/``.
   The first build of a snapshot stores its fingerprints; later builds must match them
   exactly, or the command fails and names each output that changed.

``make rebuild ARGS=--update-baseline`` accepts a deliberate change (for example after
fixing a bug in the parser) by replacing the stored fingerprints.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from cricket_value.attributes.silver import ATTRIBUTES, build_player_attributes
from cricket_value.build.fingerprint import Fingerprint, compare, fingerprint_path
from cricket_value.ingest.download import verify_raw
from cricket_value.ingest.manifest import Manifest
from cricket_value.ingest.schemas import SILVER_TABLES
from cricket_value.ingest.silver import (
    INDEX_DIR,
    INDEX_FAILURES_DIR,
    PARSE_FAILURES_DIR,
    QUARANTINE_DIR,
    REGISTERS,
    VIOLATIONS_DIR,
    build_registers,
    build_silver,
    competitions_from_config,
)
from cricket_value.models.elo_baseline import build_elo_baseline
from cricket_value.quality import report as quality_report
from cricket_value.reference import audit as reference_audit
from cricket_value.reference.reference import load_reference

REPO = Path(__file__).resolve().parents[2]
REPORTS = ["data_quality.md", "reference_audit.md", "elo_baseline.md"]


@dataclass(frozen=True)
class RebuildPaths:
    root: Path
    config: Path

    @property
    def raw(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def silver(self) -> Path:
        return self.root / "data" / "silver"

    @property
    def models(self) -> Path:
        return self.root / "data" / "models"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def fingerprints(self) -> Path:
        return self.root / "data" / "build" / "fingerprints"


@dataclass
class RebuildResult:
    snapshot_id: str
    fingerprints: dict[str, Fingerprint] = field(default_factory=dict)
    seconds: float = 0.0


@dataclass(frozen=True)
class Baseline:
    status: str  # "created", "matched" or "mismatch"
    differences: list[str]


def rebuild(paths: RebuildPaths, *, include_models: bool = True) -> RebuildResult:
    start = time.perf_counter()
    manifest = Manifest(paths.raw / "manifest")
    bad = verify_raw(paths.raw, manifest)
    if bad:
        raise RuntimeError("raw files missing or altered: " + ", ".join(bad))

    for derived in (paths.silver, paths.models):
        shutil.rmtree(derived, ignore_errors=True)

    competitions = competitions_from_config(paths.config / "sources.toml")
    build_silver(paths.raw, manifest, paths.silver, competitions)
    build_registers(paths.raw, manifest, paths.silver)
    build_player_attributes(paths.raw, manifest, paths.silver)

    snapshot = manifest.snapshot_id()
    paths.reports.mkdir(parents=True, exist_ok=True)
    (paths.reports / "data_quality.md").write_text(
        quality_report.render_markdown(paths.silver, snapshot)
    )
    ref = load_reference(paths.config / "reference")
    matches = pd.read_parquet(paths.silver / "matches")
    matches["season"] = matches["season"].astype("int64")
    findings = reference_audit.audit(matches, ref)
    (paths.reports / "reference_audit.md").write_text(
        reference_audit.render_markdown(findings, reference_audit.home_counts(matches, ref))
    )
    if include_models:
        build_elo_baseline(paths.silver, paths.config / "reference", paths.models, paths.reports)

    result = RebuildResult(snapshot_id=snapshot, fingerprints=fingerprint_outputs(paths))
    result.seconds = time.perf_counter() - start
    return result


def outputs(paths: RebuildPaths) -> dict[str, Path]:
    """Every derived output that must be reproducible, by a stable name."""
    out = {f"silver/{t}": paths.silver / t for t in [*SILVER_TABLES, *REGISTERS, ATTRIBUTES]}
    for d in (INDEX_DIR, INDEX_FAILURES_DIR, PARSE_FAILURES_DIR, VIOLATIONS_DIR):
        out[f"silver/{d}"] = paths.silver / d
    out |= {
        f"silver/{QUARANTINE_DIR}/{t}": paths.silver / QUARANTINE_DIR / t for t in SILVER_TABLES
    }
    out["models/elo/test_predictions"] = paths.models / "elo" / "test_predictions.parquet"
    out |= {f"reports/{r}": paths.reports / r for r in REPORTS}
    return {name: path for name, path in out.items() if path.exists()}


def fingerprint_outputs(paths: RebuildPaths) -> dict[str, Fingerprint]:
    return {name: fingerprint_path(path) for name, path in outputs(paths).items()}


def check_baseline(paths: RebuildPaths, result: RebuildResult, *, update: bool = False) -> Baseline:
    """Compare with the stored fingerprints for this raw snapshot, or store them."""
    stored = paths.fingerprints / f"{result.snapshot_id}.json"
    if update or not stored.exists():
        stored.parent.mkdir(parents=True, exist_ok=True)
        stored.write_text(json.dumps(result.fingerprints, indent=2, sort_keys=True) + "\n")
        return Baseline(status="created", differences=[])
    differences = compare(json.loads(stored.read_text()), result.fingerprints)
    return Baseline(status="mismatch" if differences else "matched", differences=differences)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="accept this build's fingerprints as the new baseline for its raw snapshot",
    )
    args = parser.parse_args()

    paths = RebuildPaths(root=REPO, config=REPO / "config")
    result = rebuild(paths)
    baseline = check_baseline(paths, result, update=args.update_baseline)

    for name, fp in result.fingerprints.items():
        rows = "" if fp["rows"] is None else f"{fp['rows']:>10,}"
        print(f"{name:<40} {rows:>10}  {str(fp['sha256'])[:12]}")
    print(f"\nraw snapshot {result.snapshot_id[:12]}, rebuilt in {result.seconds:.1f}s")
    if baseline.status == "created":
        print("baseline stored: later rebuilds of this raw snapshot must match it exactly")
    elif baseline.status == "matched":
        print("MATCHED: every output is identical to the previous build of this snapshot")
    else:
        print("MISMATCH: outputs differ from the previous build of this snapshot:", file=sys.stderr)
        for d in baseline.differences:
            print(f"  {d}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
