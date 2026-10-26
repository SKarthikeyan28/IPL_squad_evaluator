"""Resources: the things assets need from outside, swappable in tests."""

from __future__ import annotations

from pathlib import Path

import dagster as dg

REPO_ROOT = Path(__file__).resolve().parents[2]


class ProjectPaths(dg.ConfigurableResource):
    """Where the pipeline reads and writes. Tests point ``root`` at a temp directory."""

    root: str = str(REPO_ROOT)
    config_root: str = str(REPO_ROOT / "config")

    @property
    def raw_dir(self) -> Path:
        return Path(self.root) / "data" / "raw"

    @property
    def silver_dir(self) -> Path:
        return Path(self.root) / "data" / "silver"

    @property
    def reports_dir(self) -> Path:
        return Path(self.root) / "reports"

    @property
    def sources_path(self) -> Path:
        return Path(self.config_root) / "sources.toml"
