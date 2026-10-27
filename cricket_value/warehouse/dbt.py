"""Run the dbt project from Python, against any silver directory and warehouse file.

``make dbt`` runs the CLI on the real data. This wrapper exists so tests (and later
Dagster) can build a warehouse from fixture data in a temporary directory: the paths
reach dbt through the CV_SILVER_DIR and CV_WAREHOUSE environment variables that
``dbt/profiles.yml`` and the source definitions read.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from dbt.cli.main import dbtRunner, dbtRunnerResult

REPO = Path(__file__).resolve().parents[2]
PROJECT_DIR = REPO / "dbt"


@contextmanager
def _env(values: dict[str, str]) -> Iterator[None]:
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def run_dbt(
    args: list[str], *, silver_dir: Path, warehouse: Path, work_dir: Path
) -> dbtRunnerResult:
    """Run ``dbt <args>``; build artefacts and logs go to ``work_dir``."""
    env = {"CV_SILVER_DIR": str(silver_dir), "CV_WAREHOUSE": str(warehouse)}
    flags = [
        "--project-dir", str(PROJECT_DIR),
        "--profiles-dir", str(PROJECT_DIR),
        "--target-path", str(work_dir / "target"),
        "--log-path", str(work_dir / "logs"),
    ]  # fmt: skip
    with _env(env):
        return dbtRunner().invoke([*args, *flags])
