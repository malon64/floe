#!/usr/bin/env python3
"""Fail when designated shared orchestrator modules drift out of lockstep.

The ``dagster-floe`` and ``airflow-floe`` packages each vendor a copy of a few
modules that are meant to stay byte-identical (see issue #394). They are
published as independent PyPI packages with no shared runtime dependency, so the
copies can silently diverge — and historically did. This guard compares each
designated pair and exits non-zero on any difference, turning drift into a
red CI check instead of a latent production bug.

Run locally with::

    python scripts/check_orchestrator_drift.py

Only modules that are intended to be identical belong here. ``manifest.py`` and
the ``*_runner.py`` modules are intentionally framework-specific and are NOT
guarded.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGSTER_PKG = REPO_ROOT / "orchestrators" / "dagster-floe" / "src" / "floe_dagster"
AIRFLOW_PKG = REPO_ROOT / "orchestrators" / "airflow-floe" / "src" / "airflow_floe"

# Module filenames that must stay byte-identical between the two packages.
SHARED_MODULES = (
    "databricks_client.py",
    "k8s_status.py",
)


def main() -> int:
    drifted: list[str] = []
    missing: list[str] = []

    for name in SHARED_MODULES:
        dagster_file = DAGSTER_PKG / name
        airflow_file = AIRFLOW_PKG / name
        if not dagster_file.exists() or not airflow_file.exists():
            missing.append(name)
            continue
        if dagster_file.read_bytes() != airflow_file.read_bytes():
            drifted.append(name)

    if missing:
        print("ERROR: designated shared modules are missing in a package:")
        for name in missing:
            print(f"  - {name}")

    if drifted:
        print("ERROR: shared orchestrator modules have drifted out of sync:")
        for name in drifted:
            print(f"  - {name}")
            print(f"      {DAGSTER_PKG / name}")
            print(f"      {AIRFLOW_PKG / name}")
        print(
            "\nThese files must stay byte-identical between dagster-floe and "
            "airflow-floe.\nApply the change to both copies (see issue #394)."
        )

    if missing or drifted:
        return 1

    print(f"OK: {len(SHARED_MODULES)} shared orchestrator module(s) in sync.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
