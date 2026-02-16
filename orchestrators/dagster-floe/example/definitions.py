from __future__ import annotations

import os
from pathlib import Path

from floe_dagster.definitions import build_definitions

EXAMPLE_MANIFEST = str((Path(__file__).resolve().parent / "manifest.dagster.json").resolve())

defs = build_definitions(
    manifest_path=os.environ.get("FLOE_MANIFEST", EXAMPLE_MANIFEST),
)
