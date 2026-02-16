from __future__ import annotations

import os
from pathlib import Path

from floe_dagster.definitions import build_definitions, build_definitions_from_manifest_dir

EXAMPLE_MANIFEST = str((Path(__file__).resolve().parent / "manifest.dagster.json").resolve())
EXAMPLE_MANIFEST_DIR = str((Path(__file__).resolve().parent / "manifests").resolve())

manifest_dir = os.environ.get("FLOE_MANIFEST_DIR")
if manifest_dir:
    defs = build_definitions_from_manifest_dir(manifest_dir=manifest_dir)
elif Path(EXAMPLE_MANIFEST_DIR).exists():
    defs = build_definitions_from_manifest_dir(manifest_dir=EXAMPLE_MANIFEST_DIR)
else:
    defs = build_definitions(manifest_path=os.environ.get("FLOE_MANIFEST", EXAMPLE_MANIFEST))
