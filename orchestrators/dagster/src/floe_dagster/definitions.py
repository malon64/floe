from __future__ import annotations

import os
from pathlib import Path

from .assets import load_floe_assets
from .runner import LocalRunner

EXAMPLE_CONFIG = str(
    Path(__file__).resolve().parents[2].joinpath("example/config.yml").resolve()
)

defs = load_floe_assets(
    config_uri=EXAMPLE_CONFIG, runner=LocalRunner(os.environ.get("FLOE_BIN", "floe"))
)
