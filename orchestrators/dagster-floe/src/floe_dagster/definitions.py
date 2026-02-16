from __future__ import annotations

import os

from dagster import Definitions

from .assets import load_floe_assets
from .runner import LocalRunner, Runner


def build_runner_from_env() -> Runner:
    return LocalRunner(os.environ.get("FLOE_BIN", "floe"))


def build_definitions(
    *,
    manifest_path: str,
    entities: list[str] | None = None,
    runner: Runner | None = None,
) -> Definitions:
    return load_floe_assets(
        manifest_path=manifest_path,
        runner=runner or build_runner_from_env(),
        entities=entities,
    )
