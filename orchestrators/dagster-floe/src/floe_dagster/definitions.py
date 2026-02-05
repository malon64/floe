from __future__ import annotations

import os
from pathlib import Path

from .assets import load_floe_assets
from .runner import DockerRunner, LocalRunner

EXAMPLE_CONFIG = str(
    Path(__file__).resolve().parents[2].joinpath("example/config.yml").resolve()
)

def _build_runner():
    docker_image = os.environ.get("FLOE_DOCKER_IMAGE")
    if docker_image:
        return DockerRunner(
            image=docker_image,
            docker_bin=os.environ.get("DOCKER_BIN", "docker"),
            workdir=os.environ.get("FLOE_DOCKER_WORKDIR"),
        )

    return LocalRunner(os.environ.get("FLOE_BIN", "floe"))


defs = load_floe_assets(config_uri=EXAMPLE_CONFIG, runner=_build_runner())
