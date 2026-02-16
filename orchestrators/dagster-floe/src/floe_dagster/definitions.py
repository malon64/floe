from __future__ import annotations

import os
import re
from pathlib import Path

from dagster import AssetSelection, Definitions, define_asset_job

from .assets import build_floe_asset_defs
from .manifest import load_manifest
from .runner import LocalRunner, Runner


def build_runner_from_env() -> Runner:
    return LocalRunner(os.environ.get("FLOE_BIN", "floe"))


def build_definitions(
    *,
    manifest_path: str,
    entities: list[str] | None = None,
    runner: Runner | None = None,
    with_job: bool = True,
    job_name: str | None = None,
) -> Definitions:
    runner_impl = runner or build_runner_from_env()
    manifest = load_manifest(manifest_path)
    assets_defs, selected_entities = build_floe_asset_defs(
        manifest_path=manifest_path,
        runner=runner_impl,
        entities=entities,
    )

    jobs = []
    if with_job and selected_entities:
        selection = AssetSelection.assets(*[entity.asset_key for entity in selected_entities])
        jobs.append(
            define_asset_job(
                name=job_name or _default_job_name(manifest_path, manifest.manifest_id),
                selection=selection,
            )
        )

    return Definitions(assets=assets_defs, jobs=jobs)


def _default_job_name(manifest_path: str, manifest_id: str) -> str:
    base = manifest_id.strip() or Path(manifest_path).stem
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", base).strip("_")
    if not normalized:
        normalized = "manifest"
    if not re.match(r"[a-zA-Z_]", normalized):
        normalized = f"m_{normalized}"
    return f"floe_{normalized}_job"
