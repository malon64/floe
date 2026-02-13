"""Airflow demo DAG for Floe (advanced dynamic mapping).

This DAG loads a static manifest at import time, then:
- maps one run task per entity defined in the manifest
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.sdk import dag, task

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from airflow_floe.hooks import FloeManifestHook
from airflow_floe.runtime import (
    build_asset_event_extra,
    load_run_summary,
    parse_run_finished,
    summary_entities_by_name,
)

# Override with env vars in real deployments.
FLOE_CMD = os.environ.get("FLOE_CMD", "floe")
FLOE_MANIFEST = os.environ.get(
    "FLOE_MANIFEST",
    str(ROOT_DIR / "example" / "manifest.airflow.json"),
)
DEFAULT_CONFIG = str(ROOT_DIR / "example" / "config.yml")
MANIFEST_HOOK = FloeManifestHook(
    manifest_path=FLOE_MANIFEST,
    config_override=os.environ.get("FLOE_CONFIG"),
    default_config_path=DEFAULT_CONFIG,
)
CTX = MANIFEST_HOOK.get_context()
ENTITY_ASSETS = CTX.assets_by_entity
ALL_ENTITY_ASSETS = list(ENTITY_ASSETS.values())
ENTITIES_BY_NAME = CTX.entities_by_name
FLOE_CONFIG = CTX.config_path
ENTITY_NAMES = CTX.entity_names


def _split_cmd(command: str) -> list[str]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("FLOE_CMD cannot be empty")
    return parts


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=True, text=True, capture_output=True)


@dag(
    dag_id="floe_example_entity_mapped",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["floe", "example"],
)
def floe_example_entity_mapped() -> None:
    @task(outlets=ALL_ENTITY_ASSETS)
    def run_entity(
        entity: str,
        *,
        outlet_events: dict[Any, Any] | None = None,
    ) -> dict[str, Any]:
        cmd = [
            *_split_cmd(FLOE_CMD),
            "run",
            "-c",
            FLOE_CONFIG,
            "--entities",
            entity,
            "--log-format",
            "json",
        ]
        completed = _run_cli(cmd)
        run_finished = parse_run_finished(completed.stdout)

        summary = load_run_summary(run_finished.get("summary_uri"), FLOE_CONFIG)
        summary_entities = summary_entities_by_name(summary)
        manifest_entity = ENTITIES_BY_NAME[entity]

        if outlet_events is not None:
            asset = ENTITY_ASSETS[entity]
            event = outlet_events.get(asset)
            if event is not None:
                event.extra = build_asset_event_extra(
                    entity=manifest_entity,
                    run_finished=run_finished,
                    summary_entity=summary_entities.get(entity),
                )

        return {
            "schema": "floe.airflow.run.v1",
            "entity": entity,
            "run_id": run_finished["run_id"],
            "status": run_finished["status"],
            "exit_code": run_finished["exit_code"],
            "files": run_finished["files"],
            "rows": run_finished["rows"],
            "accepted": run_finished["accepted"],
            "rejected": run_finished["rejected"],
            "warnings": run_finished["warnings"],
            "errors": run_finished["errors"],
            "summary_uri": run_finished.get("summary_uri"),
            "config_uri": FLOE_CONFIG,
            "floe_log_schema": "floe.log.v1",
            "finished_at_ts_ms": run_finished["ts_ms"],
        }

    @task
    def run_config_without_manifest() -> dict[str, Any]:
        cmd = [*_split_cmd(FLOE_CMD), "run", "-c", FLOE_CONFIG, "--log-format", "json"]
        completed = _run_cli(cmd)
        run_finished = parse_run_finished(completed.stdout)
        return {
            "schema": "floe.airflow.run.v1",
            "run_id": run_finished["run_id"],
            "status": run_finished["status"],
            "exit_code": run_finished["exit_code"],
            "files": run_finished["files"],
            "rows": run_finished["rows"],
            "accepted": run_finished["accepted"],
            "rejected": run_finished["rejected"],
            "warnings": run_finished["warnings"],
            "errors": run_finished["errors"],
            "summary_uri": run_finished.get("summary_uri"),
            "config_uri": FLOE_CONFIG,
            "floe_log_schema": "floe.log.v1",
            "finished_at_ts_ms": run_finished["ts_ms"],
        }

    if ENTITY_NAMES:
        run_entity.expand(entity=ENTITY_NAMES)
    else:
        run_config_without_manifest()


floe_example_entity_mapped()
