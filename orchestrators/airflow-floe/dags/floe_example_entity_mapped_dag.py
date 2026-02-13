"""Airflow demo DAG for Floe (advanced dynamic mapping).

This DAG loads a static manifest at import time, then:
- validates once
- maps one run task per entity defined in the manifest
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.sdk import dag, task

from floe_manifest import load_manifest
from floe_runtime import (
    build_asset_event_extra,
    build_entity_assets,
    load_run_summary,
    resolve_config_path,
    summary_entities_by_name,
)

# Override with env vars in real deployments.
FLOE_CMD = os.environ.get("FLOE_CMD", "floe")
FLOE_MANIFEST = os.environ.get(
    "FLOE_MANIFEST",
    str(Path(__file__).resolve().parents[1] / "example" / "manifest.airflow.json"),
)
MANIFEST = load_manifest(FLOE_MANIFEST)
ENTITY_ASSETS = build_entity_assets(MANIFEST, FLOE_MANIFEST)
ALL_ENTITY_ASSETS = list(ENTITY_ASSETS.values())
ENTITIES_BY_NAME = {entity.name: entity for entity in MANIFEST.entities}

FLOE_CONFIG = os.environ.get(
    "FLOE_CONFIG",
    resolve_config_path(FLOE_MANIFEST, MANIFEST.config_uri),
)
ENTITY_NAMES = [entity.name for entity in MANIFEST.entities]


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
    @task
    def validate_config() -> dict[str, Any]:
        cmd = [*_split_cmd(FLOE_CMD), "validate", "-c", FLOE_CONFIG, "--output", "json"]
        completed = _run_cli(cmd)

        payload = json.loads(completed.stdout)
        if payload.get("schema") != "floe.plan.v1":
            raise ValueError(f"unexpected validate schema: {payload.get('schema')}")
        if not bool(payload.get("valid", False)):
            raise ValueError("floe config is invalid; mapped run stopped")

        return {
            "config": FLOE_CONFIG,
            "entities": ENTITY_NAMES,
            "entity_count": len(ENTITY_NAMES),
        }

    @task(outlets=ALL_ENTITY_ASSETS)
    def run_entity(
        entity: str,
        validate_payload: dict[str, Any],
        *,
        outlet_events: dict[Any, Any] | None = None,
    ) -> dict[str, Any]:
        cmd = [
            *_split_cmd(FLOE_CMD),
            "run",
            "-c",
            validate_payload["config"],
            "--entities",
            entity,
            "--log-format",
            "json",
        ]
        completed = _run_cli(cmd)

        run_finished: dict[str, Any] | None = None
        for line in completed.stdout.splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("schema") != "floe.log.v1":
                continue
            if event.get("event") == "run_finished":
                run_finished = event

        if run_finished is None:
            raise ValueError("run_finished event not found")

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
            "config_uri": validate_payload["config"],
            "floe_log_schema": "floe.log.v1",
            "finished_at_ts_ms": run_finished["ts_ms"],
        }

    if not ENTITY_NAMES:
        raise ValueError("manifest has no entities")

    validate_payload = validate_config()
    run_entity.partial(validate_payload=validate_payload).expand(entity=ENTITY_NAMES)


floe_example_entity_mapped()
