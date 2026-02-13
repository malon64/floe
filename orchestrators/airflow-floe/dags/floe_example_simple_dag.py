"""Airflow demo DAG for Floe (default config-level run).

This DAG loads a static manifest at import time, then:
- validates once for the full config
- runs once for the full config
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

FLOE_CMD = os.environ.get("FLOE_CMD", "floe")
FLOE_MANIFEST = os.environ.get(
    "FLOE_MANIFEST",
    str(Path(__file__).resolve().parents[1] / "example" / "manifest.airflow.json"),
)
MANIFEST = load_manifest(FLOE_MANIFEST)
ENTITY_ASSETS = build_entity_assets(MANIFEST, FLOE_MANIFEST)
ALL_ENTITY_ASSETS = list(ENTITY_ASSETS.values())

FLOE_CONFIG = os.environ.get(
    "FLOE_CONFIG",
    resolve_config_path(FLOE_MANIFEST, MANIFEST.config_uri),
)


def _split_cmd(command: str) -> list[str]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("FLOE_CMD cannot be empty")
    return parts


def _run_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=True, text=True, capture_output=True)


@dag(
    dag_id="floe_example_simple",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["floe", "example"],
)
def floe_example_simple() -> None:
    @task
    def validate_config() -> dict[str, Any]:
        cmd = [*_split_cmd(FLOE_CMD), "validate", "-c", FLOE_CONFIG, "--output", "json"]
        completed = _run_cli(cmd)

        payload = json.loads(completed.stdout)
        if payload.get("schema") != "floe.plan.v1":
            raise ValueError(f"unexpected validate schema: {payload.get('schema')}")

        errors = payload.get("errors", [])
        warnings = payload.get("warnings", [])
        entity_names = [entity.name for entity in MANIFEST.entities]

        return {
            "schema": "floe.airflow.validate.v1",
            "config_uri": FLOE_CONFIG,
            "valid": bool(payload.get("valid", False)),
            "error_count": len(errors),
            "warning_count": len(warnings),
            "entity_count": len(entity_names),
            "selected_entities": entity_names,
            "floe_schema": "floe.plan.v1",
            "generated_at_ts_ms": payload.get("generated_at_ts_ms", 0),
        }

    @task(outlets=ALL_ENTITY_ASSETS)
    def run_config(
        validate_payload: dict[str, Any],
        *,
        outlet_events: dict[Any, Any] | None = None,
    ) -> dict[str, Any]:
        if not validate_payload["valid"]:
            raise ValueError("floe config is invalid; run step stopped")

        cmd = [*_split_cmd(FLOE_CMD), "run", "-c", FLOE_CONFIG, "--log-format", "json"]
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

        if outlet_events is not None:
            for entity in MANIFEST.entities:
                asset = ENTITY_ASSETS[entity.name]
                event = outlet_events.get(asset)
                if event is None:
                    continue
                event.extra = build_asset_event_extra(
                    entity=entity,
                    run_finished=run_finished,
                    summary_entity=summary_entities.get(entity.name),
                )

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

    run_config(validate_config())


floe_example_simple()
