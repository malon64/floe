"""Airflow demo DAG for Floe (advanced dynamic mapping).

This example uses Floe CLI contracts directly:
- validate: floe validate --output json
- run: floe run --log-format json
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task

# Override with env vars in real deployments.
FLOE_CMD = os.environ.get("FLOE_CMD", "floe")
FLOE_CONFIG = os.environ.get(
    "FLOE_CONFIG",
    str(Path(__file__).resolve().parents[1] / "example" / "config.yml"),
)


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
    def validate_plan() -> dict[str, Any]:
        cmd = [*_split_cmd(FLOE_CMD), "validate", "-c", FLOE_CONFIG, "--output", "json"]
        completed = _run_cli(cmd)

        payload = json.loads(completed.stdout)
        if payload.get("schema") != "floe.plan.v1":
            raise ValueError(f"unexpected validate schema: {payload.get('schema')}")

        entities = [
            entity["name"]
            for entity in payload.get("plan", {}).get("entities", [])
            if entity.get("name")
        ]
        if not entities:
            raise ValueError("no entities found in validate plan")

        return {
            "config": FLOE_CONFIG,
            "entities": entities,
            "entity_count": len(entities),
        }

    @task
    def extract_entities(plan: dict[str, Any]) -> list[str]:
        return plan["entities"]

    @task
    def run_entity(entity: str, plan: dict[str, Any]) -> dict[str, Any]:
        cmd = [
            *_split_cmd(FLOE_CMD),
            "run",
            "-c",
            plan["config"],
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
            "config_uri": plan["config"],
            "floe_log_schema": "floe.log.v1",
            "finished_at_ts_ms": run_finished["ts_ms"],
        }

    plan = validate_plan()
    entities = extract_entities(plan)
    run_entity.partial(plan=plan).expand(entity=entities)


floe_example_entity_mapped()
