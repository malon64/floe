"""Airflow demo DAG for Floe (default config-level run).

This DAG follows the default Airflow approach for Floe:
- validate once for the full config
- run once for the full config
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
        entity_count = len(payload.get("plan", {}).get("entities", []))

        return {
            "schema": "floe.airflow.validate.v1",
            "config_uri": FLOE_CONFIG,
            "valid": bool(payload.get("valid", False)),
            "error_count": len(errors),
            "warning_count": len(warnings),
            "entity_count": entity_count,
            "floe_schema": "floe.plan.v1",
            "generated_at_ts_ms": payload.get("generated_at_ts_ms", 0),
        }

    @task
    def run_config(validate_payload: dict[str, Any]) -> dict[str, Any]:
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
