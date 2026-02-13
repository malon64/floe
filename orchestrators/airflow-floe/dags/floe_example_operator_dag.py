"""Airflow demo DAG using the reusable FloeRunOperator."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag

from floe_hook import FloeManifestHook
from floe_operators import FloeRunOperator

FLOE_CMD = os.environ.get("FLOE_CMD", "floe")
FLOE_MANIFEST = os.environ.get(
    "FLOE_MANIFEST",
    str(Path(__file__).resolve().parents[1] / "example" / "manifest.airflow.json"),
)
DEFAULT_CONFIG = str(Path(__file__).resolve().parents[1] / "example" / "config.yml")
MANIFEST_HOOK = FloeManifestHook(
    manifest_path=FLOE_MANIFEST,
    config_override=os.environ.get("FLOE_CONFIG"),
    default_config_path=DEFAULT_CONFIG,
)
FLOE_CONFIG = MANIFEST_HOOK.get_config_path()


@dag(
    dag_id="floe_example_operator",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["floe", "example", "operator"],
)
def floe_example_operator() -> None:
    FloeRunOperator(
        task_id="run_floe",
        floe_cmd=FLOE_CMD,
        config_path=FLOE_CONFIG,
    )


floe_example_operator()
