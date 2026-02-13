"""Airflow demo DAG using the reusable FloeRunOperator."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from airflow_floe.hooks import FloeManifestHook
from airflow_floe.operators import FloeRunOperator

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
