"""Databricks runner adapter for airflow-floe."""

from __future__ import annotations

import os
import time
from typing import Any

from .databricks_client import DatabricksJobSpec, DatabricksJobsClient
from .manifest import ManifestRunnerDefinition


def run_databricks_job(
    *,
    cmd_args: list[str],
    runner: ManifestRunnerDefinition,
    entities: list[str] | None,
) -> dict[str, Any]:
    workspace_url = runner.workspace_url
    existing_cluster_id = runner.existing_cluster_id
    config_uri = runner.config_uri
    if not workspace_url or not existing_cluster_id or not config_uri:
        raise ValueError(
            "databricks_job runner requires workspace_url, existing_cluster_id, and config_uri"
        )

    oauth_token = os.environ.get("FLOE_DATABRICKS_OAUTH_TOKEN")
    if not oauth_token:
        raise ValueError("FLOE_DATABRICKS_OAUTH_TOKEN is required for databricks_job runner")

    client = DatabricksJobsClient(
        http_client=_RequestsHttpClient(),
        workspace_url=workspace_url,
        oauth_bearer_token=oauth_token,
    )
    job_name = runner.job_name or "floe-{domain}-{env}"
    env_parameters = dict(runner.env_parameters or {})
    if entities:
        env_parameters.setdefault("FLOE_ENTITIES", ",".join(entities))

    spec = DatabricksJobSpec(
        workspace_url=workspace_url,
        existing_cluster_id=existing_cluster_id,
        config_uri=config_uri,
        job_name=job_name,
        command=cmd_args[:1],
        args=cmd_args[1:],
        poll_interval_seconds=runner.poll_interval_seconds or 10,
        timeout_seconds=runner.timeout_seconds or 3600,
        env_parameters=env_parameters,
    )
    job_id = client.ensure_domain_job(spec)
    run_id = client.run_now(job_id=job_id, env_parameters=spec.env_parameters)
    terminal = client.poll_run_to_terminal(
        run_id=run_id,
        poll_interval_seconds=spec.poll_interval_seconds,
        timeout_seconds=spec.timeout_seconds,
    )
    status = _map_databricks_status(
        life_cycle_state=terminal.state,
        result_state=terminal.result_state,
    )
    failure_reason = _resolve_failure_reason(
        status=status,
        result_state=terminal.result_state,
        state_message=terminal.state_message,
    )

    payload: dict[str, Any] = {
        "schema": "floe.airflow.run.v1",
        "run_id": str(run_id),
        "status": status,
        "exit_code": 0 if status == "success" else 1,
        "files": 0,
        "rows": 0,
        "accepted": 0,
        "rejected": 0,
        "warnings": 0,
        "errors": 0,
        "summary_uri": None,
        "config_uri": config_uri,
        "floe_log_schema": "floe.log.v1",
        "finished_at_ts_ms": int(time.time() * 1000),
        "backend_type": "databricks",
        "backend_run_id": str(run_id),
        "backend_status": terminal.result_state or terminal.state,
        "backend_metadata": {
            "job_id": job_id,
            "life_cycle_state": terminal.state,
            "result_state": terminal.result_state,
            "run_page_url": terminal.run_page_url,
        },
    }
    if failure_reason:
        payload["failure_reason"] = failure_reason
    if entities and len(entities) == 1:
        payload["entity"] = entities[0]
    return payload


def _map_databricks_status(*, life_cycle_state: str | None, result_state: str | None) -> str:
    normalized_result = (result_state or "").strip().upper()
    normalized_lifecycle = (life_cycle_state or "").strip().upper()

    if normalized_result == "SUCCESS":
        return "success"
    if normalized_result in {"TIMEDOUT"}:
        return "timeout"
    if normalized_result in {"CANCELED", "CANCELLED"}:
        return "canceled"
    if normalized_result:
        return "failed"

    if normalized_lifecycle in {"SKIPPED"}:
        return "canceled"
    if normalized_lifecycle in {"INTERNAL_ERROR"}:
        return "failed"
    if normalized_lifecycle in {"TERMINATED"}:
        return "failed"
    return "failed"


def _resolve_failure_reason(*, status: str, result_state: str | None, state_message: str | None) -> str | None:
    if status == "success":
        return None
    if state_message:
        return state_message
    if result_state:
        return str(result_state)
    return None


class _RequestsHttpClient:
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        import requests

        response = requests.request(
            method,
            url,
            headers=headers,
            json=json_body,
            timeout=timeout_seconds or 30,
        )
        response.raise_for_status()
        return response.json() if response.text else {}
