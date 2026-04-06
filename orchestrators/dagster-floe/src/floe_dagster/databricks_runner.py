"""Databricks runner adapter for dagster-floe."""

from __future__ import annotations

import os

from .databricks_client import DatabricksJobSpec, DatabricksJobsClient
from .manifest import ManifestRunnerDefinition
from .runner import RunResult


def run_databricks_job(
    cmd_args: list[str],
    *,
    entity: str,
    runner: ManifestRunnerDefinition,
) -> RunResult:
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
    spec = DatabricksJobSpec(
        workspace_url=workspace_url,
        existing_cluster_id=existing_cluster_id,
        config_uri=config_uri,
        job_name=runner.job_name or "floe-{domain}-{env}",
        command=cmd_args[:1],
        args=cmd_args[1:],
        poll_interval_seconds=runner.poll_interval_seconds or 10,
        timeout_seconds=runner.timeout_seconds or 3600,
        env_parameters={**(runner.env_parameters or {}), "FLOE_ENTITY": entity},
    )
    job_id = client.ensure_domain_job(spec)
    run_id = client.run_now(job_id=job_id, env_parameters=spec.env_parameters)
    terminal = client.poll_run_to_terminal(
        run_id=run_id,
        poll_interval_seconds=spec.poll_interval_seconds,
        timeout_seconds=spec.timeout_seconds,
    )

    if terminal.result_state not in {None, "SUCCESS"}:
        return RunResult(
            stdout="",
            stderr=terminal.state_message or "Databricks run failed",
            exit_code=1,
            status=terminal.result_state or terminal.state,
            failure_reason=terminal.state_message,
            backend_metadata={"backend": "databricks", "run_page_url": terminal.run_page_url},
        )

    return RunResult(
        stdout="",
        stderr="",
        exit_code=0,
        status=terminal.result_state or terminal.state,
        backend_metadata={"backend": "databricks", "run_page_url": terminal.run_page_url},
    )


class _RequestsHttpClient:
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict:
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
