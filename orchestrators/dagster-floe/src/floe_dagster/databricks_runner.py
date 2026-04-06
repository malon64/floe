"""Databricks runner adapter for dagster-floe."""

from __future__ import annotations

import os

from .databricks_client import DatabricksJobSpec, DatabricksJobsClient, DatabricksRunResult
from .k8s_status import STATUS_CANCELED, STATUS_FAILED, STATUS_SUCCEEDED, STATUS_TIMEOUT
from .manifest import ManifestRunnerDefinition
from .runner import RunResult


def run_databricks_job(
    cmd_args: list[str],
    *,
    entity: str,
    runner: ManifestRunnerDefinition,
    client: DatabricksJobsClient | None = None,
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

    dbx_client = client or DatabricksJobsClient(
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

    job_id: int | None = None
    run_id: int | None = None
    try:
        job_id = dbx_client.ensure_domain_job(spec)
        run_id = dbx_client.run_now(job_id=job_id, env_parameters=spec.env_parameters)
        terminal = dbx_client.poll_run_to_terminal(
            run_id=run_id,
            poll_interval_seconds=spec.poll_interval_seconds,
            timeout_seconds=spec.timeout_seconds,
        )
    except TimeoutError as exc:
        return _infra_failure_result(
            status=STATUS_TIMEOUT,
            failure_reason=str(exc),
            stderr=str(exc),
            workspace_url=workspace_url,
            job_id=job_id,
            run_id=run_id,
            error_type="timeout",
        )
    except Exception as exc:  # noqa: BLE001
        return _infra_failure_result(
            status=STATUS_FAILED,
            failure_reason=str(exc) or exc.__class__.__name__,
            stderr=str(exc),
            workspace_url=workspace_url,
            job_id=job_id,
            run_id=run_id,
            error_type=exc.__class__.__name__,
        )

    status = _map_status(terminal)
    backend_metadata = _backend_metadata(
        workspace_url=workspace_url,
        terminal=terminal,
        job_id=job_id,
    )

    if status != STATUS_SUCCEEDED:
        return RunResult(
            stdout="",
            stderr=terminal.state_message or "Databricks run failed",
            exit_code=1,
            status=status,
            failure_reason=terminal.state_message,
            backend_metadata=backend_metadata,
        )

    return RunResult(
        stdout="",
        stderr="",
        exit_code=0,
        status=status,
        failure_reason=None,
        backend_metadata=backend_metadata,
    )


def _map_status(terminal: DatabricksRunResult) -> str:
    result_state = (terminal.result_state or "").upper()
    life_cycle_state = (terminal.state or "").upper()

    if result_state == "SUCCESS":
        return STATUS_SUCCEEDED
    if result_state in {"CANCELED", "CANCELLED"}:
        return STATUS_CANCELED
    if result_state == "TIMEDOUT":
        return STATUS_TIMEOUT
    if result_state:
        return STATUS_FAILED

    if life_cycle_state in {"SKIPPED"}:
        return STATUS_CANCELED
    if life_cycle_state in {"INTERNAL_ERROR"}:
        return STATUS_FAILED

    return STATUS_FAILED


def _backend_metadata(
    *,
    workspace_url: str,
    terminal: DatabricksRunResult,
    job_id: int | None,
) -> dict[str, str | int | None]:
    return {
        "backend_type": "databricks",
        "backend_run_id": terminal.run_id,
        "workspace_url": workspace_url,
        "job_id": job_id,
        "run_page_url": terminal.run_page_url,
        "life_cycle_state": terminal.state,
        "result_state": terminal.result_state,
        "state_message": terminal.state_message,
    }


def _infra_failure_result(
    *,
    status: str,
    failure_reason: str,
    stderr: str,
    workspace_url: str,
    job_id: int | None,
    run_id: int | None,
    error_type: str,
) -> RunResult:
    backend_metadata: dict[str, str | int | None] = {
        "backend_type": "databricks",
        "backend_run_id": run_id,
        "workspace_url": workspace_url,
        "job_id": job_id,
        "error_type": error_type,
        "state_message": failure_reason,
    }
    return RunResult(
        stdout="",
        stderr=stderr,
        exit_code=1,
        status=status,
        failure_reason=failure_reason,
        backend_metadata=backend_metadata,
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
