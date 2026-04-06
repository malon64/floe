from __future__ import annotations

from floe_dagster.databricks_client import DatabricksRunResult
from floe_dagster.databricks_runner import run_databricks_job
from floe_dagster.k8s_status import STATUS_CANCELED, STATUS_FAILED, STATUS_SUCCEEDED, STATUS_TIMEOUT
from floe_dagster.manifest import ManifestRunnerDefinition


class FakeDatabricksClient:
    def __init__(self, terminal: DatabricksRunResult | None = None, exc: Exception | None = None) -> None:
        self._terminal = terminal
        self._exc = exc
        self.last_run_now: dict[str, object] | None = None

    def ensure_domain_job(self, spec):  # type: ignore[no-untyped-def]
        if self._exc is not None:
            raise self._exc
        return 123

    def run_now(self, *, job_id, env_parameters=None):  # type: ignore[no-untyped-def]
        self.last_run_now = {"job_id": job_id, "env_parameters": env_parameters}
        return 456

    def poll_run_to_terminal(self, *, run_id, poll_interval_seconds, timeout_seconds):  # type: ignore[no-untyped-def]
        del run_id, poll_interval_seconds, timeout_seconds
        if self._exc is not None:
            raise self._exc
        assert self._terminal is not None
        return self._terminal


def _runner() -> ManifestRunnerDefinition:
    return ManifestRunnerDefinition(
        runner_type="databricks_job",
        image=None,
        namespace=None,
        service_account=None,
        env=None,
        command=None,
        args=None,
        timeout_seconds=60,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=1,
        secrets=None,
        workspace_url="https://adb.example.com",
        existing_cluster_id="1111-222222-abc123",
        config_uri="dbfs:/floe/configs/prod.yml",
        job_name="floe-sales-prod",
        auth=None,
        env_parameters={"FLOE_ENV": "prod"},
    )


def test_run_databricks_job_success_sets_structured_backend_metadata(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "token")
    client = FakeDatabricksClient(
        terminal=DatabricksRunResult(
            run_id=456,
            state="TERMINATED",
            result_state="SUCCESS",
            state_message="ok",
            run_page_url="https://example/run/456",
        )
    )

    result = run_databricks_job(["floe", "run", "-c", "cfg.yml"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 0
    assert result.status == STATUS_SUCCEEDED
    assert result.failure_reason is None
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["backend_run_id"] == 456
    assert result.backend_metadata["job_id"] == 123
    assert result.backend_metadata["run_page_url"] == "https://example/run/456"
    assert client.last_run_now == {
        "job_id": 123,
        "env_parameters": {"FLOE_ENV": "prod", "FLOE_ENTITY": "orders"},
    }


def test_run_databricks_job_failure_maps_to_failed(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "token")
    client = FakeDatabricksClient(
        terminal=DatabricksRunResult(
            run_id=456,
            state="TERMINATED",
            result_state="FAILED",
            state_message="task failed",
            run_page_url="https://example/run/456",
        )
    )

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_FAILED
    assert result.failure_reason == "task failed"
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["backend_run_id"] == 456


def test_run_databricks_job_canceled_maps_to_canceled(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "token")
    client = FakeDatabricksClient(
        terminal=DatabricksRunResult(
            run_id=456,
            state="TERMINATED",
            result_state="CANCELED",
            state_message="run canceled",
            run_page_url="https://example/run/456",
        )
    )

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_CANCELED
    assert result.failure_reason == "run canceled"


def test_run_databricks_job_timeout_error_returns_timeout_status(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "token")
    client = FakeDatabricksClient(exc=TimeoutError("run 456 timed out"))

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_TIMEOUT
    assert result.failure_reason == "run 456 timed out"
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["error_type"] == "timeout"


def test_run_databricks_job_infra_failure_preserves_status_context(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "token")
    client = FakeDatabricksClient(exc=RuntimeError("jobs api unavailable"))

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_FAILED
    assert result.failure_reason == "jobs api unavailable"
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["error_type"] == "RuntimeError"
