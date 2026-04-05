from __future__ import annotations

from unittest.mock import MagicMock, patch

from floe_dagster.kubernetes_runner import (
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUBMITTED,
    STATUS_SUCCEEDED,
    build_k8s_job_spec,
    map_k8s_job_status,
    run_kubernetes_job,
)
from floe_dagster.manifest import ManifestRunnerDefinition


def _runner() -> ManifestRunnerDefinition:
    return ManifestRunnerDefinition(
        runner_type="kubernetes_job",
        image="floe-runner:latest",
        namespace="data",
        service_account=None,
        env={"LOG_LEVEL": "INFO"},
        command=None,
        args=None,
        timeout_seconds=60,
        ttl_seconds_after_finished=30,
        poll_interval_seconds=1,
        secrets=None,
    )


def _job(active: int = 0, succeeded: int = 0, failed: int = 0, conditions=None):
    return {
        "status": {
            "active": active,
            "succeeded": succeeded,
            "failed": failed,
            "conditions": conditions or [],
        }
    }


def test_build_k8s_job_spec_minimal_mapping() -> None:
    spec = build_k8s_job_spec("floe-orders-abc", ["floe", "run", "-c", "cfg.yml"], _runner())
    container = spec["spec"]["template"]["spec"]["containers"][0]
    assert spec["metadata"]["namespace"] == "data"
    assert container["image"] == "floe-runner:latest"
    assert container["command"] == ["floe"]
    assert container["args"] == ["run", "-c", "cfg.yml"]


def test_status_mapping_states() -> None:
    assert map_k8s_job_status(_job(active=1)) == STATUS_RUNNING
    assert map_k8s_job_status(_job(succeeded=1)) == STATUS_SUCCEEDED
    assert map_k8s_job_status(_job(failed=1)) == STATUS_FAILED
    assert map_k8s_job_status({"status": None}) == STATUS_SUBMITTED


def test_run_kubernetes_job_success() -> None:
    jobs_api = MagicMock()
    jobs_api.create_namespaced_job.return_value = None
    jobs_api.read_namespaced_job.side_effect = [
        _job(active=1),
        _job(succeeded=1, conditions=[{"type": "Complete", "status": "True"}]),
    ]

    core_api = MagicMock()
    pods = MagicMock()
    pod = MagicMock()
    pod.metadata.name = "pod-1"
    pods.items = [pod]
    core_api.list_namespaced_pod.return_value = pods
    core_api.read_namespaced_pod_log.return_value = '{"schema":"floe.log.v1","event":"run_finished","status":"success"}'

    with patch("time.sleep"):
        result = run_kubernetes_job(
            ["floe", "run", "-c", "cfg.yml"],
            entity="orders",
            runner=_runner(),
            jobs_api=jobs_api,
            core_api=core_api,
        )

    assert result.exit_code == 0
    assert "run_finished" in result.stdout
