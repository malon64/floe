from __future__ import annotations

import time
import uuid
from typing import Any

from .manifest import ManifestRunnerDefinition
from .runner import RunResult

STATUS_SUBMITTED = "submitted"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_TIMEOUT = "timeout"
STATUS_CANCELED = "canceled"

_DEFAULT_POLL_INTERVAL_S = 10
_DEFAULT_TIMEOUT_S = 3600


def run_kubernetes_job(
    cmd_args: list[str],
    *,
    entity: str,
    runner: ManifestRunnerDefinition,
    jobs_api: Any = None,
    core_api: Any = None,
) -> RunResult:
    namespace = runner.namespace
    if not namespace:
        raise ValueError("kubernetes_job runner requires a non-empty 'namespace' field")

    job_name = _make_job_name(entity)
    job_spec = build_k8s_job_spec(job_name, cmd_args, runner)

    poll_interval = runner.poll_interval_seconds or _DEFAULT_POLL_INTERVAL_S
    timeout = runner.timeout_seconds or _DEFAULT_TIMEOUT_S

    if jobs_api is None:
        jobs_api = _make_batch_client()
    if core_api is None:
        core_api = _make_core_client(jobs_api)

    jobs_api.create_namespaced_job(namespace=namespace, body=job_spec)

    deadline = time.monotonic() + timeout
    status = STATUS_SUBMITTED

    while True:
        job = jobs_api.read_namespaced_job(name=job_name, namespace=namespace)
        status = map_k8s_job_status(job)
        if status in (STATUS_SUCCEEDED, STATUS_FAILED, STATUS_TIMEOUT, STATUS_CANCELED):
            break
        if time.monotonic() >= deadline:
            status = STATUS_TIMEOUT
            break
        time.sleep(poll_interval)

    stdout = _collect_job_logs(core_api, job_name, namespace)
    exit_code = 0 if status == STATUS_SUCCEEDED else 1
    return RunResult(stdout=stdout, stderr="", exit_code=exit_code)


def build_k8s_job_spec(job_name: str, cmd_args: list[str], runner: ManifestRunnerDefinition) -> dict[str, Any]:
    if not runner.image:
        raise ValueError("kubernetes_job runner requires a non-empty 'image' field")
    if not runner.namespace:
        raise ValueError("kubernetes_job runner requires a non-empty 'namespace' field")

    container_cmd: list[str] = [cmd_args[0]] if cmd_args else []
    container_args: list[str] = list(cmd_args[1:]) if len(cmd_args) > 1 else []
    if runner.command is not None:
        container_cmd = runner.command
    if runner.args is not None:
        container_args = runner.args

    env_vars: list[dict[str, Any]] = []
    if runner.env:
        for k, v in runner.env.items():
            env_vars.append({"name": k, "value": v})
    if runner.secrets:
        for secret_ref in runner.secrets:
            env_vars.append(
                {
                    "name": secret_ref["name"],
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": secret_ref["secret_name"],
                            "key": secret_ref["key"],
                        }
                    },
                }
            )

    container: dict[str, Any] = {"name": "floe-runner", "image": runner.image}
    if container_cmd:
        container["command"] = container_cmd
    if container_args:
        container["args"] = container_args
    if env_vars:
        container["env"] = env_vars

    pod_spec: dict[str, Any] = {"containers": [container], "restartPolicy": "Never"}
    if runner.service_account:
        pod_spec["serviceAccountName"] = runner.service_account

    job_spec: dict[str, Any] = {
        "template": {
            "metadata": {"labels": {"app": "floe-runner", "floe-job": job_name}},
            "spec": pod_spec,
        },
        "backoffLimit": 0,
    }
    if runner.ttl_seconds_after_finished is not None:
        job_spec["ttlSecondsAfterFinished"] = runner.ttl_seconds_after_finished
    if runner.timeout_seconds is not None:
        job_spec["activeDeadlineSeconds"] = runner.timeout_seconds

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name, "namespace": runner.namespace, "labels": {"app": "floe-runner"}},
        "spec": job_spec,
    }


def map_k8s_job_status(job: Any) -> str:
    if job is None:
        return STATUS_CANCELED
    status = job.get("status") if isinstance(job, dict) else getattr(job, "status", None)
    if status is None:
        return STATUS_SUBMITTED

    if hasattr(status, "active"):
        active = status.active or 0
        succeeded = status.succeeded or 0
        failed = status.failed or 0
        conditions = status.conditions or []
    else:
        active = status.get("active") or 0
        succeeded = status.get("succeeded") or 0
        failed = status.get("failed") or 0
        conditions = status.get("conditions") or []

    for condition in conditions:
        if hasattr(condition, "type"):
            cond_type = condition.type or ""
            cond_status = condition.status or ""
            reason = condition.reason or ""
        else:
            cond_type = condition.get("type", "")
            cond_status = condition.get("status", "")
            reason = condition.get("reason", "")

        if cond_type == "Complete" and cond_status == "True":
            return STATUS_SUCCEEDED
        if cond_type == "Failed" and cond_status == "True":
            if reason == "DeadlineExceeded":
                return STATUS_TIMEOUT
            return STATUS_FAILED

    if succeeded > 0:
        return STATUS_SUCCEEDED
    if failed > 0:
        return STATUS_FAILED
    if active > 0:
        return STATUS_RUNNING
    return STATUS_SUBMITTED


def _make_job_name(entity: str) -> str:
    return f"floe-{entity.replace('_', '-').lower()[:20]}-{uuid.uuid4().hex[:8]}"


def _make_batch_client() -> Any:
    try:
        from kubernetes import client as k8s_client  # type: ignore[import]
        from kubernetes import config as k8s_config  # type: ignore[import]
    except ImportError as exc:
        raise ImportError("kubernetes Python package is required for kubernetes_job runner") from exc
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()
    return k8s_client.BatchV1Api()


def _make_core_client(batch_client: Any) -> Any:
    from kubernetes import client as k8s_client  # type: ignore[import]

    api_client = getattr(batch_client, "api_client", None)
    if api_client is not None:
        return k8s_client.CoreV1Api(api_client)
    return k8s_client.CoreV1Api()


def _collect_job_logs(core_api: Any, job_name: str, namespace: str) -> str:
    try:
        pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=f"floe-job={job_name}")
        items = getattr(pods, "items", None) or pods.get("items", [])
        if not items:
            return ""
        pod = items[0]
        pod_name = pod.metadata.name if hasattr(pod, "metadata") else pod.get("metadata", {}).get("name", "")
        if not pod_name:
            return ""
        return core_api.read_namespaced_pod_log(name=pod_name, namespace=namespace) or ""
    except Exception:
        return ""
