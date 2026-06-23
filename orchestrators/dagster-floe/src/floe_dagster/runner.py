from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from .manifest import ManifestExecution, ManifestRunnerDefinition, render_execution_args


@dataclass(frozen=True)
class RunResult:
    stdout: str
    stderr: str
    exit_code: int
    status: str | None = None
    failure_reason: str | None = None
    backend_metadata: dict[str, str | int | None] = field(default_factory=dict)


class Runner:
    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution: ManifestExecution | None = None,
        runner_definition: ManifestRunnerDefinition | None = None,
        manifest_uri: str | None = None,
        dagster_job_name: str | None = None,
    ) -> RunResult:
        raise NotImplementedError


class LocalRunner(Runner):
    def __init__(self, floe_bin: str = "floe", timeout: float | None = None) -> None:
        self._floe_cmd = shlex.split(floe_bin)
        self._timeout = timeout

    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution: ManifestExecution | None = None,
        runner_definition: ManifestRunnerDefinition | None = None,
        manifest_uri: str | None = None,
        dagster_job_name: str | None = None,
    ) -> RunResult:
        if runner_definition is not None and runner_definition.runner_type == "kubernetes_job":
            if execution is None:
                raise ValueError("execution contract is required for kubernetes_job runner")
            args = [*self._floe_cmd]
            args.extend(
                render_execution_args(
                    execution,
                    config_uri=config_uri,
                    entity_name=entity,
                    run_id=run_id,
                    manifest_uri=manifest_uri,
                )
            )
            from .kubernetes_runner import run_kubernetes_job

            return run_kubernetes_job(args, entity=entity, runner=runner_definition)

        if runner_definition is not None and runner_definition.runner_type == "databricks_job":
            if execution is None:
                raise ValueError("execution contract is required for databricks_job runner")
            args = [*self._floe_cmd]
            args.extend(
                render_execution_args(
                    execution,
                    config_uri=config_uri,
                    entity_name=entity,
                    run_id=run_id,
                    manifest_uri=manifest_uri,
                )
            )
            from .databricks_runner import run_databricks_job

            return run_databricks_job(args, entity=entity, runner=runner_definition)

        if runner_definition is not None and runner_definition.runner_type != "local_process":
            raise NotImplementedError(
                "unsupported runner type for dagster-floe LocalRunner: "
                f"{runner_definition.runner_type!r}"
            )

        if execution is not None:
            if execution.log_format != "json":
                raise ValueError(
                    "unsupported execution.log_format for LocalRunner: "
                    f"{execution.log_format}"
                )
            if not execution.result_contract.run_finished_event:
                raise ValueError(
                    "execution.result_contract.run_finished_event must be true"
                )
            args = [*self._floe_cmd]
            args.extend(
                render_execution_args(
                    execution,
                    config_uri=config_uri,
                    entity_name=entity,
                    run_id=run_id,
                    manifest_uri=manifest_uri,
                )
            )
            if run_id and not _contains_run_id_placeholder(execution):
                args.extend(["--run-id", run_id])
            cwd = _resolve_workdir(
                config_uri=config_uri, workdir=execution.defaults.workdir
            )
            env_overrides = execution.defaults.env
        else:
            args = [*self._floe_cmd, "run", "-c", config_uri, "--entities", entity]
            if run_id:
                args.extend(["--run-id", run_id])
            args.extend(["--log-format", log_format])
            cwd = None
            env_overrides = None
        return _run(
            args,
            cwd=cwd,
            env_overrides=env_overrides,
            dagster_job_name=dagster_job_name,
            timeout=self._timeout,
        )


# Conventional exit code for a process killed by a timeout (matches timeout(1)).
_TIMEOUT_EXIT_CODE = 124


def _run(
    args: list[str],
    cwd: str | None = None,
    env_overrides: dict[str, str] | None = None,
    dagster_job_name: str | None = None,
    timeout: float | None = None,
) -> RunResult:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    if dagster_job_name:
        env["DAGSTER_JOB_NAME"] = dagster_job_name
    try:
        proc = subprocess.run(
            args,
            cwd=cwd,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        # Surface the timeout as a failed RunResult (non-zero exit) rather than
        # raising, so the asset layer turns it into a clean Dagster Failure with
        # the reason in metadata — mirroring how the k8s/databricks runners
        # report failures and keeping runner.py free of any dagster imports.
        return RunResult(
            stdout=_as_text(exc.stdout),
            stderr=_as_text(exc.stderr),
            exit_code=_TIMEOUT_EXIT_CODE,
            status="timeout",
            failure_reason=f"floe run exceeded the {timeout}s timeout and was terminated",
        )
    return RunResult(stdout=proc.stdout, stderr=proc.stderr, exit_code=proc.returncode)


def _as_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _contains_run_id_placeholder(execution: ManifestExecution) -> bool:
    return any("{run_id}" in token for token in execution.base_args) or any(
        "{run_id}" in token for token in execution.per_entity_args
    )


def _resolve_workdir(config_uri: str, workdir: str | None) -> str | None:
    if not workdir:
        return None

    workdir_path = Path(workdir)
    if workdir_path.is_absolute():
        return str(workdir_path)

    if "://" not in config_uri:
        return str((Path(config_uri).resolve().parent / workdir_path).resolve())

    return str(workdir_path.resolve())
