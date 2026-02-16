from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass

from .manifest import ManifestExecution, ManifestRunnerDefinition, render_execution_args


@dataclass(frozen=True)
class RunResult:
    stdout: str
    stderr: str
    exit_code: int


class Runner:
    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution: ManifestExecution | None = None,
        runner_definition: ManifestRunnerDefinition | None = None,
    ) -> RunResult:
        raise NotImplementedError


class LocalRunner(Runner):
    def __init__(self, floe_bin: str = "floe") -> None:
        self._floe_cmd = shlex.split(floe_bin)

    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution: ManifestExecution | None = None,
        runner_definition: ManifestRunnerDefinition | None = None,
    ) -> RunResult:
        if runner_definition is not None and runner_definition.runner_type != "local_process":
            raise ValueError(
                "unsupported runner type for LocalRunner: "
                f"{runner_definition.runner_type}"
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
                    execution, config_uri=config_uri, entity_name=entity, run_id=run_id
                )
            )
            if run_id and not _contains_run_id_placeholder(execution):
                args.extend(["--run-id", run_id])
        else:
            args = [*self._floe_cmd, "run", "-c", config_uri, "--entities", entity]
            if run_id:
                args.extend(["--run-id", run_id])
            args.extend(["--log-format", log_format])
        return _run(args)


def _run(args: list[str], cwd: str | None = None) -> RunResult:
    env = os.environ.copy()
    proc = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
    )
    return RunResult(stdout=proc.stdout, stderr=proc.stderr, exit_code=proc.returncode)


def _contains_run_id_placeholder(execution: ManifestExecution) -> bool:
    return any("{run_id}" in token for token in execution.base_args) or any(
        "{run_id}" in token for token in execution.per_entity_args
    )
