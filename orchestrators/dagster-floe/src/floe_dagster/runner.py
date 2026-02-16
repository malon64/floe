from __future__ import annotations

import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path

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


class DockerRunner(Runner):
    def __init__(
        self,
        image: str,
        docker_bin: str = "docker",
        workdir: str | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        self._image = image
        self._docker = docker_bin
        self._workdir = workdir
        self._env = env or {}

    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution: ManifestExecution | None = None,
        runner_definition: ManifestRunnerDefinition | None = None,
    ) -> RunResult:
        if runner_definition is not None and runner_definition.runner_type not in (
            "docker",
            "local_process",
        ):
            raise ValueError(
                "unsupported runner type for DockerRunner: "
                f"{runner_definition.runner_type}"
            )

        if execution is not None:
            args = render_execution_args(
                execution, config_uri=config_uri, entity_name=entity, run_id=run_id
            )
            if run_id and not _contains_run_id_placeholder(execution):
                args.extend(["--run-id", run_id])
        else:
            args = ["run", "-c", config_uri, "--entities", entity, "--log-format", log_format]
            if run_id:
                args.extend(["--run-id", run_id])
        return self._run_in_container(args, config_uri=config_uri)

    def _run_in_container(self, floe_args: list[str], config_uri: str) -> RunResult:
        docker_cmd = [self._docker, "run", "--rm"]

        for key, value in self._env.items():
            docker_cmd.extend(["-e", f"{key}={value}"])

        if self._workdir:
            host_dir = Path(self._workdir).resolve()
            docker_cmd.extend(["-v", f"{host_dir}:/work", "-w", "/work"])
        else:
            maybe_path = Path(config_uri)
            if not config_uri.startswith(("s3://", "gs://", "abfs://")) and maybe_path.exists():
                config_path = maybe_path.resolve()
                mount_root = _infer_mount_root_for_config(config_path)
                docker_cmd.extend(["-v", f"{mount_root}:/work"])
                # When mounting local files, run the container as the current user so Floe
                # can write reports/outputs back into the mounted directory.
                uid = getattr(os, "getuid", None)
                gid = getattr(os, "getgid", None)
                if uid is not None and gid is not None:
                    docker_cmd.extend(["--user", f"{uid()}:{gid()}"])

                floe_args = floe_args.copy()
                if "-c" in floe_args:
                    idx = floe_args.index("-c")
                    container_config_path = Path("/work").joinpath(
                        config_path.relative_to(mount_root)
                    )
                    floe_args[idx + 1] = str(container_config_path)

                    container_config_dir = container_config_path.parent
                    docker_cmd.extend(["-w", str(container_config_dir)])

        docker_cmd.append(self._image)
        docker_cmd.extend(floe_args)

        return _run(docker_cmd)


def _infer_mount_root_for_config(config_path: Path) -> Path:
    """
    Determine a mount root for DockerRunner when config paths may use '../'.

    Floe resolves relative paths against the *config directory*; if a config references
    paths like '../data', mounting only the config dir hides those referenced paths.

    Heuristic:
    - Scan the config file text for sequences like '../' or '../../'
    - Mount the config dir ancestor that makes those paths visible inside the container.
    """
    config_dir = config_path.parent
    max_ups = 0
    try:
        text = config_path.read_text(encoding="utf-8", errors="ignore")
        for match in re.finditer(r"(?:(?:\.\./)+)", text):
            ups = match.group(0).count("../")
            if ups > max_ups:
                max_ups = ups
    except OSError:
        max_ups = 0

    mount_root = config_dir
    for _ in range(max_ups):
        parent = mount_root.parent
        if parent == mount_root:
            break
        mount_root = parent
    return mount_root


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
