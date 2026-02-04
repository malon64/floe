from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RunResult:
    stdout: str
    stderr: str
    exit_code: int


class Runner:
    def run_floe_validate(self, config_uri: str, entities: list[str] | None) -> dict[str, Any]:
        raise NotImplementedError

    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
    ) -> RunResult:
        raise NotImplementedError


class LocalRunner(Runner):
    def __init__(self, floe_bin: str = "floe") -> None:
        self._floe_cmd = shlex.split(floe_bin)

    def run_floe_validate(self, config_uri: str, entities: list[str] | None) -> dict[str, Any]:
        args = [*self._floe_cmd, "validate", "-c", config_uri, "--output", "json"]
        if entities:
            args.extend(["--entities", ",".join(entities)])
        result = _run(args)
        if result.exit_code != 0:
            raise RuntimeError(result.stdout.strip() or result.stderr.strip())
        import json

        return json.loads(result.stdout)

    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
    ) -> RunResult:
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

    def run_floe_validate(self, config_uri: str, entities: list[str] | None) -> dict[str, Any]:
        args = ["validate", "-c", config_uri, "--output", "json"]
        if entities:
            args.extend(["--entities", ",".join(entities)])
        result = self._run_in_container(args, config_uri=config_uri)
        if result.exit_code != 0:
            raise RuntimeError(result.stdout.strip() or result.stderr.strip())
        import json

        return json.loads(result.stdout)

    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
    ) -> RunResult:
        args = ["run", "-c", config_uri, "--entities", entity, "--log-format", log_format]
        if run_id:
            args.extend(["--run-id", run_id])
        return self._run_in_container(args, config_uri=config_uri)

    def _run_in_container(self, floe_args: list[str], config_uri: str) -> RunResult:
        docker_cmd = [self._docker, "run", "--rm"]

        for key, value in self._env.items():
            docker_cmd.extend(["-e", f"{key}={value}"])

        container_workdir = None
        if self._workdir:
            host_dir = Path(self._workdir).resolve()
            docker_cmd.extend(["-v", f"{host_dir}:/work", "-w", "/work"])
            container_workdir = "/work"
        else:
            maybe_path = Path(config_uri)
            if not config_uri.startswith(("s3://", "gs://", "abfs://")) and maybe_path.exists():
                host_dir = maybe_path.resolve().parent
                docker_cmd.extend(["-v", f"{host_dir}:/work", "-w", "/work"])
                container_workdir = "/work"

                floe_args = floe_args.copy()
                if "-c" in floe_args:
                    idx = floe_args.index("-c")
                    floe_args[idx + 1] = f"/work/{Path(floe_args[idx + 1]).name}"

        docker_cmd.append(self._image)
        docker_cmd.extend(floe_args)

        return _run(docker_cmd, cwd=container_workdir)


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
