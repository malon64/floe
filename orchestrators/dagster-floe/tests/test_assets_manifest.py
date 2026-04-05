from pathlib import Path
import subprocess
from unittest.mock import patch

import pytest

from floe_dagster.assets import load_floe_assets
from floe_dagster.manifest import ManifestRunnerDefinition
from floe_dagster.runner import LocalRunner, RunResult, Runner


class _NoopRunner(Runner):
    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution=None,
        runner_definition=None,
    ) -> RunResult:
        del config_uri, run_id, entity, log_format, execution, runner_definition
        return RunResult(stdout="", stderr="", exit_code=0)


FIXTURE = Path(__file__).parent / "fixtures" / "manifest.json"


# ── load-time: manifest parsing ───────────────────────────────────────────────

def test_no_runner_manifest_loads_and_defaults_to_local() -> None:
    """Entity with no explicit runner → falls back to manifest default (local_process)."""
    defs = load_floe_assets(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],  # employees.runner == null → default "local" (local_process)
    )
    assert defs is not None


def test_local_runner_manifest_loads_successfully() -> None:
    """Entity explicitly assigned the local_process runner loads without error."""
    defs = load_floe_assets(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert defs is not None


def test_kubernetes_runner_manifest_loads_successfully() -> None:
    """Entity with a non-local runner should load — error is deferred to execution time."""
    defs = load_floe_assets(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["orders"],  # orders.runner == "k8s" (kubernetes_job)
    )
    assert defs is not None


# ── execution-time: LocalRunner routing ───────────────────────────────────────

def test_local_runner_accepts_local_process_definition() -> None:
    """LocalRunner.run_floe_entity proceeds when runner_definition.runner_type is local_process."""
    runner = LocalRunner(floe_bin="floe")
    definition = ManifestRunnerDefinition(
        runner_type="local_process",
        image=None,
        namespace=None,
        service_account=None,
        env=None,
    )
    fake_result = subprocess.CompletedProcess(
        args=[], returncode=0, stdout="", stderr=""
    )
    with patch("subprocess.run", return_value=fake_result):
        result = runner.run_floe_entity(
            config_uri="local:///tmp/config.yml",
            run_id=None,
            entity="employees",
            runner_definition=definition,
        )
    assert result.exit_code == 0


def test_local_runner_raises_not_implemented_for_kubernetes_runner() -> None:
    """LocalRunner raises NotImplementedError for non-local_process runner types."""
    runner = LocalRunner(floe_bin="floe")
    definition = ManifestRunnerDefinition(
        runner_type="kubernetes",
        image=None,
        namespace=None,
        service_account=None,
        env=None,
    )
    with pytest.raises(NotImplementedError) as exc_info:
        runner.run_floe_entity(
            config_uri="local:///tmp/config.yml",
            run_id=None,
            entity="orders",
            runner_definition=definition,
        )
    assert "kubernetes" in str(exc_info.value).lower()
