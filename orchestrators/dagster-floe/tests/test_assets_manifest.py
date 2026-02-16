from pathlib import Path

import pytest

from floe_dagster.assets import load_floe_assets
from floe_dagster.runner import RunResult, Runner


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


def test_load_assets_accepts_local_process_entities() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    defs = load_floe_assets(
        manifest_path=str(fixture),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert defs is not None


def test_load_assets_rejects_unsupported_runner_type() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    with pytest.raises(ValueError, match="unsupported runner type"):
        load_floe_assets(
            manifest_path=str(fixture),
            runner=_NoopRunner(),
            entities=["orders"],
        )
