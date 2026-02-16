from pathlib import Path

from floe_dagster.definitions import build_definitions
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


def test_build_definitions_from_manifest_path() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    defs = build_definitions(
        manifest_path=str(fixture),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert defs is not None
