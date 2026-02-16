from __future__ import annotations

from pathlib import Path

from floe_dagster.manifest import ManifestExecution
from floe_dagster.runner import LocalRunner, RunResult


def _execution(*, env: dict[str, str], workdir: str | None) -> ManifestExecution:
    return ManifestExecution.from_dict(
        {
            "entrypoint": "floe",
            "base_args": ["run", "-c", "{config_uri}", "--log-format", "json"],
            "per_entity_args": ["--entities", "{entity_name}"],
            "log_format": "json",
            "result_contract": {
                "run_finished_event": True,
                "summary_uri_field": "summary_uri",
                "exit_codes": {"0": "ok"},
            },
            "defaults": {
                "env": env,
                "workdir": workdir,
            },
        }
    )


def test_local_runner_applies_execution_defaults(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run(args, cwd=None, env_overrides=None):
        captured["args"] = args
        captured["cwd"] = cwd
        captured["env_overrides"] = env_overrides
        return RunResult(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr("floe_dagster.runner._run", fake_run)

    config_path = tmp_path / "cfg" / "config.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("version: \"0.2\"\n", encoding="utf-8")

    runner = LocalRunner("floe")
    execution = _execution(env={"FLOE_TEST_FLAG": "1"}, workdir="../work")
    runner.run_floe_entity(
        config_uri=str(config_path),
        run_id="run-123",
        entity="customers",
        execution=execution,
    )

    assert captured["env_overrides"] == {"FLOE_TEST_FLAG": "1"}
    assert captured["cwd"] == str((config_path.parent / "../work").resolve())


def test_local_runner_without_execution_keeps_default_process_context(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(args, cwd=None, env_overrides=None):
        captured["args"] = args
        captured["cwd"] = cwd
        captured["env_overrides"] = env_overrides
        return RunResult(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr("floe_dagster.runner._run", fake_run)

    runner = LocalRunner("floe")
    runner.run_floe_entity(
        config_uri="/tmp/config.yml",
        run_id=None,
        entity="customers",
    )

    assert captured["cwd"] is None
    assert captured["env_overrides"] is None
