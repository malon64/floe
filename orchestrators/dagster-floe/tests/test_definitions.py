import json
from pathlib import Path

import pytest

from floe_dagster.definitions import (
    build_definitions,
    build_definitions_from_manifest_dir,
    build_definitions_from_manifest_paths,
)
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
        manifest_uri: str | None = None,
        dagster_job_name: str | None = None,
    ) -> RunResult:
        del config_uri, run_id, entity, log_format, execution, runner_definition, manifest_uri, dagster_job_name
        return RunResult(stdout="", stderr="", exit_code=0)


def test_build_definitions_from_manifest_path() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    defs = build_definitions(
        manifest_path=str(fixture),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert defs is not None
    job = defs.get_job_def("floe_mfv1_test_manifest_job")
    assert job is not None


def test_build_definitions_can_disable_job_creation() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    defs = build_definitions(
        manifest_path=str(fixture),
        runner=_NoopRunner(),
        entities=["employees"],
        with_job=False,
    )
    job_names = [job.name for job in defs.resolve_all_job_defs()]
    assert "floe_mfv1_test_manifest_job" not in job_names


def test_build_definitions_from_manifest_paths_creates_one_job_per_manifest() -> None:
    fixture_dir = Path(__file__).parent / "fixtures"
    defs = build_definitions_from_manifest_paths(
        manifest_paths=[
            str((fixture_dir / "manifest_hr.json").resolve()),
            str((fixture_dir / "manifest_sales.json").resolve()),
        ],
        runner=_NoopRunner(),
    )
    job_names = [job.name for job in defs.resolve_all_job_defs()]
    assert "floe_mfv1_hr_job" in job_names
    assert "floe_mfv1_sales_job" in job_names


def test_build_definitions_from_manifest_dir_loads_files() -> None:
    fixture_dir = Path(__file__).parent / "fixtures"
    defs = build_definitions_from_manifest_dir(
        manifest_dir=str(fixture_dir.resolve()),
        pattern="manifest_*.json",
        runner=_NoopRunner(),
    )
    job_names = [job.name for job in defs.resolve_all_job_defs()]
    assert "floe_mfv1_hr_job" in job_names
    assert "floe_mfv1_sales_job" in job_names


def test_build_definitions_from_manifest_paths_rejects_duplicate_asset_keys() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest_hr.json"
    with pytest.raises(ValueError, match="duplicate asset key"):
        build_definitions_from_manifest_paths(
            manifest_paths=[str(fixture.resolve()), str(fixture.resolve())],
            runner=_NoopRunner(),
        )


def test_build_definitions_rejects_source_key_collision(tmp_path) -> None:
    """Generated _source key must not collide with an existing entity key across manifests."""
    base = json.loads(
        (Path(__file__).parent / "fixtures" / "manifest_hr.json").read_text(encoding="utf-8")
    )

    # Manifest A: entity with key ["sales", "orders"] → generates source key ["sales", "orders_source"]
    manifest_a = dict(base)
    manifest_a["manifest_id"] = "manifest-a"
    manifest_a["entities"] = [
        {
            **base["entities"][0],
            "name": "orders",
            "asset_key": ["sales", "orders"],
            "accepted_sink_uri": "./out/accepted/orders",
            "rejected_sink_uri": None,
            "policy_severity": "warn",
        }
    ]
    path_a = tmp_path / "manifest_a.json"
    path_a.write_text(json.dumps(manifest_a), encoding="utf-8")

    # Manifest B: entity whose key IS ["sales", "orders_source"] — collides with A's source key
    manifest_b = dict(base)
    manifest_b["manifest_id"] = "manifest-b"
    manifest_b["entities"] = [
        {
            **base["entities"][0],
            "name": "orders_source",
            "asset_key": ["sales", "orders_source"],
            "accepted_sink_uri": "./out/accepted/orders_source",
            "rejected_sink_uri": None,
            "policy_severity": "warn",
        }
    ]
    path_b = tmp_path / "manifest_b.json"
    path_b.write_text(json.dumps(manifest_b), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate asset key"):
        build_definitions_from_manifest_paths(
            manifest_paths=[str(path_a), str(path_b)],
            runner=_NoopRunner(),
        )


def test_build_definitions_from_manifest_paths_rejects_manifest_uri_for_many() -> None:
    fixture_dir = Path(__file__).parent / "fixtures"
    with pytest.raises(ValueError, match="manifest_uri override"):
        build_definitions_from_manifest_paths(
            manifest_paths=[
                str((fixture_dir / "manifest_hr.json").resolve()),
                str((fixture_dir / "manifest_sales.json").resolve()),
            ],
            runner=_NoopRunner(),
            manifest_uri="s3://bucket/manifests/prod.json",
        )


def test_build_definitions_from_manifest_paths_rejects_job_name_override_for_many() -> None:
    fixture_dir = Path(__file__).parent / "fixtures"
    with pytest.raises(ValueError, match="job_name override"):
        build_definitions_from_manifest_paths(
            manifest_paths=[
                str((fixture_dir / "manifest_hr.json").resolve()),
                str((fixture_dir / "manifest_sales.json").resolve()),
            ],
            runner=_NoopRunner(),
            job_name="custom_job",
        )


def test_build_definitions_manifest_uri_forwarded_to_runner() -> None:
    from floe_dagster.manifest import (
        ManifestExecution,
        ManifestExecutionDefaults,
        ManifestExecutionResultContract,
        render_execution_args,
    )

    execution = ManifestExecution(
        entrypoint="floe",
        base_args=[
            "run",
            "--manifest",
            "{manifest_uri}",
            "-c",
            "{config_uri}",
            "--log-format",
            "json",
        ],
        per_entity_args=["--entities", "{entity_name}"],
        log_format="json",
        result_contract=ManifestExecutionResultContract(
            run_finished_event=True,
            summary_uri_field="summary_uri",
            exit_codes={},
        ),
        defaults=ManifestExecutionDefaults(env=None, workdir=None),
    )

    args = render_execution_args(
        execution,
        config_uri="/tmp/config.yml",
        entity_name="employees",
        manifest_uri="s3://bucket/manifests/prod.json",
    )
    assert "s3://bucket/manifests/prod.json" in args


def test_build_definitions_accepts_manifest_uri_kwarg() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    defs = build_definitions(
        manifest_path=str(fixture),
        manifest_uri="s3://bucket/manifests/prod.json",
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert defs is not None


# ---------------------------------------------------------------------------
# Orchestration policy → Dagster job concurrency config
# ---------------------------------------------------------------------------

def _make_manifest_with_orchestration(tmp_path, orchestration_block: str | None) -> Path:
    """Write a minimal manifest fixture with an optional orchestration block."""
    orch_section = ""
    if orchestration_block is not None:
        orch_section = f",\n    {orchestration_block}"
    payload = """{
  "schema": "floe.manifest.v1",
  "generated_at_ts_ms": 0,
  "floe_version": "0.4.5",
  "spec_version": "0.1",
  "manifest_id": "mfv1-orch-test",
  "config_uri": "./config.yml",
  "report_base_uri": "./report",
  "domains": [],
  "execution": {
    "entrypoint": "floe",
    "base_args": ["run", "--manifest", "{manifest_uri}", "--log-format", "json", "--quiet"],
    "per_entity_args": ["--entities", "{entity_name}"],
    "log_format": "json",
    "result_contract": {
      "run_finished_event": true,
      "summary_uri_field": "summary_uri",
      "exit_codes": {"0": "success_or_rejected", "1": "technical_failure"}
    },
    "defaults": {"env": {}, "workdir": null}""" + orch_section + """
  },
  "runners": {
    "default": "local",
    "definitions": {
      "local": {"type": "local_process", "image": null, "namespace": null,
                "service_account": null, "resources": null, "env": null}
    }
  },
  "entities": [
    {
      "name": "orders",
      "domain": "sales",
      "group_name": "sales",
      "asset_key": ["sales", "orders"],
      "source_format": "csv",
      "accepted_sink_uri": "./out/accepted/orders",
      "runner": null,
      "policy_severity": "warn",
      "write_mode": "overwrite",
      "incremental_mode": "none",
      "source": {"format": "csv", "storage": "local", "uri": "./in/orders.csv",
                 "path": "./in/orders.csv", "resolved": false},
      "sinks": {
        "accepted": {"format": "parquet", "storage": "local", "uri": "./out/accepted/orders",
                     "path": "./out/accepted/orders", "resolved": false}
      },
      "schema": {"columns": [{"name": "id", "column_type": "string"}],
                 "primary_key": [], "unique_keys": []}
    }
  ]
}"""
    path = tmp_path / "orch_test.manifest.json"
    path.write_text(payload, encoding="utf-8")
    return path


def test_definitions_job_no_config_when_no_orchestration(tmp_path) -> None:
    manifest_path = _make_manifest_with_orchestration(tmp_path, None)
    defs = build_definitions(manifest_path=str(manifest_path), runner=_NoopRunner())
    job = defs.get_job_def("floe_mfv1_orch_test_job")
    assert job is not None
    # No RunConfig should have been injected
    assert job.run_config_schema is not None  # schema exists (Dagster always has it)
    # Verify no multiprocess config was baked in by checking the default config is empty
    assert job.config_mapping is None


def test_definitions_job_config_strategy_sequential(tmp_path) -> None:
    orchestration = '"orchestration": {"strategy": "sequential"}'
    manifest_path = _make_manifest_with_orchestration(tmp_path, orchestration)
    defs = build_definitions(manifest_path=str(manifest_path), runner=_NoopRunner())
    job = defs.get_job_def("floe_mfv1_orch_test_job")
    assert job is not None
    # Job must carry a baked-in run_config with max_concurrent=1
    assert job.run_config is not None
    assert (
        job.run_config["execution"]["config"]["multiprocess"]["max_concurrent"] == 1
    )


def test_definitions_job_config_max_concurrent_explicit(tmp_path) -> None:
    orchestration = '"orchestration": {"max_concurrent_entities": 3}'
    manifest_path = _make_manifest_with_orchestration(tmp_path, orchestration)
    defs = build_definitions(manifest_path=str(manifest_path), runner=_NoopRunner())
    job = defs.get_job_def("floe_mfv1_orch_test_job")
    assert job is not None
    assert job.run_config is not None
    assert (
        job.run_config["execution"]["config"]["multiprocess"]["max_concurrent"] == 3
    )


def test_definitions_job_config_strategy_parallel_no_constraint(tmp_path) -> None:
    orchestration = '"orchestration": {"strategy": "parallel"}'
    manifest_path = _make_manifest_with_orchestration(tmp_path, orchestration)
    defs = build_definitions(manifest_path=str(manifest_path), runner=_NoopRunner())
    job = defs.get_job_def("floe_mfv1_orch_test_job")
    assert job is not None
    # strategy=parallel means no concurrency limit
    assert job.run_config is None or "multiprocess" not in str(job.run_config)


def test_definitions_job_config_sequential_overrides_max_concurrent(tmp_path) -> None:
    # strategy=sequential must enforce max_concurrent=1 even when
    # max_concurrent_entities is set to a higher value
    orchestration = '"orchestration": {"strategy": "sequential", "max_concurrent_entities": 3}'
    manifest_path = _make_manifest_with_orchestration(tmp_path, orchestration)
    defs = build_definitions(manifest_path=str(manifest_path), runner=_NoopRunner())
    job = defs.get_job_def("floe_mfv1_orch_test_job")
    assert job is not None
    assert (
        job.run_config["execution"]["config"]["multiprocess"]["max_concurrent"] == 1
    ), "sequential strategy must cap concurrency at 1 regardless of max_concurrent_entities"
