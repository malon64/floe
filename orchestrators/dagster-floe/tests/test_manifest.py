import json
from pathlib import Path

import pytest

from floe_dagster.manifest import (
    DagsterManifest,
    load_manifest,
    render_execution_args,
    resolve_config_uri,
    resolve_entity_runner,
)


def test_manifest_asset_keys_and_groups():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)

    assert [e.asset_key for e in manifest.entities] == [["hr", "employees"], ["sales", "orders"]]
    assert [e.group_name for e in manifest.entities] == ["hr", "sales"]


def test_render_execution_args_replaces_placeholders():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)
    args = render_execution_args(
        manifest.execution,
        config_uri="/tmp/config.yml",
        entity_name="employees",
        run_id="run-42",
    )
    assert args == [
        "run",
        "-c",
        "/tmp/config.yml",
        "--log-format",
        "json",
        "--run-id",
        "run-42",
        "--entities",
        "employees",
    ]


def test_resolve_config_uri_with_relative_path():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    data = json.loads(fixture.read_text(encoding="utf-8"))
    manifest = load_manifest(fixture)
    resolved = resolve_config_uri(str(fixture), manifest.config_uri)
    assert resolved == str((fixture.parent / data["config_uri"]).resolve())


def test_resolve_entity_runner_prefers_entity_override():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)

    employees_runner = resolve_entity_runner(manifest, manifest.entities[0])
    orders_runner = resolve_entity_runner(manifest, manifest.entities[1])

    assert employees_runner.runner_type == "local_process"
    assert orders_runner.runner_type == "kubernetes_job"


def test_manifest_rejects_non_json_log_format():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["execution"]["log_format"] = "text"

    with pytest.raises(ValueError, match="execution.log_format"):
        DagsterManifest.from_dict(payload)


def test_manifest_schema_rejects_unknown_top_level_key(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["unexpected_top_level"] = True
    manifest_path = tmp_path / "manifest.invalid.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest schema validation failed"):
        load_manifest(manifest_path)


def test_manifest_schema_rejects_missing_entity_source(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    del payload["entities"][0]["source"]
    manifest_path = tmp_path / "manifest.invalid.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest schema validation failed"):
        load_manifest(manifest_path)


def test_env_matrix_manifest_runner_and_paths() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    base = json.loads(fixture.read_text(encoding="utf-8"))

    matrix = [
        (
            "dev",
            "local",
            "local_process",
            "local:///workspace/dev/in/orders.csv",
            "local:///workspace/dev/out/accepted/orders",
        ),
        (
            "uat",
            "k8s",
            "kubernetes_job",
            "s3://bucket-uat/in/orders.csv",
            "s3://bucket-uat/out/accepted/orders",
        ),
        (
            "prod",
            "k8s",
            "kubernetes_job",
            "s3://bucket-prod/in/orders.csv",
            "s3://bucket-prod/out/accepted/orders",
        ),
    ]

    for env, runner_name, runner_type, source_uri, accepted_uri in matrix:
        payload = json.loads(json.dumps(base))
        payload["runners"]["default"] = runner_name
        payload["runners"]["definitions"] = {
            runner_name: {
                "type": runner_type,
                "image": "ghcr.io/malon64/floe:latest" if runner_type == "kubernetes_job" else None,
                "namespace": "floe" if runner_type == "kubernetes_job" else None,
                "service_account": "floe" if runner_type == "kubernetes_job" else None,
                "resources": None,
                "env": {"ENV": env},
                "command": None,
                "args": None,
                "timeout_seconds": 600 if runner_type == "kubernetes_job" else None,
                "ttl_seconds_after_finished": 120 if runner_type == "kubernetes_job" else None,
                "poll_interval_seconds": 5 if runner_type == "kubernetes_job" else None,
                "secrets": None,
            }
        }
        payload["entities"][1]["source"]["uri"] = source_uri
        payload["entities"][1]["accepted_sink_uri"] = accepted_uri
        payload["entities"][1]["runner"] = None

        manifest = DagsterManifest.from_dict(payload)
        orders = next(e for e in manifest.entities if e.name == "orders")
        runner = resolve_entity_runner(manifest, orders)

        assert runner.runner_type == runner_type
        assert runner.env is not None and runner.env["ENV"] == env
        assert orders.accepted_sink_uri == accepted_uri


def test_manifest_schema_accepts_extended_k8_runner_fields(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["runners"]["definitions"]["k8s"].update(
        {
            "command": ["floe"],
            "args": ["run", "-c", "{config_uri}", "--entities", "{entity_name}"],
            "timeout_seconds": 120,
            "ttl_seconds_after_finished": 60,
            "poll_interval_seconds": 5,
            "secrets": [{"name": "API_TOKEN", "secret_name": "floe-secrets", "key": "token"}],
        }
    )
    manifest_path = tmp_path / "manifest.k8.extended.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    manifest = load_manifest(manifest_path)
    k8s_runner = manifest.runners.definitions["k8s"]
    assert k8s_runner.command == ["floe"]
    assert k8s_runner.poll_interval_seconds == 5
    assert k8s_runner.secrets is not None and k8s_runner.secrets[0]["name"] == "API_TOKEN"
