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
