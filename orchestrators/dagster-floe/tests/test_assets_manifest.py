from pathlib import Path
import subprocess
from unittest.mock import patch

import pytest

from dagster import AssetKey

from floe_dagster.assets import build_floe_asset_defs, load_floe_assets
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
        manifest_uri: str | None = None,
    ) -> RunResult:
        del config_uri, run_id, entity, log_format, execution, runner_definition, manifest_uri
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
        command=None,
        args=None,
        timeout_seconds=None,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=None,
        secrets=None,
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


def test_local_runner_raises_not_implemented_for_unknown_runner() -> None:
    """LocalRunner raises NotImplementedError for non-local_process runner types."""
    runner = LocalRunner(floe_bin="floe")
    definition = ManifestRunnerDefinition(
        runner_type="docker",
        image=None,
        namespace=None,
        service_account=None,
        env=None,
        command=None,
        args=None,
        timeout_seconds=None,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=None,
        secrets=None,
    )
    with pytest.raises(NotImplementedError) as exc_info:
        runner.run_floe_entity(
            config_uri="local:///tmp/config.yml",
            run_id=None,
            entity="orders",
            runner_definition=definition,
        )
    assert "docker" in str(exc_info.value).lower()


# ── multi-asset structure ──────────────────────────────────────────────────────

def _get_asset_def(manifest_path, entity_name):
    asset_defs, entities = build_floe_asset_defs(
        manifest_path=str(manifest_path),
        runner=_NoopRunner(),
        entities=[entity_name],
    )
    return asset_defs[0], entities[0]


def test_multi_asset_accepted_key_matches_entity_asset_key() -> None:
    """Accepted output key == entity.asset_key (backward compatibility)."""
    asset_def, entity = _get_asset_def(FIXTURE, "employees")
    assert AssetKey(entity.asset_key) in asset_def.keys


def test_multi_asset_rejected_key_present_when_policy_is_reject() -> None:
    """employees has policy_severity='reject' → rejected output exposed."""
    asset_def, entity = _get_asset_def(FIXTURE, "employees")
    rejected_key = AssetKey(entity.asset_key[:-1] + [entity.asset_key[-1] + "_rejected"])
    assert rejected_key in asset_def.keys


def test_multi_asset_no_rejected_key_when_policy_is_warn() -> None:
    """orders has policy_severity='warn' → no rejected output (rows discarded silently)."""
    asset_def, entity = _get_asset_def(FIXTURE, "orders")
    rejected_key = AssetKey(entity.asset_key[:-1] + [entity.asset_key[-1] + "_rejected"])
    assert rejected_key not in asset_def.keys


def test_multi_asset_rejected_key_present_when_policy_is_abort(tmp_path) -> None:
    """policy_severity='abort' → rejected output exposed (same as reject)."""
    import json

    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    # Patch employees to use abort severity
    payload["entities"][0]["policy_severity"] = "abort"
    manifest_path = tmp_path / "manifest.abort.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    asset_def, entity = _get_asset_def(manifest_path, "employees")
    rejected_key = AssetKey(entity.asset_key[:-1] + [entity.asset_key[-1] + "_rejected"])
    assert rejected_key in asset_def.keys


def test_multi_asset_source_dep_declared() -> None:
    """The _source dep key is declared as an upstream dependency."""
    asset_def, entity = _get_asset_def(FIXTURE, "employees")
    source_key = AssetKey(entity.asset_key[:-1] + [entity.asset_key[-1] + "_source"])
    dep_keys = set(asset_def.keys_by_input_name.values())
    assert source_key in dep_keys
