from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


def _install_airflow_stub() -> None:
    if "airflow.sdk" in sys.modules:
        return

    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")

    class Asset:  # pragma: no cover - simple import stub
        def __init__(self, name: str, uri: str, group: str | None = None) -> None:
            self.name = name
            self.uri = uri
            self.group = group

    sdk_module.Asset = Asset
    airflow_module.sdk = sdk_module
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = sdk_module


_install_airflow_stub()
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.hooks import FloeManifestHook  # noqa: E402
from airflow_floe.operators import FloeRunHook, FloeRunOperator  # noqa: E402


class HookAndOperatorTests(unittest.TestCase):
    def _write_manifest(self, base: Path, config_path: Path) -> Path:
        manifest_path = base / "manifest.airflow.json"
        payload = {
            "schema": "floe.airflow.manifest.v1",
            "generated_at_ts_ms": 1739500000000,
            "floe_version": "0.2.4",
            "config_uri": str(config_path),
            "config_checksum": None,
            "entities": [
                {
                    "name": "orders",
                    "domain": "sales",
                    "group_name": "sales",
                    "source_format": "csv",
                    "accepted_sink_uri": f"local://{base}/out/accepted/orders",
                    "rejected_sink_uri": None,
                    "asset_key": ["sales", "orders"],
                }
            ],
        }
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")
        return manifest_path

    def test_manifest_hook_loads_context_and_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            manifest_path = self._write_manifest(base, config_path)

            hook = FloeManifestHook(str(manifest_path))
            context = hook.get_context()
            self.assertEqual(context.entity_names, ["orders"])
            self.assertEqual(hook.get_config_path(), str(config_path))
            self.assertEqual(len(hook.get_assets()), 1)

    def test_manifest_hook_fallback_without_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            missing_manifest = base / "missing.json"

            hook = FloeManifestHook(
                str(missing_manifest),
                default_config_path=str(config_path),
            )
            context = hook.get_context()
            self.assertIsNone(context.manifest)
            self.assertEqual(context.entity_names, [])
            self.assertEqual(hook.get_assets(), [])

    def test_run_hook_build_args_with_entities(self) -> None:
        hook = FloeRunHook(floe_cmd="floe")
        args = hook.build_args("/tmp/config.yml", entities=["orders", "customer"])
        self.assertIn("--entities", args)
        self.assertIn("orders,customer", args)

    def test_run_hook_run_returns_normalized_payload(self) -> None:
        hook = FloeRunHook(floe_cmd="floe")
        stdout = json.dumps(
            {
                "schema": "floe.log.v1",
                "event": "run_finished",
                "run_id": "run-1",
                "status": "success",
                "exit_code": 0,
                "files": 1,
                "rows": 10,
                "accepted": 9,
                "rejected": 1,
                "warnings": 0,
                "errors": 0,
                "summary_uri": "local:///tmp/report/run.summary.json",
                "ts_ms": 1739500000000,
            }
        )
        with patch("subprocess.run") as run_mock:
            run_mock.return_value = subprocess.CompletedProcess(
                args=["floe"],
                returncode=0,
                stdout=stdout,
                stderr="",
            )
            payload = hook.run("/tmp/config.yml", entities=["orders"])

        self.assertEqual(payload["schema"], "floe.airflow.run.v1")
        self.assertEqual(payload["run_id"], "run-1")
        self.assertEqual(payload["entity"], "orders")
        self.assertEqual(payload["config_uri"], "/tmp/config.yml")

    def test_run_operator_execute_uses_hook(self) -> None:
        operator = FloeRunOperator(
            task_id="run_floe",
            config_path="/tmp/config.yml",
            entities=["orders"],
            floe_cmd="floe",
        )
        expected = {"schema": "floe.airflow.run.v1", "run_id": "run-1"}
        with patch("airflow_floe.operators.FloeRunHook.run", return_value=expected) as run_mock:
            actual = operator.execute({})
        run_mock.assert_called_once_with("/tmp/config.yml", entities=["orders"])
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
