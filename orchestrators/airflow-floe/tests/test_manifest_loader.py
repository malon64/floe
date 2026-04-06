from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.manifest import (  # noqa: E402
    MANIFEST_SCHEMA,
    AirflowManifest,
    load_manifest,
)


class ManifestLoaderTests(unittest.TestCase):
    def _manifest_payload(self) -> dict:
        return {
            "schema": MANIFEST_SCHEMA,
            "floe_version": "0.2.4",
            "generated_at_ts_ms": 1739500000000,
            "config_uri": "./example/config.yml",
            "config_checksum": "abc123",
            "execution": {
                "entrypoint": "floe",
                "base_args": ["run", "-c", "{config_uri}", "--log-format", "json", "--quiet"],
                "per_entity_args": ["--entities", "{entity_name}"],
                "log_format": "json",
                "result_contract": {
                    "run_finished_event": True,
                    "summary_uri_field": "summary_uri",
                    "exit_codes": {
                        "0": "success_or_rejected",
                        "1": "technical_failure",
                        "2": "aborted",
                    },
                },
                "defaults": {"env": {}, "workdir": None},
            },
            "runners": {
                "default": "local",
                "definitions": {
                    "local": {
                        "type": "local_process",
                        "image": None,
                        "namespace": None,
                        "service_account": None,
                        "resources": None,
                        "env": None,
                        "command": None,
                        "args": None,
                        "timeout_seconds": None,
                        "ttl_seconds_after_finished": None,
                        "poll_interval_seconds": None,
                        "secrets": None,
                    }
                },
            },
            "entities": [
                {
                    "name": "employees",
                    "domain": "hr",
                    "group_name": "hr",
                    "source_format": "csv",
                    "accepted_sink_uri": "./out/accepted/employees",
                    "rejected_sink_uri": "./out/rejected/employees",
                    "asset_key": ["hr", "employees"],
                    "runner": None,
                },
                {
                    "name": "orders",
                    "domain": None,
                    "group_name": "floe",
                    "source_format": "csv",
                    "accepted_sink_uri": "./out/accepted/orders",
                    "rejected_sink_uri": None,
                    "asset_key": ["orders"],
                    "runner": None,
                },
            ],
        }

    def test_load_manifest_from_native_manifest_schema(self) -> None:
        payload = self._manifest_payload()

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "manifest.json"
            target.write_text(json.dumps(payload), encoding="utf-8")

            loaded = load_manifest(target)
            self.assertIsInstance(loaded, AirflowManifest)
            self.assertEqual(loaded.schema, MANIFEST_SCHEMA)
            self.assertEqual(loaded.config_uri, "./example/config.yml")
            self.assertEqual(len(loaded.entities), 2)
            self.assertEqual(loaded.entities[0].name, "employees")

    def test_load_manifest_rejects_legacy_validate_schema(self) -> None:
        payload = self._manifest_payload()
        payload["schema"] = "floe.plan.v1"

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "legacy-validate.json"
            target.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unsupported schema in manifest loader"):
                load_manifest(target)


if __name__ == "__main__":
    unittest.main()
