from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.manifest import (  # noqa: E402
    MANIFEST_SCHEMA,
    VALIDATE_SCHEMA,
    AirflowManifest,
    build_manifest_from_validate_payload,
    load_manifest,
)


class ManifestLoaderTests(unittest.TestCase):
    def _validate_payload(self) -> dict:
        return {
            "schema": VALIDATE_SCHEMA,
            "floe_version": "0.2.4",
            "generated_at_ts_ms": 1739500000000,
            "config": {
                "uri": "./example/config.yml",
                "checksum": "abc123",
            },
            "valid": True,
            "errors": [],
            "warnings": [],
            "plan": {
                "entities": [
                    {
                        "name": "employees",
                        "domain": "hr",
                        "group_name": "hr",
                        "source": {"format": "csv"},
                        "sinks": {
                            "accepted": {"uri": "./out/accepted/employees"},
                            "rejected": {"uri": "./out/rejected/employees"},
                        },
                    },
                    {
                        "name": "orders",
                        "source": {"format": "csv"},
                        "sinks": {
                            "accepted": {"uri": "./out/accepted/orders"},
                        },
                    },
                ]
            },
        }

    def test_build_manifest_from_validate_payload(self) -> None:
        payload = self._validate_payload()
        manifest = build_manifest_from_validate_payload(payload)

        self.assertEqual(manifest.schema, MANIFEST_SCHEMA)
        self.assertEqual(manifest.config_uri, "./example/config.yml")
        self.assertEqual(manifest.floe_version, "0.2.4")
        self.assertEqual(len(manifest.entities), 2)
        self.assertEqual(manifest.execution.entrypoint, "floe")
        self.assertEqual(manifest.runners.default, "local")
        self.assertIn("local", manifest.runners.definitions)

        employees = manifest.entities[0]
        self.assertEqual(employees.name, "employees")
        self.assertEqual(employees.asset_key, ["hr", "employees"])
        self.assertEqual(employees.rejected_sink_uri, "./out/rejected/employees")
        self.assertIsNone(employees.runner)

        orders = manifest.entities[1]
        self.assertEqual(orders.group_name, "floe")
        self.assertEqual(orders.asset_key, ["orders"])

    def test_build_manifest_with_entity_filter(self) -> None:
        payload = self._validate_payload()
        manifest = build_manifest_from_validate_payload(
            payload,
            selected_entities=["orders"],
        )
        self.assertEqual([entity.name for entity in manifest.entities], ["orders"])

    def test_load_manifest_from_native_manifest_schema(self) -> None:
        payload = self._validate_payload()
        manifest = build_manifest_from_validate_payload(payload)

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "manifest.json"
            target.write_text(json.dumps(manifest.to_dict()), encoding="utf-8")

            loaded = load_manifest(target)
            self.assertIsInstance(loaded, AirflowManifest)
            self.assertEqual(loaded.schema, MANIFEST_SCHEMA)
            self.assertEqual(len(loaded.entities), 2)

    def test_load_manifest_from_validate_schema(self) -> None:
        payload = self._validate_payload()

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "validate.json"
            target.write_text(json.dumps(payload), encoding="utf-8")

            loaded = load_manifest(target)
            self.assertEqual(loaded.schema, MANIFEST_SCHEMA)
            self.assertEqual(loaded.entities[0].name, "employees")

    def test_invalid_validate_payload_rejected(self) -> None:
        payload = self._validate_payload()
        payload["valid"] = False

        with self.assertRaisesRegex(
            ValueError,
            "cannot build manifest from invalid validate payload",
        ):
            build_manifest_from_validate_payload(payload)

    def test_env_matrix_manifest_preserves_runner_and_paths(self) -> None:
        """Integration-style matrix: dev(local) + uat/prod(k8) with env-specific paths."""

        def native_manifest(*, env: str, runner_type: str, source_uri: str, accepted_uri: str) -> dict:
            runner_name = "local" if runner_type == "local_process" else "k8s"
            return {
                "schema": MANIFEST_SCHEMA,
                "generated_at_ts_ms": 1739500000000,
                "floe_version": "0.3.0",
                "config_uri": "./config.yml",
                "config_checksum": None,
                "execution": {
                    "entrypoint": "floe",
                    "base_args": ["run", "-c", "{config_uri}", "--log-format", "json"],
                    "per_entity_args": ["--entities", "{entity_name}"],
                    "log_format": "json",
                    "result_contract": {
                        "run_finished_event": True,
                        "summary_uri_field": "summary_uri",
                        "exit_codes": {"0": "success_or_rejected", "1": "technical_failure"},
                    },
                    "defaults": {"env": {"ENV": env}, "workdir": None},
                },
                "runners": {
                    "default": runner_name,
                    "definitions": {
                        runner_name: {
                            "type": runner_type,
                            "image": "ghcr.io/malon64/floe:latest" if runner_type == "kubernetes_job" else None,
                            "namespace": "floe" if runner_type == "kubernetes_job" else None,
                            "service_account": "floe" if runner_type == "kubernetes_job" else None,
                            "resources": None,
                            "env": None,
                            "command": None,
                            "args": None,
                            "timeout_seconds": 600 if runner_type == "kubernetes_job" else None,
                            "ttl_seconds_after_finished": 120 if runner_type == "kubernetes_job" else None,
                            "poll_interval_seconds": 5 if runner_type == "kubernetes_job" else None,
                            "secrets": None,
                        }
                    },
                },
                "entities": [
                    {
                        "name": "orders",
                        "domain": "sales",
                        "group_name": "sales",
                        "source_format": "csv",
                        "accepted_sink_uri": accepted_uri,
                        "rejected_sink_uri": None,
                        "asset_key": ["sales", "orders"],
                        "runner": None,
                    }
                ],
            }

        matrix = [
            ("dev", "local_process", "local:///workspace/dev/in/orders.csv", "local:///workspace/dev/out/accepted/orders"),
            ("uat", "kubernetes_job", "s3://bucket-uat/in/orders.csv", "s3://bucket-uat/out/accepted/orders"),
            ("prod", "kubernetes_job", "s3://bucket-prod/in/orders.csv", "s3://bucket-prod/out/accepted/orders"),
        ]

        for env, runner_type, source_uri, accepted_uri in matrix:
            payload = native_manifest(
                env=env,
                runner_type=runner_type,
                source_uri=source_uri,
                accepted_uri=accepted_uri,
            )
            # keep source_uri tracked through env defaults (for matrix readability) and entity sink URI contract
            payload["execution"]["defaults"]["env"]["SOURCE_URI"] = source_uri

            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / f"manifest.{env}.json"
                target.write_text(json.dumps(payload), encoding="utf-8")
                loaded = load_manifest(target)

            runner_name = loaded.runners.default
            definition = loaded.runners.definitions[runner_name]
            self.assertEqual(definition.runner_type, runner_type)
            self.assertEqual(loaded.execution.defaults.env["ENV"], env)
            self.assertEqual(loaded.execution.defaults.env["SOURCE_URI"], source_uri)
            self.assertEqual(loaded.entities[0].accepted_sink_uri, accepted_uri)


if __name__ == "__main__":
    unittest.main()
