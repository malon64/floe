from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

# airflow-floe is not packaged yet; import module directly from DAG folder.
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from floe_manifest import (  # noqa: E402
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

        employees = manifest.entities[0]
        self.assertEqual(employees.name, "employees")
        self.assertEqual(employees.asset_key, ["hr", "employees"])
        self.assertEqual(employees.rejected_sink_uri, "./out/rejected/employees")

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


if __name__ == "__main__":
    unittest.main()
