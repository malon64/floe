"""Manifest loader for Airflow-Floe integration.

This module supports two input schemas:
- floe.manifest.v1 (native manifest)
- floe.plan.v1 (output of `floe validate --output json`, converted on load)
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

MANIFEST_SCHEMA = "floe.manifest.v1"
VALIDATE_SCHEMA = "floe.plan.v1"


@dataclass(frozen=True)
class ManifestEntity:
    name: str
    domain: str | None
    group_name: str
    source_format: str
    accepted_sink_uri: str
    rejected_sink_uri: str | None
    asset_key: list[str]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestEntity":
        name = _required_str(data, "name")
        group_name = _required_str(data, "group_name")
        source_format = _required_str(data, "source_format")
        accepted_sink_uri = _required_str(data, "accepted_sink_uri")
        rejected_sink_uri = _optional_str(data, "rejected_sink_uri")
        domain = _optional_str(data, "domain")
        asset_key = data.get("asset_key")
        if not isinstance(asset_key, list) or not asset_key or not all(
            isinstance(part, str) and part for part in asset_key
        ):
            raise ValueError("manifest entity asset_key must be a non-empty string list")

        return ManifestEntity(
            name=name,
            domain=domain,
            group_name=group_name,
            source_format=source_format,
            accepted_sink_uri=accepted_sink_uri,
            rejected_sink_uri=rejected_sink_uri,
            asset_key=asset_key,
        )


@dataclass(frozen=True)
class AirflowManifest:
    schema: str
    generated_at_ts_ms: int
    floe_version: str | None
    config_uri: str
    config_checksum: str | None
    entities: list[ManifestEntity]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "AirflowManifest":
        schema = _required_str(data, "schema")
        if schema != MANIFEST_SCHEMA:
            raise ValueError(f"unexpected manifest schema: {schema}")

        ts = data.get("generated_at_ts_ms")
        if not isinstance(ts, int) or ts < 0:
            raise ValueError("generated_at_ts_ms must be a non-negative integer")

        entities_raw = data.get("entities")
        if not isinstance(entities_raw, list):
            raise ValueError("entities must be a list")

        entities = [ManifestEntity.from_dict(item) for item in entities_raw]

        return AirflowManifest(
            schema=schema,
            generated_at_ts_ms=ts,
            floe_version=_optional_str(data, "floe_version"),
            config_uri=_required_str(data, "config_uri"),
            config_checksum=_optional_str(data, "config_checksum"),
            entities=entities,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "generated_at_ts_ms": self.generated_at_ts_ms,
            "floe_version": self.floe_version,
            "config_uri": self.config_uri,
            "config_checksum": self.config_checksum,
            "entities": [
                {
                    "name": entity.name,
                    "domain": entity.domain,
                    "group_name": entity.group_name,
                    "source_format": entity.source_format,
                    "accepted_sink_uri": entity.accepted_sink_uri,
                    "rejected_sink_uri": entity.rejected_sink_uri,
                    "asset_key": entity.asset_key,
                }
                for entity in self.entities
            ],
        }


def load_manifest(path: str | Path) -> AirflowManifest:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest file must contain a JSON object")

    schema = payload.get("schema")
    if schema == MANIFEST_SCHEMA:
        return AirflowManifest.from_dict(payload)

    if schema == VALIDATE_SCHEMA:
        return build_manifest_from_validate_payload(payload)

    raise ValueError(f"unsupported schema in manifest loader: {schema}")


def build_manifest_from_validate_payload(
    payload: dict[str, Any],
    *,
    selected_entities: list[str] | None = None,
) -> AirflowManifest:
    schema = payload.get("schema")
    if schema != VALIDATE_SCHEMA:
        raise ValueError(f"validate schema mismatch: expected {VALIDATE_SCHEMA}, got {schema}")

    if payload.get("valid") is not True:
        raise ValueError("cannot build manifest from invalid validate payload")

    config_obj = payload.get("config")
    if not isinstance(config_obj, dict):
        raise ValueError("validate payload missing config object")

    config_uri = _required_str(config_obj, "uri")
    config_checksum = _optional_str(config_obj, "checksum")
    floe_version = _optional_str(payload, "floe_version")

    generated_at_ts_ms = payload.get("generated_at_ts_ms")
    if not isinstance(generated_at_ts_ms, int) or generated_at_ts_ms < 0:
        raise ValueError("validate payload missing valid generated_at_ts_ms")

    plan = payload.get("plan")
    if not isinstance(plan, dict):
        raise ValueError("validate payload missing plan object")

    entities_raw = plan.get("entities")
    if not isinstance(entities_raw, list):
        raise ValueError("validate payload plan.entities must be a list")

    selected = set(selected_entities or [])
    use_filter = bool(selected_entities)

    entities: list[ManifestEntity] = []
    for raw in entities_raw:
        if not isinstance(raw, dict):
            raise ValueError("validate payload entity must be an object")

        name = _required_str(raw, "name")
        if use_filter and name not in selected:
            continue

        domain = _optional_str(raw, "domain")
        group_name = _optional_str(raw, "group_name") or domain or "floe"

        source_obj = raw.get("source")
        if not isinstance(source_obj, dict):
            raise ValueError(f"entity {name} source must be an object")
        source_format = _required_str(source_obj, "format")

        sinks_obj = raw.get("sinks")
        if not isinstance(sinks_obj, dict):
            raise ValueError(f"entity {name} sinks must be an object")

        accepted_obj = sinks_obj.get("accepted")
        if not isinstance(accepted_obj, dict):
            raise ValueError(f"entity {name} sinks.accepted must be an object")
        accepted_sink_uri = _required_str(accepted_obj, "uri")

        rejected_sink_uri: str | None = None
        rejected_obj = sinks_obj.get("rejected")
        if isinstance(rejected_obj, dict):
            rejected_sink_uri = _required_str(rejected_obj, "uri")

        asset_key = [domain, name] if domain else [name]

        entities.append(
            ManifestEntity(
                name=name,
                domain=domain,
                group_name=group_name,
                source_format=source_format,
                accepted_sink_uri=accepted_sink_uri,
                rejected_sink_uri=rejected_sink_uri,
                asset_key=asset_key,
            )
        )

    return AirflowManifest(
        schema=MANIFEST_SCHEMA,
        generated_at_ts_ms=generated_at_ts_ms,
        floe_version=floe_version,
        config_uri=config_uri,
        config_checksum=config_checksum,
        entities=entities,
    )


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _optional_str(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string when provided")
    return value
