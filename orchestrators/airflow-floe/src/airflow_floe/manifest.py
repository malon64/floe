"""Manifest loader for Airflow-Floe integration.

This module supports two input schemas:
- floe.manifest.v1 (native manifest)
- floe.plan.v1 (legacy payload, converted on load for backward compatibility)
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
    runner: str | None

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
            runner=_optional_str(data, "runner"),
        )


@dataclass(frozen=True)
class ManifestExecutionResultContract:
    run_finished_event: bool
    summary_uri_field: str
    exit_codes: dict[str, str]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestExecutionResultContract":
        run_finished_event = data.get("run_finished_event")
        if not isinstance(run_finished_event, bool):
            raise ValueError("execution.result_contract.run_finished_event must be boolean")
        summary_uri_field = _required_str(data, "summary_uri_field")
        exit_codes = _required_string_map(data, "exit_codes")
        return ManifestExecutionResultContract(
            run_finished_event=run_finished_event,
            summary_uri_field=summary_uri_field,
            exit_codes=exit_codes,
        )


@dataclass(frozen=True)
class ManifestExecutionDefaults:
    env: dict[str, str]
    workdir: str | None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestExecutionDefaults":
        return ManifestExecutionDefaults(
            env=_required_string_map(data, "env"),
            workdir=_optional_str(data, "workdir"),
        )


@dataclass(frozen=True)
class ManifestExecution:
    entrypoint: str
    base_args: list[str]
    per_entity_args: list[str]
    log_format: str
    result_contract: ManifestExecutionResultContract
    defaults: ManifestExecutionDefaults

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestExecution":
        return ManifestExecution(
            entrypoint=_required_str(data, "entrypoint"),
            base_args=_required_string_list(data, "base_args"),
            per_entity_args=_required_string_list(data, "per_entity_args"),
            log_format=_required_str(data, "log_format"),
            result_contract=ManifestExecutionResultContract.from_dict(
                _required_object(data, "result_contract")
            ),
            defaults=ManifestExecutionDefaults.from_dict(
                _required_object(data, "defaults")
            ),
        )


@dataclass(frozen=True)
class ManifestRunnerResources:
    cpu: str | None
    memory_mb: int | None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestRunnerResources":
        memory_mb_raw = data.get("memory_mb")
        if memory_mb_raw is not None and not isinstance(memory_mb_raw, int):
            raise ValueError("runners.definitions.*.resources.memory_mb must be int or null")
        return ManifestRunnerResources(
            cpu=_optional_str(data, "cpu"),
            memory_mb=memory_mb_raw,
        )


@dataclass(frozen=True)
class ManifestRunnerDefinition:
    runner_type: str
    image: str | None
    namespace: str | None
    service_account: str | None
    resources: ManifestRunnerResources | None
    env: dict[str, str] | None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestRunnerDefinition":
        resources_obj = data.get("resources")
        resources = None
        if resources_obj is not None:
            if not isinstance(resources_obj, dict):
                raise ValueError("runners.definitions.*.resources must be object or null")
            resources = ManifestRunnerResources.from_dict(resources_obj)

        env_obj = data.get("env")
        env = None
        if env_obj is not None:
            if not isinstance(env_obj, dict) or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in env_obj.items()
            ):
                raise ValueError("runners.definitions.*.env must be map<string,string> or null")
            env = env_obj

        return ManifestRunnerDefinition(
            runner_type=_required_str(data, "type"),
            image=_optional_str(data, "image"),
            namespace=_optional_str(data, "namespace"),
            service_account=_optional_str(data, "service_account"),
            resources=resources,
            env=env,
        )


@dataclass(frozen=True)
class ManifestRunners:
    default: str
    definitions: dict[str, ManifestRunnerDefinition]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestRunners":
        default = _required_str(data, "default")
        definitions_raw = _required_object(data, "definitions")
        definitions = {
            key: ManifestRunnerDefinition.from_dict(value)
            for key, value in _required_object_map(definitions_raw, "definitions").items()
        }
        if default not in definitions:
            raise ValueError(f"runners.default '{default}' not found in runners.definitions")
        return ManifestRunners(default=default, definitions=definitions)


@dataclass(frozen=True)
class AirflowManifest:
    schema: str
    generated_at_ts_ms: int
    floe_version: str | None
    config_uri: str
    config_checksum: str | None
    execution: ManifestExecution
    runners: ManifestRunners
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
            execution=ManifestExecution.from_dict(_required_object(data, "execution")),
            runners=ManifestRunners.from_dict(_required_object(data, "runners")),
            entities=entities,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "generated_at_ts_ms": self.generated_at_ts_ms,
            "floe_version": self.floe_version,
            "config_uri": self.config_uri,
            "config_checksum": self.config_checksum,
            "execution": {
                "entrypoint": self.execution.entrypoint,
                "base_args": self.execution.base_args,
                "per_entity_args": self.execution.per_entity_args,
                "log_format": self.execution.log_format,
                "result_contract": {
                    "run_finished_event": self.execution.result_contract.run_finished_event,
                    "summary_uri_field": self.execution.result_contract.summary_uri_field,
                    "exit_codes": self.execution.result_contract.exit_codes,
                },
                "defaults": {
                    "env": self.execution.defaults.env,
                    "workdir": self.execution.defaults.workdir,
                },
            },
            "runners": {
                "default": self.runners.default,
                "definitions": {
                    name: {
                        "type": definition.runner_type,
                        "image": definition.image,
                        "namespace": definition.namespace,
                        "service_account": definition.service_account,
                        "resources": (
                            None
                            if definition.resources is None
                            else {
                                "cpu": definition.resources.cpu,
                                "memory_mb": definition.resources.memory_mb,
                            }
                        ),
                        "env": definition.env,
                    }
                    for name, definition in self.runners.definitions.items()
                },
            },
            "entities": [
                {
                    "name": entity.name,
                    "domain": entity.domain,
                    "group_name": entity.group_name,
                    "source_format": entity.source_format,
                    "accepted_sink_uri": entity.accepted_sink_uri,
                    "rejected_sink_uri": entity.rejected_sink_uri,
                    "asset_key": entity.asset_key,
                    "runner": entity.runner,
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
                runner=None,
            )
        )

    return AirflowManifest(
        schema=MANIFEST_SCHEMA,
        generated_at_ts_ms=generated_at_ts_ms,
        floe_version=floe_version,
        config_uri=config_uri,
        config_checksum=config_checksum,
        execution=default_execution_contract(),
        runners=default_runners_contract(),
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


def _required_object(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return value


def _required_object_map(data: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict):
        raise ValueError(f"{key} must be an object")
    if not all(isinstance(name, str) and isinstance(value, dict) for name, value in data.items()):
        raise ValueError(f"{key} must be a map<string, object>")
    return data


def _required_string_list(data: dict[str, Any], key: str) -> list[str]:
    value = data.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be an array of strings")
    return value


def _required_string_map(data: dict[str, Any], key: str) -> dict[str, str]:
    value = data.get(key)
    if not isinstance(value, dict) or not all(
        isinstance(item_key, str) and isinstance(item_value, str)
        for item_key, item_value in value.items()
    ):
        raise ValueError(f"{key} must be a map<string,string>")
    return value


def default_execution_contract() -> ManifestExecution:
    return ManifestExecution(
        entrypoint="floe",
        base_args=["run", "-c", "{config_uri}", "--log-format", "json", "--quiet"],
        per_entity_args=["--entities", "{entity_name}"],
        log_format="json",
        result_contract=ManifestExecutionResultContract(
            run_finished_event=True,
            summary_uri_field="summary_uri",
            exit_codes={
                "0": "success_or_rejected",
                "1": "technical_failure",
                "2": "aborted",
            },
        ),
        defaults=ManifestExecutionDefaults(env={}, workdir=None),
    )


def default_runners_contract() -> ManifestRunners:
    return ManifestRunners(
        default="local",
        definitions={
            "local": ManifestRunnerDefinition(
                runner_type="local_process",
                image=None,
                namespace=None,
                service_account=None,
                resources=None,
                env=None,
            )
        },
    )
