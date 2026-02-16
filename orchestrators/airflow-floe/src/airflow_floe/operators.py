"""Operators for Airflow-Floe connector."""

from __future__ import annotations

import shlex
import subprocess
from typing import Any

from .manifest import ManifestExecution, ManifestRunnerDefinition
from .runtime import (
    DagManifestContext,
    build_asset_event_extra,
    load_run_summary,
    parse_run_finished,
    summary_entities_by_name,
)

try:
    from airflow.models.baseoperator import BaseOperator
except Exception:  # pragma: no cover - fallback for local unit tests without Airflow
    class BaseOperator:  # type: ignore[override]
        template_fields: tuple[str, ...] = ()

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs


def _split_cmd(command: str) -> list[str]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("FLOE_CMD cannot be empty")
    return parts


class FloeRunHook:
    """Execute `floe run` and return normalized Airflow run payload."""

    def __init__(self, floe_cmd: str | None = None) -> None:
        self.floe_cmd = floe_cmd

    def build_args(
        self,
        config_path: str,
        entities: list[str] | None = None,
        *,
        execution: ManifestExecution | None = None,
    ) -> list[str]:
        if execution is None:
            cmd = self.floe_cmd or "floe"
            args = [*_split_cmd(cmd), "run", "-c", config_path, "--log-format", "json"]
            if entities:
                args.extend(["--entities", ",".join(entities)])
            return args

        command = self.floe_cmd or execution.entrypoint
        args = [*_split_cmd(command)]
        args.extend(_render_tokens(execution.base_args, config_path))
        if entities:
            args.extend(_render_entity_tokens(execution.per_entity_args, config_path, entities))
        return args

    def run(
        self,
        config_path: str,
        entities: list[str] | None = None,
        *,
        execution: ManifestExecution | None = None,
        runner_definition: ManifestRunnerDefinition | None = None,
    ) -> dict[str, Any]:
        if runner_definition is not None and runner_definition.runner_type != "local_process":
            raise ValueError(
                "unsupported runner type for Airflow FloeRunOperator: "
                f"{runner_definition.runner_type}"
            )
        if execution is not None and execution.log_format != "json":
            raise ValueError(
                "unsupported execution.log_format for Airflow FloeRunOperator: "
                f"{execution.log_format}. Expected 'json'."
            )

        completed = subprocess.run(
            self.build_args(config_path, entities=entities, execution=execution),
            check=True,
            text=True,
            capture_output=True,
        )
        run_finished = parse_run_finished(completed.stdout)
        summary_uri_field = (
            execution.result_contract.summary_uri_field
            if execution is not None
            else "summary_uri"
        )
        payload: dict[str, Any] = {
            "schema": "floe.airflow.run.v1",
            "run_id": run_finished["run_id"],
            "status": run_finished["status"],
            "exit_code": run_finished["exit_code"],
            "files": run_finished["files"],
            "rows": run_finished["rows"],
            "accepted": run_finished["accepted"],
            "rejected": run_finished["rejected"],
            "warnings": run_finished["warnings"],
            "errors": run_finished["errors"],
            "summary_uri": run_finished.get(summary_uri_field),
            "config_uri": config_path,
            "floe_log_schema": "floe.log.v1",
            "finished_at_ts_ms": run_finished["ts_ms"],
        }
        if entities and len(entities) == 1:
            payload["entity"] = entities[0]
        return payload


class FloeRunOperator(BaseOperator):
    """Run Floe CLI and push normalized run payload to XCom."""

    template_fields = ("config_path", "entities")

    def __init__(
        self,
        *,
        config_path: str,
        entities: list[str] | None = None,
        floe_cmd: str | None = None,
        manifest_context: DagManifestContext | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.entities = entities or []
        self.floe_cmd = floe_cmd
        self.manifest_context = manifest_context

    def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        hook = FloeRunHook(floe_cmd=self.floe_cmd)
        execution, runner_definition = self._resolve_execution_contract()
        payload = hook.run(
            self.config_path,
            entities=self.entities,
            execution=execution,
            runner_definition=runner_definition,
        )
        self._emit_asset_events(payload, context)
        return payload

    def _resolve_execution_contract(
        self,
    ) -> tuple[ManifestExecution | None, ManifestRunnerDefinition | None]:
        if self.manifest_context is None or self.manifest_context.manifest is None:
            return None, None

        manifest = self.manifest_context.manifest
        execution = manifest.execution
        runners = manifest.runners

        entity_names = (
            self.entities
            if self.entities
            else list(self.manifest_context.entities_by_name.keys())
        )
        requested_runner_names = set()
        for entity_name in entity_names:
            entity = self.manifest_context.entities_by_name.get(entity_name)
            if entity is None:
                continue
            requested_runner_names.add(entity.runner or runners.default)

        if not requested_runner_names:
            requested_runner_names.add(runners.default)
        if len(requested_runner_names) > 1:
            raise ValueError(
                "multiple runner names are not supported in one FloeRunOperator call: "
                f"{sorted(requested_runner_names)}"
            )

        runner_name = next(iter(requested_runner_names))
        runner_definition = runners.definitions.get(runner_name)
        if runner_definition is None:
            raise ValueError(
                f"runner '{runner_name}' not found in manifest.runners.definitions"
            )

        return execution, runner_definition

    def _emit_asset_events(
        self, payload: dict[str, Any], context: dict[str, Any] | None
    ) -> None:
        if self.manifest_context is None:
            return
        if not isinstance(context, dict):
            return
        outlet_events = context.get("outlet_events")
        if not hasattr(outlet_events, "get"):
            return

        summary = load_run_summary(payload.get("summary_uri"), self.config_path)
        summary_entities = summary_entities_by_name(summary)

        if self.entities:
            target_entities = self.entities
        else:
            target_entities = list(self.manifest_context.entities_by_name.keys())

        for entity_name in target_entities:
            manifest_entity = self.manifest_context.entities_by_name.get(entity_name)
            if manifest_entity is None:
                continue
            asset = self.manifest_context.assets_by_entity.get(entity_name)
            if asset is None:
                continue
            event = outlet_events.get(asset)
            if event is None:
                continue
            event.extra = build_asset_event_extra(
                entity=manifest_entity,
                run_finished=payload,
                summary_entity=summary_entities.get(entity_name),
            )


def _render_tokens(tokens: list[str], config_uri: str) -> list[str]:
    return [token.replace("{config_uri}", config_uri) for token in tokens]


def _render_entity_tokens(
    tokens: list[str], config_uri: str, entities: list[str]
) -> list[str]:
    joined_entities = ",".join(entities)
    return [
        token.replace("{config_uri}", config_uri).replace("{entity_name}", joined_entities)
        for token in tokens
    ]
