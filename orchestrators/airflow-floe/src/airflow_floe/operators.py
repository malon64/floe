"""Operators for Airflow-Floe connector."""

from __future__ import annotations

import shlex
import subprocess
from typing import Any

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

    def __init__(self, floe_cmd: str = "floe") -> None:
        self.floe_cmd = floe_cmd

    def build_args(self, config_path: str, entities: list[str] | None = None) -> list[str]:
        args = [*_split_cmd(self.floe_cmd), "run", "-c", config_path, "--log-format", "json"]
        if entities:
            args.extend(["--entities", ",".join(entities)])
        return args

    def run(self, config_path: str, entities: list[str] | None = None) -> dict[str, Any]:
        completed = subprocess.run(
            self.build_args(config_path, entities=entities),
            check=True,
            text=True,
            capture_output=True,
        )
        run_finished = parse_run_finished(completed.stdout)
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
            "summary_uri": run_finished.get("summary_uri"),
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
        floe_cmd: str = "floe",
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
        payload = hook.run(self.config_path, entities=self.entities)
        self._emit_asset_events(payload, context)
        return payload

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
