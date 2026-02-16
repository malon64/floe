from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from dagster import Definitions, Failure, MaterializeResult, asset

from .events import last_run_finished, parse_ndjson_events, parse_run_finished
from .manifest import (
    DagsterManifest,
    ManifestExecution,
    ManifestEntity,
    ManifestRunnerDefinition,
    load_manifest,
    resolve_config_uri,
    resolve_entity_runner,
)
from .runner import Runner


def load_floe_assets(
    manifest_path: str,
    runner: Runner,
    entities: list[str] | None = None,
) -> Definitions:
    assets_defs, _selected_entities = build_floe_asset_defs(
        manifest_path=manifest_path,
        runner=runner,
        entities=entities,
    )
    return Definitions(assets=assets_defs)


def build_floe_asset_defs(
    manifest_path: str,
    runner: Runner,
    entities: list[str] | None = None,
) -> tuple[list[Any], list[ManifestEntity]]:
    manifest = load_manifest(manifest_path)
    config_uri = resolve_config_uri(manifest_path, manifest.config_uri)
    entity_items = selected_manifest_entities(manifest, entities)

    assets_defs = []
    for entity in entity_items:
        runner_definition = resolve_entity_runner(manifest, entity)
        if runner_definition.runner_type != "local_process":
            raise ValueError(
                f"unsupported runner type for dagster-floe: {runner_definition.runner_type}"
            )

        assets_defs.append(
            _make_entity_asset(
                key_prefix=list(entity.asset_key[:-1]),
                asset_name=entity.asset_key[-1],
                group_name=entity.group_name,
                entity_name=entity.name,
                config_uri=config_uri,
                runner=runner,
                execution=manifest.execution,
                runner_definition=runner_definition,
            )
        )

    return assets_defs, entity_items


def selected_manifest_entities(
    manifest: DagsterManifest, entities: list[str] | None = None
) -> list[ManifestEntity]:
    if not entities:
        return list(manifest.entities)
    selected = set(entities)
    return [item for item in manifest.entities if item.name in selected]


def _make_entity_asset(
    *,
    key_prefix: list[str],
    asset_name: str,
    group_name: str,
    entity_name: str,
    config_uri: str,
    runner: Runner,
    execution: ManifestExecution,
    runner_definition: ManifestRunnerDefinition,
):
    @asset(name=asset_name, key_prefix=key_prefix, group_name=group_name)
    def _asset(context) -> MaterializeResult:
        run_id = getattr(context, "run_id", None)
        result = runner.run_floe_entity(
            config_uri=config_uri,
            run_id=run_id,
            entity=entity_name,
            log_format=execution.log_format,
            execution=execution,
            runner_definition=runner_definition,
        )

        if result.stderr.strip():
            for line in result.stderr.splitlines():
                context.log.info(line)

        events = parse_ndjson_events(result.stdout)
        finished_event = last_run_finished(events)
        finished = parse_run_finished(
            finished_event,
            summary_uri_field=execution.result_contract.summary_uri_field,
        )

        summary_uri = finished.summary_uri
        entity_stats: dict[str, Any] = {}

        if summary_uri:
            try:
                summary_json = _load_summary_json(summary_uri, config_uri)
                entity_stats = _extract_entity_stats(summary_json, entity_name)
            except Exception as exc:  # noqa: BLE001
                context.log.warning(f"failed to load run summary: {exc}")

        metadata: dict[str, Any] = {
            "run_id": finished.run_id,
            "status": finished.status,
            "exit_code": finished.exit_code,
        }
        if summary_uri:
            metadata["summary_uri"] = summary_uri

        metadata.update(entity_stats)

        if result.exit_code != 0 or finished.exit_code != 0:
            raise Failure(
                description=f"floe run failed for entity {entity_name}",
                metadata=metadata,
            )

        return MaterializeResult(metadata=metadata)

    return _asset


def _load_summary_json(summary_uri: str, config_uri: str) -> dict[str, Any]:
    if summary_uri.startswith("local://"):
        raw_path = unquote(summary_uri[len("local://") :])
        path = Path(raw_path)
    elif summary_uri.startswith("file://"):
        parsed = urlparse(summary_uri)
        path = Path(unquote(parsed.path))
    elif "://" in summary_uri:
        raise ValueError(f"unsupported non-local summary URI: {summary_uri}")
    else:
        path = Path(summary_uri)

    if not path.is_absolute() and "://" not in config_uri:
        path = (Path(config_uri).resolve().parent / path).resolve()

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("run.summary.json must be an object")
    return data


def _extract_entity_stats(summary_json: dict[str, Any], entity_name: str) -> dict[str, Any]:
    entities = summary_json.get("entities") or []
    if not isinstance(entities, list):
        return {}
    for item in entities:
        if not isinstance(item, dict):
            continue
        if item.get("name") != entity_name:
            continue
        results = item.get("results") or {}
        if not isinstance(results, dict):
            results = {}
        return {
            "files": results.get("files_total"),
            "rows": results.get("rows_total"),
            "accepted": results.get("accepted_total"),
            "rejected": results.get("rejected_total"),
            "warnings": results.get("warnings_total"),
            "errors": results.get("errors_total"),
            "entity_status": item.get("status"),
            "entity_report_file": item.get("report_file"),
        }
    return {}
