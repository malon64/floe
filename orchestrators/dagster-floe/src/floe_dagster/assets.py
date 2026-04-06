from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from dagster import Definitions, Failure, MaterializeResult, asset

from .k8s_status import STATUS_SUCCEEDED

from .asset_checks import build_asset_check_results, build_asset_check_specs
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
    asset_key = [*key_prefix, asset_name]

    @asset(
        name=asset_name,
        key_prefix=key_prefix,
        group_name=group_name,
        check_specs=build_asset_check_specs(asset_key),
    )
    def _asset(context):
        run = getattr(context, "run", None)
        run_id = getattr(run, "run_id", None) if run is not None else None
        if run_id is None:
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

        try:
            events = parse_ndjson_events(result.stdout)
            finished_event = last_run_finished(events)
            finished = parse_run_finished(
                finished_event,
                summary_uri_field=execution.result_contract.summary_uri_field,
            )
        except ValueError:
            synthetic_status = result.status or ("success" if result.exit_code == 0 else "failed")
            synthetic_exit_code = result.exit_code
            from .events import FloeRunFinished

            finished = FloeRunFinished(
                run_id=(result.backend_metadata or {}).get("backend_run_id") or (run_id or "unknown"),
                status=synthetic_status,
                exit_code=synthetic_exit_code,
                summary_uri=None,
            )

        summary_uri = finished.summary_uri
        entity_stats: dict[str, Any] = {}
        entity_report_json: dict[str, Any] | None = None
        entity_report_uri: str | None = None

        if summary_uri:
            try:
                summary_json = _load_summary_json(summary_uri, config_uri)
                entity_stats = _extract_entity_stats(summary_json, entity_name)
                entity_report_uri = _as_optional_str(entity_stats.get("entity_report_file"))
                if entity_report_uri:
                    entity_report_json = _load_entity_report_json(entity_report_uri, config_uri)
            except Exception as exc:  # noqa: BLE001
                context.log.warning(f"failed to load run summary/entity report: {exc}")

        metadata: dict[str, Any] = {
            "run_id": finished.run_id,
            "status": finished.status,
            "exit_code": finished.exit_code,
        }
        if summary_uri:
            metadata["summary_uri"] = summary_uri

        metadata.update(entity_stats)
        if entity_report_uri:
            metadata["entity_report_uri"] = entity_report_uri
        if result.failure_reason:
            metadata["failure_reason"] = result.failure_reason
        if result.status:
            metadata["backend_status"] = result.status
        if result.backend_metadata:
            metadata["backend_metadata"] = result.backend_metadata

        check_results = build_asset_check_results(
            asset_key=asset_key,
            entity_name=entity_name,
            finished=finished,
            entity_report=entity_report_json,
            entity_stats=entity_stats,
            summary_uri=summary_uri,
            entity_report_uri=entity_report_uri,
        )
        for check_result in check_results:
            yield check_result

        if (
            result.exit_code != 0
            or finished.exit_code != 0
            or ((result.status is not None) and (result.status != STATUS_SUCCEEDED))
        ):
            raise Failure(
                description=f"floe run failed for entity {entity_name}",
                metadata=metadata,
            )

        yield MaterializeResult(metadata=metadata)

    return _asset


def _load_summary_json(summary_uri: str, config_uri: str) -> dict[str, Any]:
    return _load_local_json_document(summary_uri, config_uri, label="summary")


def _load_entity_report_json(entity_report_uri: str, config_uri: str) -> dict[str, Any]:
    return _load_local_json_document(entity_report_uri, config_uri, label="entity report")


def _load_local_json_document(ref: str, config_uri: str, *, label: str) -> dict[str, Any]:
    path = _resolve_local_json_path(ref, config_uri)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{label} JSON must be an object")
    return data


def _resolve_local_json_path(ref: str, config_uri: str) -> Path:
    if ref.startswith("local://"):
        raw_path = unquote(ref[len("local://") :])
        path = Path(raw_path)
    elif ref.startswith("file://"):
        parsed = urlparse(ref)
        path = Path(unquote(parsed.path))
    elif "://" in ref:
        raise ValueError(f"unsupported non-local JSON URI: {ref}")
    else:
        path = Path(ref)

    if not path.is_absolute() and "://" not in config_uri:
        path = (Path(config_uri).resolve().parent / path).resolve()
    return path


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


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)
