"""Runtime helpers for Airflow-Floe DAGs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from airflow.sdk import Asset

from floe_manifest import AirflowManifest, ManifestEntity


def resolve_config_path(manifest_path: str, config_uri: str) -> str:
    if "://" in config_uri:
        # Keep remote config URI untouched.
        return config_uri

    config_path = Path(config_uri)
    if config_path.is_absolute():
        return str(config_path)

    base = Path(manifest_path).resolve().parent
    return str((base / config_path).resolve())


def _resolve_target_uri(manifest_path: str, target_uri: str) -> str:
    if "://" in target_uri:
        return target_uri

    target_path = Path(target_uri)
    if not target_path.is_absolute():
        base = Path(manifest_path).resolve().parent
        target_path = (base / target_path).resolve()

    return target_path.as_uri()


def build_entity_assets(manifest: AirflowManifest, manifest_path: str) -> dict[str, Asset]:
    assets: dict[str, Asset] = {}
    for entity in manifest.entities:
        name = "floe." + ".".join(entity.asset_key)
        uri = _resolve_target_uri(manifest_path, entity.accepted_sink_uri)
        assets[entity.name] = Asset(name=name, uri=uri, group=entity.group_name)
    return assets


def load_run_summary(summary_uri: str | None, config_path: str) -> dict[str, Any] | None:
    if not summary_uri:
        return None

    if summary_uri.startswith("file://"):
        parsed = urlparse(summary_uri)
        summary_path = Path(unquote(parsed.path))
    elif "://" in summary_uri:
        # Remote summary loading is out of scope for MVP.
        return None
    else:
        summary_path = Path(summary_uri)
        if not summary_path.is_absolute():
            base = Path(config_path).resolve().parent
            summary_path = (base / summary_path).resolve()

    if not summary_path.exists():
        return None

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None

    return payload


def summary_entities_by_name(summary_payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(summary_payload, dict):
        return {}

    entities = summary_payload.get("entities")
    if not isinstance(entities, list):
        return {}

    indexed: dict[str, dict[str, Any]] = {}
    for item in entities:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if isinstance(name, str) and name:
            indexed[name] = item
    return indexed


def build_asset_event_extra(
    entity: ManifestEntity,
    run_finished: dict[str, Any],
    summary_entity: dict[str, Any] | None,
) -> dict[str, Any]:
    summary_entity = summary_entity or {}
    results = summary_entity.get("results") if isinstance(summary_entity, dict) else None
    if not isinstance(results, dict):
        results = {}

    return {
        "entity": entity.name,
        "domain": entity.domain,
        "run_id": run_finished.get("run_id"),
        "status": summary_entity.get("status", run_finished.get("status")),
        "summary_uri": run_finished.get("summary_uri"),
        "files_total": results.get("files_total"),
        "rows_total": results.get("rows_total"),
        "accepted_total": results.get("accepted_total", run_finished.get("accepted")),
        "rejected_total": results.get("rejected_total", run_finished.get("rejected")),
        "warnings_total": results.get("warnings_total", run_finished.get("warnings")),
        "errors_total": results.get("errors_total", run_finished.get("errors")),
    }
