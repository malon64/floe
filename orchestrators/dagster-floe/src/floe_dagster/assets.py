from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dagster import Definitions, Failure, MaterializeResult, asset

from .events import last_run_finished, parse_ndjson_events, parse_run_finished
from .plan import FloeValidatePlan
from .runner import Runner


def load_floe_assets(
    config_uri: str,
    runner: Runner,
    entities: list[str] | None = None,
) -> Definitions:
    plan_json = runner.run_floe_validate(config_uri=config_uri, entities=entities)
    plan = FloeValidatePlan.from_validate_json(plan_json)

    assets_defs = []
    for entity in plan.entities:
        group_name = entity.group_name
        name = entity.name

        assets_defs.append(
            _make_entity_asset(
                key_prefix=list(entity.asset_key_parts[:-1]),
                group_name=group_name,
                name=name,
                config_uri=config_uri,
                runner=runner,
            )
        )

    return Definitions(assets=assets_defs)


def _make_entity_asset(
    *,
    key_prefix: list[str],
    group_name: str,
    name: str,
    config_uri: str,
    runner: Runner,
):
    @asset(name=name, key_prefix=key_prefix, group_name=group_name)
    def _asset(context) -> MaterializeResult:
        run_id = getattr(context, "run_id", None)
        result = runner.run_floe_entity(
            config_uri=config_uri,
            run_id=run_id,
            entity=name,
            log_format="json",
        )

        if result.stderr.strip():
            for line in result.stderr.splitlines():
                context.log.info(line)

        events = parse_ndjson_events(result.stdout)
        finished_event = last_run_finished(events)
        finished = parse_run_finished(finished_event)

        summary_uri = finished.summary_uri
        entity_stats: dict[str, Any] = {}

        if summary_uri:
            try:
                summary_json = _load_summary_json(summary_uri)
                entity_stats = _extract_entity_stats(summary_json, name)
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
                description=f"floe run failed for entity {name}",
                metadata=metadata,
            )

        return MaterializeResult(metadata=metadata)

    return _asset


def _load_summary_json(summary_uri: str) -> dict[str, Any]:
    if summary_uri.startswith("local://"):
        path_str = summary_uri[len("local://") :]
        path = Path(path_str)
    else:
        path = Path(summary_uri)

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
