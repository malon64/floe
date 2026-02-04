import json
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

import yaml
from dagster import AssetExecutionContext, AssetOut, Output, multi_asset

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = REPO_ROOT / "floe-dagster" / "example" / "config.yml"


def _load_config(config_path: Path) -> Tuple[Dict[str, dict], List[dict]]:
    data = yaml.safe_load(config_path.read_text())
    domains = {item["name"]: item for item in data.get("domains", [])}
    entities = data.get("entities", [])
    return domains, entities


def _output_name(entity_name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]", "_", entity_name)
    if not name or name[0].isdigit():
        name = f"entity_{name}"
    return name


def _resolve_report_path(summary_path: Path, report_file: str) -> Path:
    report_path = Path(report_file)
    if report_path.is_absolute():
        return report_path
    return summary_path.parent / report_path


def _parse_summary(summary_path: Path) -> dict:
    return json.loads(summary_path.read_text())


def _parse_run_report(path: Path) -> dict:
    return json.loads(path.read_text())


def _extract_summary_path(stdout: str) -> str | None:
    for line in stdout.splitlines():
        if line.startswith("Run summary:"):
            return line.replace("Run summary:", "").strip()
    return None


def _stream_events(context: AssetExecutionContext, stdout: str) -> Dict[str, str]:
    info: Dict[str, str] = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                context.log.info(line)
                continue
            event_type = event.get("event")
            if event_type == "run_started":
                if event.get("run_id"):
                    info["run_id"] = event["run_id"]
                if event.get("report_base"):
                    info["report_base"] = event["report_base"]
            if event_type == "run_finished" and event.get("summary_uri"):
                info["summary_uri"] = event["summary_uri"]
            if event_type == "warning":
                context.log.warning(event.get("message", ""))
            elif event_type == "error":
                context.log.error(event.get("message", ""))
            else:
                context.log.info(json.dumps(event, ensure_ascii=False))
        else:
            context.log.info(line)
    return info


def _build_outs(domains: Dict[str, dict], entities: List[dict]) -> Dict[str, AssetOut]:
    outs: Dict[str, AssetOut] = {}
    for entity in entities:
        domain = entity.get("domain")
        group = domain if domain in domains else "default"
        outs[_output_name(entity["name"]) ] = AssetOut(group_name=group)
    return outs


_config_path = Path(os.getenv("FLOE_CONFIG", str(DEFAULT_CONFIG)))
_domains, _entities = _load_config(_config_path)
_outs = _build_outs(_domains, _entities)
_entity_names = [entity["name"] for entity in _entities]
_name_to_output = {entity["name"]: _output_name(entity["name"]) for entity in _entities}


@multi_asset(
    outs=_outs,
    name="floe_run",
    compute_kind="floe",
    description="Runs Floe once and materializes one asset per entity.",
)
def floe_run(context: AssetExecutionContext):
    config_path = str(_config_path)
    result = subprocess.run(
        [
            "cargo",
            "run",
            "-p",
            "floe-cli",
            "--",
            "run",
            "-c",
            config_path,
            "--log-format",
            "json",
        ],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )

    event_info = _stream_events(context, result.stdout)
    if result.stderr:
        context.log.warning(result.stderr.strip())

    if result.returncode != 0:
        context.log.error("Floe failed with exit code %s", result.returncode)
        context.log.error("Floe stdout (tail): %s", result.stdout[-2000:].strip())
        context.log.error("Floe stderr (tail): %s", result.stderr[-2000:].strip())
        raise RuntimeError(f"Floe failed with exit code {result.returncode}")
    if result.returncode not in (0, 1, 2):
        context.log.warning("Unexpected exit code: %s", result.returncode)

    summary_path_str = event_info.get("summary_uri") or _extract_summary_path(
        result.stdout
    )
    if not summary_path_str:
        run_id = event_info.get("run_id")
        report_base = event_info.get("report_base")
        if run_id and report_base:
            summary_path_str = str(
                Path(report_base) / f"run_{run_id}" / "run.summary.json"
            )
        else:
            context.log.error("Floe stdout (tail): %s", result.stdout[-2000:].strip())
            context.log.error("Floe stderr (tail): %s", result.stderr[-2000:].strip())
            raise RuntimeError("Run summary path not found in floe output")

    summary_path = Path(summary_path_str)
    summary = _parse_summary(summary_path)

    per_entity_report: Dict[str, dict] = {}
    for entity in summary.get("entities", []):
        report_file = entity.get("report_file")
        if not report_file:
            continue
        report_path = _resolve_report_path(summary_path, report_file)
        per_entity_report[entity["name"]] = _parse_run_report(report_path)

    for entity in summary.get("entities", []):
        name = entity["name"]
        output_name = _name_to_output.get(name)
        if output_name is None:
            continue

        report = per_entity_report.get(name, {})
        results = report.get("results", {})
        metadata = {
            "run_id": summary.get("run", {}).get("run_id"),
            "entity": name,
            "status": entity.get("status"),
            "files_total": results.get("files_total"),
            "rows_total": results.get("rows_total"),
            "accepted_total": results.get("accepted_total"),
            "rejected_total": results.get("rejected_total"),
            "report_file": entity.get("report_file"),
        }

        yield Output(None, output_name=output_name, metadata=metadata)
