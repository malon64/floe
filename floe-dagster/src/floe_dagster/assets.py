import json
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, Definitions, MetadataValue, asset

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = REPO_ROOT / "example" / "config.yml"


@asset(
    name="floe_example_run",
    group_name="floe",
    compute_kind="floe",
    description="Runs Floe using the example config.yml and emits run summary metadata.",
)
def floe_example_run(context: AssetExecutionContext) -> None:
    config_path = str(DEFAULT_CONFIG)
    result = subprocess.run(
        ["floe", "run", "-c", config_path],
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout.strip())
    if result.stderr:
        context.log.warning(result.stderr.strip())

    if result.returncode not in (0, 2, 1):
        context.log.warning("Unexpected exit code: %s", result.returncode)

    summary_path = None
    for line in result.stdout.splitlines():
        if line.startswith("Run summary:"):
            summary_path = line.replace("Run summary:", "").strip()
            break

    metadata = {
        "config": MetadataValue.path(config_path),
        "exit_code": result.returncode,
    }
    if summary_path:
        metadata["run_summary"] = MetadataValue.path(summary_path)
        try:
            summary_json = json.loads(Path(summary_path).read_text())
            metadata["summary"] = MetadataValue.json(summary_json)
        except Exception as exc:  # noqa: BLE001
            context.log.warning("Failed to read summary JSON: %s", exc)

    context.add_output_metadata(metadata)


defs = Definitions(assets=[floe_example_run])
