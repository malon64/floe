import os
import shutil

import pytest

from floe_dagster.events import last_run_finished, parse_ndjson_events
from floe_dagster.runner import LocalRunner


@pytest.mark.skipif(
    shutil.which("floe") is None or not bool(os.environ.get("FLOE_TEST_LOCALRUNNER")),
    reason="requires a local floe binary + FLOE_TEST_LOCALRUNNER=1",
)
def test_localrunner_runs_example_validate_json(tmp_path):
    runner = LocalRunner("floe")
    data = runner.run_floe_validate(config_uri="example/config.yml", entities=None)
    assert data["schema"] == "floe.plan.v1"


@pytest.mark.skipif(
    shutil.which("floe") is None or not bool(os.environ.get("FLOE_TEST_LOCALRUNNER")),
    reason="requires a local floe binary + FLOE_TEST_LOCALRUNNER=1",
)
def test_localrunner_runs_example_entity_json_logs():
    runner = LocalRunner("floe")
    result = runner.run_floe_entity(
        config_uri="example/config.yml", run_id=None, entity="employees", log_format="json"
    )
    events = parse_ndjson_events(result.stdout)
    assert last_run_finished(events)["event"] == "run_finished"
