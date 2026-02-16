from pathlib import Path

from floe_dagster.events import last_run_finished, parse_ndjson_events, parse_run_finished


def test_ndjson_parsing_finds_run_finished():
    fixture = Path(__file__).parent / "fixtures" / "run_events.ndjson"
    stdout = fixture.read_text(encoding="utf-8")
    events = parse_ndjson_events(stdout)
    finished = parse_run_finished(last_run_finished(events))
    assert finished.run_id == "run-123"
    assert finished.summary_uri is not None


def test_parse_run_finished_supports_custom_summary_field():
    event = {
        "event": "run_finished",
        "run_id": "run-1",
        "status": "success",
        "exit_code": 0,
        "custom_summary": "local:///tmp/run.summary.json",
    }
    finished = parse_run_finished(event, summary_uri_field="custom_summary")
    assert finished.summary_uri == "local:///tmp/run.summary.json"

