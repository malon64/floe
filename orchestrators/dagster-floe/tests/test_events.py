from pathlib import Path

from floe_dagster.events import last_run_finished, parse_ndjson_events, parse_run_finished


def test_ndjson_parsing_finds_run_finished():
    fixture = Path(__file__).parent / "fixtures" / "run_events.ndjson"
    stdout = fixture.read_text(encoding="utf-8")
    events = parse_ndjson_events(stdout)
    finished = parse_run_finished(last_run_finished(events))
    assert finished.run_id == "run-123"
    assert finished.summary_uri is not None


