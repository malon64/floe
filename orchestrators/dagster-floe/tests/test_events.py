from pathlib import Path

from floe_dagster.events import (
    last_run_finished,
    parse_json_event_lines,
    parse_ndjson_events,
    parse_run_finished,
    render_log_event,
)


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


def test_json_event_line_parsing_ignores_human_summary_lines():
    stdout = "\n".join(
        [
            '{"event":"run_started","run_id":"run-1"}',
            (
                '{"event":"run_finished","run_id":"run-1","status":"success",'
                '"exit_code":0,"summary_uri":"s3://bucket/report/run.summary.json"}'
            ),
            "run id: run-1",
            "Totals: files=1 rows=3 accepted=3 rejected=0",
            "Overall: success (exit_code=0)",
        ]
    )

    events = parse_json_event_lines(stdout)
    finished = parse_run_finished(last_run_finished(events))

    assert len(events) == 2
    assert finished.run_id == "run-1"
    assert finished.summary_uri == "s3://bucket/report/run.summary.json"


def test_render_log_event_formats_file_and_summary_details():
    line = render_log_event(
        {
            "event": "file_finished",
            "run_id": "run-1",
            "entity": "customers",
            "input": "s3://bucket/bronze/customers.csv",
            "status": "success",
            "rows": 3,
            "accepted": 3,
            "rejected": 0,
            "elapsed_ms": 42,
        }
    )

    assert line == (
        "floe file finished run_id=run-1 entity=customers "
        "input=s3://bucket/bronze/customers.csv status=success "
        "rows=3 accepted=3 rejected=0 elapsed_ms=42"
    )

    line = render_log_event(
        {
            "event": "run_finished",
            "run_id": "run-1",
            "status": "success",
            "exit_code": 0,
            "files": 1,
            "rows": 3,
            "accepted": 3,
            "rejected": 0,
            "summary_uri": "s3://bucket/report/run.summary.json",
        }
    )

    assert "floe run finished run_id=run-1 status=success exit_code=0" in line
    assert "summary_uri=s3://bucket/report/run.summary.json" in line


def test_parse_run_finished_extracts_new_fields():
    event = {
        "event": "run_finished",
        "run_id": "run-2",
        "status": "success",
        "exit_code": 0,
        "summary_uri": "s3://bucket/reports/run_2/run.summary.json",
        "report_base": "s3://bucket/reports",
        "entity_report_uris": {
            "customers": "s3://bucket/reports/run_2/entities/customers.report.json",
        },
    }
    finished = parse_run_finished(event)
    assert finished.report_base == "s3://bucket/reports"
    assert finished.entity_report_uris["customers"].endswith("customers.report.json")


def test_parse_run_finished_defaults_new_fields_when_absent():
    event = {
        "event": "run_finished",
        "run_id": "run-3",
        "status": "success",
        "exit_code": 0,
        "summary_uri": None,
    }
    finished = parse_run_finished(event)
    assert finished.report_base is None
    assert finished.entity_report_uris == {}
