"""
Test suite covering the PR test plan for floe-python.

Run with:  pytest crates/floe-python/tests/ -v
"""

import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

import floe

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures"
VALID_CONFIG = str(FIXTURE_DIR / "config.yml")
INVALID_CONFIG = str(FIXTURE_DIR / "invalid_config.yml")


# ---------------------------------------------------------------------------
# Test plan item 1: cargo check (verified in CI — import test doubles as smoke)
# Test plan item 2: maturin develop + import floe
# ---------------------------------------------------------------------------


def test_import():
    """import floe works and exposes expected public names."""
    assert callable(floe.validate)
    assert callable(floe.run)
    assert callable(floe.load_config)
    assert callable(floe.extract_config_env_vars)
    assert callable(floe.inspect_entity_state)
    assert callable(floe.reset_entity_state)
    assert callable(floe.set_observer)
    assert callable(floe.clear_observer)
    assert issubclass(floe.FloeError, Exception)
    assert issubclass(floe.FloeConfigError, floe.FloeError)
    assert issubclass(floe.FloeRunError, floe.FloeError)


# ---------------------------------------------------------------------------
# Test plan item 3: validate() returns without error on a valid config
# ---------------------------------------------------------------------------


def test_validate_valid_config():
    floe.validate(VALID_CONFIG)


def test_validate_with_entity_filter():
    floe.validate(VALID_CONFIG, entities=["customer"])


# ---------------------------------------------------------------------------
# Test plan item 6: FloeConfigError raised on invalid config
# ---------------------------------------------------------------------------


def test_validate_invalid_config_raises():
    with pytest.raises(floe.FloeConfigError):
        floe.validate(INVALID_CONFIG)


def test_validate_missing_file_raises():
    with pytest.raises(floe.FloeError):
        floe.validate("/nonexistent/path/config.yml")


# ---------------------------------------------------------------------------
# Test plan item 7: load_config returns RootConfig with correct entity_names
# ---------------------------------------------------------------------------


def test_load_config_returns_root_config():
    cfg = floe.load_config(VALID_CONFIG)
    assert isinstance(cfg, floe.RootConfig)
    assert cfg.version == "0.1"
    assert "customer" in cfg.entity_names


def test_load_config_entities():
    cfg = floe.load_config(VALID_CONFIG)
    entities = cfg.entities
    assert len(entities) > 0
    customer = next(e for e in entities if e.name == "customer")
    assert customer.source_format == "csv"
    assert customer.accepted_format == "parquet"
    assert customer.incremental_mode == "none"


def test_incremental_mode_canonical_strings():
    """incremental_mode must use lowercase canonical values matching YAML config."""
    cfg = floe.load_config(VALID_CONFIG)
    for entity in cfg.entities:
        assert entity.incremental_mode in ("none", "file", "row", "archive"), (
            f"unexpected incremental_mode: {entity.incremental_mode!r}"
        )


def test_root_config_repr():
    cfg = floe.load_config(VALID_CONFIG)
    r = repr(cfg)
    assert "RootConfig" in r
    assert "version" in r


# ---------------------------------------------------------------------------
# Test plan item 3/4: run() returns RunOutcome with correct summary dict
# ---------------------------------------------------------------------------


def test_run_returns_outcome(tmp_path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    report_dir = tmp_path / "report"
    report_dir.mkdir()

    outcome = floe.run(
        VALID_CONFIG,
        profile_vars={
            "out_root": str(out_dir),
            "report_root": str(report_dir),
            "incoming_root": str(FIXTURE_DIR / "in"),
        },
    )
    assert isinstance(outcome, floe.RunOutcome)
    assert outcome.run_id
    assert not outcome.dry_run

    summary = outcome.summary
    assert isinstance(summary, dict)
    assert "results" in summary
    assert "accepted_total" in summary["results"]
    assert summary["results"]["accepted_total"] >= 0


def test_run_entity_reports(tmp_path):
    outcome = floe.run(
        VALID_CONFIG,
        entities=["customer"],
        profile_vars={
            "out_root": str(tmp_path / "out"),
            "report_root": str(tmp_path / "report"),
            "incoming_root": str(FIXTURE_DIR / "in"),
        },
    )
    reports = outcome.entity_reports
    assert len(reports) == 1
    report = reports[0]
    assert "results" in report
    assert report["entity"]["name"] == "customer"


def test_dry_run(tmp_path):
    outcome = floe.run(
        VALID_CONFIG,
        dry_run=True,
        profile_vars={
            "out_root": str(tmp_path / "out"),
            "report_root": str(tmp_path / "report"),
            "incoming_root": str(FIXTURE_DIR / "in"),
        },
    )
    assert outcome.dry_run is True
    assert outcome.dry_run_previews is not None
    assert len(outcome.dry_run_previews) > 0
    preview = outcome.dry_run_previews[0]
    assert "name" in preview
    assert "scanned_files" in preview
    assert isinstance(preview["scanned_files"], list)


def test_run_outcome_to_dict(tmp_path):
    outcome = floe.run(
        VALID_CONFIG,
        dry_run=True,
        profile_vars={
            "out_root": str(tmp_path / "out"),
            "report_root": str(tmp_path / "report"),
            "incoming_root": str(FIXTURE_DIR / "in"),
        },
    )
    d = outcome.to_dict()
    assert isinstance(d, dict)
    assert "run_id" in d
    assert "summary" in d
    assert "entity_reports" in d


# ---------------------------------------------------------------------------
# Test plan item 5: set_observer fires for entity_finished and run_finished
# ---------------------------------------------------------------------------


def test_observer_receives_events(tmp_path):
    events = []
    floe.set_observer(lambda e: events.append(e))

    floe.run(
        VALID_CONFIG,
        profile_vars={
            "out_root": str(tmp_path / "out"),
            "report_root": str(tmp_path / "report"),
            "incoming_root": str(FIXTURE_DIR / "in"),
        },
    )

    event_types = {e["event"] for e in events}
    assert "run_started" in event_types
    assert "run_finished" in event_types

    floe.clear_observer()


def test_observer_callback_is_swappable(tmp_path):
    """set_observer can be called multiple times to swap the callback."""
    first_events = []
    second_events = []

    floe.set_observer(lambda e: first_events.append(e["event"]))
    floe.run(
        VALID_CONFIG,
        profile_vars={
            "out_root": str(tmp_path / "out"),
            "report_root": str(tmp_path / "report"),
            "incoming_root": str(FIXTURE_DIR / "in"),
        },
    )
    assert "run_finished" in first_events

    floe.set_observer(lambda e: second_events.append(e["event"]))
    floe.run(
        VALID_CONFIG,
        profile_vars={
            "out_root": str(tmp_path / "out2"),
            "report_root": str(tmp_path / "report2"),
            "incoming_root": str(FIXTURE_DIR / "in"),
        },
    )
    assert "run_finished" in second_events
    # first observer must not have received second run's events
    first_count = first_events.count("run_finished")
    assert first_count == 1

    floe.clear_observer()


# ---------------------------------------------------------------------------
# Test plan item 8: existing floe-core and floe-cli tests unaffected
# (covered by the CI test job which runs the full workspace test suite)
# ---------------------------------------------------------------------------


def test_extract_config_env_vars():
    env_vars = floe.extract_config_env_vars(VALID_CONFIG)
    assert isinstance(env_vars, dict)
    assert "report_root" in env_vars
    assert "out_root" in env_vars
    assert "incoming_root" in env_vars


def test_inspect_entity_state_no_state(tmp_path):
    """inspect_entity_state returns a dict even when no state file exists."""
    result = floe.inspect_entity_state(VALID_CONFIG, "customer")
    assert isinstance(result, dict)
    assert result["entity_name"] == "customer"
    assert result["incremental_mode"] in ("none", "file", "row", "archive")
    assert "path_uri" in result
    assert "state" in result
