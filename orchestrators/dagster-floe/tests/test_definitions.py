from pathlib import Path

import pytest

from floe_dagster.definitions import (
    build_definitions,
    build_definitions_from_manifest_dir,
    build_definitions_from_manifest_paths,
)
from floe_dagster.runner import RunResult, Runner


class _NoopRunner(Runner):
    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution=None,
        runner_definition=None,
    ) -> RunResult:
        del config_uri, run_id, entity, log_format, execution, runner_definition
        return RunResult(stdout="", stderr="", exit_code=0)


def test_build_definitions_from_manifest_path() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    defs = build_definitions(
        manifest_path=str(fixture),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert defs is not None
    job = defs.get_job_def("floe_mfv1_test_manifest_job")
    assert job is not None


def test_build_definitions_can_disable_job_creation() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    defs = build_definitions(
        manifest_path=str(fixture),
        runner=_NoopRunner(),
        entities=["employees"],
        with_job=False,
    )
    job_names = [job.name for job in defs.resolve_all_job_defs()]
    assert "floe_mfv1_test_manifest_job" not in job_names


def test_build_definitions_from_manifest_paths_creates_one_job_per_manifest() -> None:
    fixture_dir = Path(__file__).parent / "fixtures"
    defs = build_definitions_from_manifest_paths(
        manifest_paths=[
            str((fixture_dir / "manifest_hr.json").resolve()),
            str((fixture_dir / "manifest_sales.json").resolve()),
        ],
        runner=_NoopRunner(),
    )
    job_names = [job.name for job in defs.resolve_all_job_defs()]
    assert "floe_mfv1_hr_job" in job_names
    assert "floe_mfv1_sales_job" in job_names


def test_build_definitions_from_manifest_dir_loads_files() -> None:
    fixture_dir = Path(__file__).parent / "fixtures"
    defs = build_definitions_from_manifest_dir(
        manifest_dir=str(fixture_dir.resolve()),
        pattern="manifest_*.json",
        runner=_NoopRunner(),
    )
    job_names = [job.name for job in defs.resolve_all_job_defs()]
    assert "floe_mfv1_hr_job" in job_names
    assert "floe_mfv1_sales_job" in job_names


def test_build_definitions_from_manifest_paths_rejects_duplicate_asset_keys() -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest_hr.json"
    with pytest.raises(ValueError, match="duplicate asset key"):
        build_definitions_from_manifest_paths(
            manifest_paths=[str(fixture.resolve()), str(fixture.resolve())],
            runner=_NoopRunner(),
        )


def test_build_definitions_from_manifest_paths_rejects_job_name_override_for_many() -> None:
    fixture_dir = Path(__file__).parent / "fixtures"
    with pytest.raises(ValueError, match="job_name override"):
        build_definitions_from_manifest_paths(
            manifest_paths=[
                str((fixture_dir / "manifest_hr.json").resolve()),
                str((fixture_dir / "manifest_sales.json").resolve()),
            ],
            runner=_NoopRunner(),
            job_name="custom_job",
        )
