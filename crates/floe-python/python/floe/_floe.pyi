from __future__ import annotations

from typing import Callable, Literal, TypedDict, Union

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class FloeError(Exception): ...
class FloeConfigError(FloeError): ...
class FloeRunError(FloeError): ...
class FloeStorageError(FloeError): ...
class FloeIoError(FloeError): ...

# ---------------------------------------------------------------------------
# Observer event types
# ---------------------------------------------------------------------------

class RunStartedEvent(TypedDict):
    event: Literal["run_started"]
    run_id: str
    config: str
    report_base: str | None
    ts_ms: int

class EntityStartedEvent(TypedDict):
    event: Literal["entity_started"]
    run_id: str
    name: str
    ts_ms: int

class FileStartedEvent(TypedDict):
    event: Literal["file_started"]
    run_id: str
    entity: str
    input: str
    ts_ms: int

class FileFinishedEvent(TypedDict):
    event: Literal["file_finished"]
    run_id: str
    entity: str
    input: str
    status: str
    rows: int
    accepted: int
    rejected: int
    elapsed_ms: int
    ts_ms: int

class SchemaEvolutionAppliedEvent(TypedDict):
    event: Literal["schema_evolution_applied"]
    run_id: str
    entity: str
    mode: str
    added_columns: list[str]
    ts_ms: int

class EntityFinishedEvent(TypedDict):
    event: Literal["entity_finished"]
    run_id: str
    name: str
    status: str
    files: int
    rows: int
    accepted: int
    rejected: int
    warnings: int
    errors: int
    ts_ms: int

class RunFinishedEvent(TypedDict):
    event: Literal["run_finished"]
    run_id: str
    status: str
    exit_code: int
    files: int
    rows: int
    accepted: int
    rejected: int
    warnings: int
    errors: int
    summary_uri: str | None
    ts_ms: int

class LogEvent(TypedDict):
    event: Literal["log"]
    run_id: str
    log_level: str
    code: str | None
    message: str
    entity: str | None
    input: str | None
    ts_ms: int

RunEvent = Union[
    RunStartedEvent,
    EntityStartedEvent,
    FileStartedEvent,
    FileFinishedEvent,
    SchemaEvolutionAppliedEvent,
    EntityFinishedEvent,
    RunFinishedEvent,
    LogEvent,
]

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class EntityConfig:
    name: str
    domain: str | None
    source_format: str
    source_path: str
    source_storage: str | None
    accepted_format: str
    accepted_path: str
    accepted_storage: str | None
    incremental_mode: str
    def __repr__(self) -> str: ...

class RootConfig:
    @property
    def version(self) -> str: ...
    @property
    def entity_names(self) -> list[str]: ...
    @property
    def entities(self) -> list[EntityConfig]: ...
    def __repr__(self) -> str: ...

class RunOutcome:
    run_id: str
    report_base_path: str | None
    dry_run: bool
    @property
    def summary(self) -> dict: ...
    @property
    def entity_reports(self) -> list[dict]: ...
    @property
    def dry_run_previews(self) -> list[dict] | None: ...
    def to_dict(self) -> dict: ...
    def _repr_html_(self) -> str: ...
    def __repr__(self) -> str: ...

# ---------------------------------------------------------------------------
# Capability flags
# ---------------------------------------------------------------------------

HAS_DUCKDB: bool
"""True only in the `+duckdb` build (the off-PyPI `floe-duckdb` companion)."""

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def config_targets_duckdb(config_path: str) -> bool:
    """True when the config writes to a DuckDB sink. Returns False if the config
    cannot be loaded, so the normal run path surfaces the real error."""
    ...

def validate(
    config_path: str,
    entities: list[str] | None = None,
    profile_vars: dict[str, str] | None = None,
    profile_path: str | None = None,
) -> None:
    """Validate a floe config file. Raises FloeConfigError on invalid config."""
    ...

def run(
    config_path: str,
    entities: list[str] | None = None,
    dry_run: bool = False,
    run_id: str | None = None,
    full_refresh: bool = False,
    profile_vars: dict[str, str] | None = None,
    profile_path: str | None = None,
) -> RunOutcome:
    """Execute the floe ingestion pipeline. Raises FloeRunError on failure."""
    ...

def load_config(config_path: str) -> RootConfig:
    """Parse a floe config YAML and return a RootConfig object."""
    ...

def extract_config_env_vars(config_path: str) -> dict[str, str]:
    """Return the env variable placeholders declared in the config."""
    ...

def inspect_entity_state(config_path: str, entity_name: str) -> dict:
    """Return the incremental state for an entity as a dict."""
    ...

def reset_entity_state(config_path: str, entity_name: str) -> bool:
    """Delete the incremental state file for an entity. Returns True if deleted."""
    ...

# ---------------------------------------------------------------------------
# Observer
# ---------------------------------------------------------------------------

def set_observer(callback: Callable[[RunEvent], None]) -> bool:
    """
    Register a Python callable to receive live run events.

    The callback receives a typed dict (RunEvent) with an ``event`` key
    identifying the event type (e.g. ``"run_started"``, ``"entity_finished"``).
    The observer is swappable between notebook cells — calling set_observer
    again replaces the previous callback.

    Returns True.
    """
    ...

def clear_observer() -> None:
    """Remove the current observer callback (events are silently discarded)."""
    ...
