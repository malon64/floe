from __future__ import annotations

from typing import Callable

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class FloeError(Exception): ...
class FloeConfigError(FloeError): ...
class FloeRunError(FloeError): ...
class FloeStorageError(FloeError): ...
class FloeIoError(FloeError): ...

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
    def __repr__(self) -> str: ...

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def validate(
    config_path: str,
    entities: list[str] | None = None,
    profile_vars: dict[str, str] | None = None,
) -> None:
    """Validate a floe config file. Raises FloeConfigError on invalid config."""
    ...

def run(
    config_path: str,
    entities: list[str] | None = None,
    dry_run: bool = False,
    run_id: str | None = None,
    profile_vars: dict[str, str] | None = None,
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

def set_observer(callback: Callable[[dict], None]) -> bool:
    """
    Register a Python callable to receive live run events.

    The callback receives a dict representing a RunEvent with an ``event`` key
    (e.g. ``"run_started"``, ``"entity_finished"``, ``"run_finished"``).

    Returns True. The observer is swappable between notebook cells — calling
    set_observer again replaces the callback.
    """
    ...

def clear_observer() -> None:
    """Remove the current observer callback (events are silently discarded)."""
    ...
