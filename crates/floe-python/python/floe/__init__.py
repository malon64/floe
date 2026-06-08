"""
floe — Python bindings for the floe ingestion engine.

Run floe pipelines at Rust speed from Python notebooks::

    import floe

    # Live progress
    floe.set_observer(lambda e: print(f"[{e['event']}]", e.get("name", "")))

    # Validate config
    floe.validate("pipeline.yml")

    # Execute pipeline
    outcome = floe.run("pipeline.yml", entities=["customers"])
    print(outcome.summary["results"])

    # Dry run
    outcome = floe.run("pipeline.yml", dry_run=True)
    for p in outcome.dry_run_previews:
        print(p["name"], p["scanned_files"])

    # Inspect incremental state
    state = floe.inspect_entity_state("pipeline.yml", "customers")
    print(state["incremental_mode"], state["state"])

    # Use a profile (with catalog / variable overrides)
    floe.run("pipeline.yml", profile_path="profile.yml")
    floe.run("pipeline.yml", profile_vars={"output_bucket": "s3://my-bucket"})
"""

from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("floe-python")
except Exception:
    __version__ = "unknown"

from floe._floe import (
    FloeError,
    FloeConfigError,
    FloeRunError,
    FloeStorageError,
    FloeIoError,
    RootConfig,
    EntityConfig,
    RunOutcome,
    validate,
    run as _run,
    load_config,
    extract_config_env_vars,
    inspect_entity_state,
    reset_entity_state,
    set_observer as _lean_set_observer,
    clear_observer as _lean_clear_observer,
)
from floe import _floe as _lean

# DuckDB sinks compile a bundled native build that is too large for PyPI, so the
# published `floe` wheel is lean. DuckDB support ships as a separate off-PyPI
# `floe-duckdb` wheel that installs a `floe._floe_duckdb` companion extension into
# this same package. When a run targets a DuckDB sink and this is the lean build,
# `run` transparently delegates to that companion, mirroring the lean `floe` CLI
# re-execing the `floe-duckdb` binary. Install instructions for the companion:
_DUCKDB_INSTALL_HINT = (
    "this config writes to a DuckDB sink, but the installed `floe` wheel is the "
    "lean build without DuckDB support and the `floe-duckdb` companion wheel is "
    "not installed. Install it from the off-PyPI index, e.g. "
    "`pip install floe-duckdb --extra-index-url "
    "https://malon64.github.io/floe/simple/`, or use the "
    "`ghcr.io/malon64/floe-duckdb` image."
)


# The lean `_floe` and companion `_floe_duckdb` are separate native libraries
# with independent observer state, so the registered callback must be mirrored
# into the companion before a delegated run or its progress/log events are lost.
# Track the active observer here so delegation can re-install it.
_current_observer = None


def set_observer(callback):
    """Register a callback for live run/validation events.

    Mirrors into the DuckDB companion (if loaded) so delegated DuckDB runs
    emit the same events as native runs.
    """
    global _current_observer
    _current_observer = callback
    companion = _duckdb_module()
    if companion is not None:
        companion.set_observer(callback)
    return _lean_set_observer(callback)


def clear_observer():
    """Remove the previously registered event callback."""
    global _current_observer
    _current_observer = None
    companion = _duckdb_module()
    if companion is not None:
        companion.clear_observer()
    return _lean_clear_observer()


def _duckdb_module():
    """Return the `floe._floe_duckdb` companion extension, or None if not installed.

    Only a genuinely absent companion maps to None. If the companion IS installed
    but its native extension fails to load (incompatible libc, unresolved symbol),
    the import raises a plain ImportError (not ModuleNotFoundError); that is allowed
    to propagate so the real loader error surfaces instead of a misleading
    "companion wheel is not installed" hint.
    """
    try:
        from floe import _floe_duckdb

        return _floe_duckdb
    except ModuleNotFoundError as exc:
        if exc.name in ("floe._floe_duckdb", "_floe_duckdb"):
            return None
        raise


def run(config_path, *args, **kwargs):
    """Execute a floe pipeline.

    Transparently delegates to the `floe-duckdb` companion when the config
    targets a DuckDB sink and this lean build lacks native DuckDB support.
    """
    # Dry runs resolve inputs and return previews before any sink write, so they
    # never touch the DuckDB writer — run them on the lean build without the
    # companion. Positional order mirrors the native `run`:
    # (config_path, entities, dry_run, run_id, full_refresh, profile_vars, profile_path).
    dry_run = kwargs.get("dry_run", args[1] if len(args) >= 2 else False)
    # The sink format can be selected through a profile variable (e.g.
    # `format: "{{SINK_FORMAT}}"`), so the delegation check must apply the same
    # profile inputs as the run or it would miss the DuckDB sink and run lean.
    profile_vars = kwargs.get("profile_vars", args[4] if len(args) >= 5 else None)
    profile_path = kwargs.get("profile_path", args[5] if len(args) >= 6 else None)
    if (
        not dry_run
        and not getattr(_lean, "HAS_DUCKDB", False)
        and _lean.config_targets_duckdb(
            config_path, profile_vars=profile_vars, profile_path=profile_path
        )
    ):
        companion = _duckdb_module()
        if companion is None:
            raise FloeConfigError(_DUCKDB_INSTALL_HINT)
        if _current_observer is not None:
            companion.set_observer(_current_observer)
        return companion.run(config_path, *args, **kwargs)
    return _run(config_path, *args, **kwargs)


__all__ = [
    "__version__",
    "FloeError",
    "FloeConfigError",
    "FloeRunError",
    "FloeStorageError",
    "FloeIoError",
    "RootConfig",
    "EntityConfig",
    "RunOutcome",
    "validate",
    "run",
    "load_config",
    "extract_config_env_vars",
    "inspect_entity_state",
    "reset_entity_state",
    "set_observer",
    "clear_observer",
]
