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
    set_observer,
    clear_observer,
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


def _duckdb_module():
    """Return the `floe._floe_duckdb` companion extension, or None if absent."""
    try:
        from floe import _floe_duckdb

        return _floe_duckdb
    except ImportError:
        return None


def run(config_path, *args, **kwargs):
    """Execute a floe pipeline.

    Transparently delegates to the `floe-duckdb` companion when the config
    targets a DuckDB sink and this lean build lacks native DuckDB support.
    """
    if not getattr(_lean, "HAS_DUCKDB", False) and _lean.config_targets_duckdb(
        config_path
    ):
        companion = _duckdb_module()
        if companion is None:
            raise FloeConfigError(_DUCKDB_INSTALL_HINT)
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
