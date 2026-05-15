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
    run,
    load_config,
    extract_config_env_vars,
    inspect_entity_state,
    reset_entity_state,
    set_observer,
    clear_observer,
)

__all__ = [
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
