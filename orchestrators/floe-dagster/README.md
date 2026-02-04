# Floe + Dagster example

This folder contains a Dagster project that runs Floe and materializes one asset per entity.

## Setup (uv)

From the repo root:

```bash
uv venv floe-dagster/.venv
uv pip install -p floe-dagster/.venv dagster dagster-webserver pyyaml
```

## Run

```bash
cd floe-dagster
source .venv/bin/activate

dagster dev
```

Then open the Dagster UI and materialize `floe_run` (it yields one asset per entity).

Notes:
- The job runs Floe once with `--log-format json` and parses `run.summary.json`.
- Entity assets are grouped by `domain` from the config.
- Default config used: `floe-dagster/example/config.yml`
- Override config with env var: `FLOE_CONFIG=/path/to/config.yml`
