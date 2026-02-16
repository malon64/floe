# Floe + Dagster

This folder contains the Dagster connector for Floe.

For local setup of both Dagster and Airflow with isolated virtual environments, see:
- `orchestrators/LOCAL_DEV.md`

Current model:
- Parse-time: Dagster loads a generated `floe.manifest.v1` JSON file.
- Run-time: each asset executes Floe for one entity using the manifest `execution` contract.
- Metadata: connector parses NDJSON events and attaches run stats to materializations.

## Install

Prereqs:
- Floe installed (either the `floe` CLI binary **or** Docker with a Floe image).
- Python 3.10+.

```bash
pip install dagster-floe
```

## Development install (from this repo)

```bash
python3 -m venv orchestrators/dagster-floe/.venv
source orchestrators/dagster-floe/.venv/bin/activate
pip install -e orchestrators/dagster-floe[dev]
```

## Generate a manifest

```bash
floe manifest generate \
  -c orchestrators/dagster-floe/example/config.yml \
  --output orchestrators/dagster-floe/example/manifest.dagster.json
```

## Run the example (repo-only)

```bash
cd orchestrators/dagster-floe
FLOE_MANIFEST=./example/manifest.dagster.json dagster dev
```

The example workspace loads `example/definitions.py`, which wires local example files/manifest to the reusable connector APIs.

## Notes

- This connector does **not** parse YAML directly; it consumes `floe.manifest.v1`.
- Connector logic lives under `src/floe_dagster/`; local wiring for demo lives in `example/definitions.py`.
- For local development without an installed `floe` binary, you can point `LocalRunner` to a custom command, e.g.:
  - `LocalRunner(\"cargo run -p floe-cli --\")`
- Manifest runner support in connector is currently `local_process` only.
- Design notes and future work: `orchestrators/dagster-floe/INTEGRATION_SPEC.md`

## Releasing

This repo is a monorepo. Floe and this connector are versioned and tagged independently:

- Floe CLI release tags: `vX.Y.Z`
- Dagster connector release tags: `dagster-floe-vX.Y.Z` (triggers the PyPI publish workflow)

Example:

```bash
git checkout main
git pull
git tag dagster-floe-v0.1.0
git push origin dagster-floe-v0.1.0
```
