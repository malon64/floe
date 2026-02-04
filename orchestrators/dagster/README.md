# Floe + Dagster (MVP)

This folder contains a small Dagster connector for Floe, implemented as a Python package.

Goals of the MVP:
- Generate one Dagster asset per Floe entity by calling `floe validate --output json`
- Execute one entity per asset with `floe run --entities <name> --log-format json`
- Parse NDJSON events from stdout and attach run stats to Dagster materialization metadata

## Install (editable)

From repo root:

```bash
python -m venv orchestrators/dagster/.venv
source orchestrators/dagster/.venv/bin/activate
pip install -e orchestrators/dagster[dev]
```

## Run the example

```bash
cd orchestrators/dagster
dagster dev
```

The example workspace loads `floe_dagster.definitions` which generates assets from `orchestrators/dagster/example/config.yml`.

## Notes

- This connector does **not** parse YAML directly; it uses `floe validate --output json` as the source of truth.
- For local development without an installed `floe` binary, you can point `LocalRunner` to a custom command, e.g.:
  - `LocalRunner(\"cargo run -p floe-cli --\")`
- `DockerRunner` mounts the config directory by default; if your config uses paths like `../data/...`, it mounts a higher parent directory so those paths remain visible. For full control, pass `workdir=...`.
- Design notes and future work: `orchestrators/dagster/INTEGRATION_SPEC.md`
