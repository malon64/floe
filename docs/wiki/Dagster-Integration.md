# Dagster Integration (MVP)

Floe includes a Dagster connector in `orchestrators/dagster-floe`.

## Install (editable)

From repo root:

```bash
python3 -m venv orchestrators/dagster-floe/.venv
source orchestrators/dagster-floe/.venv/bin/activate
pip install -e orchestrators/dagster-floe[dev]
```

## Run the example

```bash
cd orchestrators/dagster-floe
dagster dev
```

The example workspace loads `floe_dagster.definitions` and generates one asset per entity in `orchestrators/dagster-floe/example/config.yml`.

## Run with Docker (instead of a local binary)

```bash
export FLOE_DOCKER_IMAGE="ghcr.io/malon64/floe:latest"
cd orchestrators/dagster-floe
source .venv/bin/activate
FLOE_DOCKER_IMAGE="$FLOE_DOCKER_IMAGE" dagster dev
```

## Notes

- The connector calls `floe validate --output json` to generate assets.
- It calls `floe run --entities <name> --log-format json` to execute.
- NDJSON events are parsed and attached to Dagster materializations.

See `orchestrators/dagster-floe/README.md` for details.
