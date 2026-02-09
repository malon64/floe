# Floe + Dagster (MVP)

This folder contains a small Dagster connector for Floe, implemented as a Python package.

Goals of the MVP:
- Generate one Dagster asset per Floe entity by calling `floe validate --output json`
- Execute one entity per asset with `floe run --entities <name> --log-format json`
- Parse NDJSON events from stdout and attach run stats to Dagster materialization metadata

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

## Run the example (repo-only)

```bash
cd orchestrators/dagster-floe
dagster dev
```

The example workspace loads `floe_dagster.definitions` which generates assets from `orchestrators/dagster-floe/example/config.yml`.

## Run using Docker (instead of a local `floe` binary)

Prereqs:
- Docker installed and working (`docker run hello-world` succeeds).
- A Floe image available (either build locally or pull from GHCR).

Option A — pull from GHCR:

```bash
export FLOE_DOCKER_IMAGE="ghcr.io/malon64/floe:latest"
```

Option B — build locally from this repo:

```bash
docker build -t floe:dev .
export FLOE_DOCKER_IMAGE="floe:dev"
```

Then run Dagster:

```bash
cd orchestrators/dagster-floe
source .venv/bin/activate
FLOE_DOCKER_IMAGE="$FLOE_DOCKER_IMAGE" dagster dev
```

## Notes

- This connector does **not** parse YAML directly; it uses `floe validate --output json` as the source of truth.
- For local development without an installed `floe` binary, you can point `LocalRunner` to a custom command, e.g.:
  - `LocalRunner(\"cargo run -p floe-cli --\")`
- `DockerRunner` mounts the config directory by default; if your config uses paths like `../data/...`, it mounts a higher parent directory so those paths remain visible. For full control, pass `workdir=...`.
- When `DockerRunner` mounts local files, it runs the container as your current user (`--user uid:gid`) so Floe can write reports/outputs back into the mounted directory.
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
