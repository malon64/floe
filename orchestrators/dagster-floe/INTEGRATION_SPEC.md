# Floe × Dagster integration (spec, MVP → future)

This document describes how the `orchestrators/dagster-floe` connector is intended to integrate Floe into Dagster.

## Goals

- **One Floe config → many Dagster assets** (one asset per Floe entity).
- **No YAML parsing in Python**: Dagster must use Floe as the source of truth:
  - `floe validate --output json` produces the machine-readable plan.
  - `floe run --log-format json` emits machine-readable NDJSON events.
- **Orchestrator-friendly metadata**: attach per-entity counts and report locations to Dagster materializations.

## Asset mapping

- **1 Floe entity = 1 Dagster asset**
- Domain support (optional):
  - if `entity.domain` is set: asset key = `[domain, entity_name]`, group = `domain`
  - else: asset key = `[entity_name]`, group = `"floe"`

## Execution model (MVP)

Each entity asset runs Floe for that entity:

- validate (plan): `floe validate -c <config> --output json`
- run (events): `floe run -c <config> --entities <entity> --log-format json`
  - NDJSON events must be on **stdout** only
  - human summary must be on **stderr** (Dagster will log it as normal text)
  - the last event must be `run_finished` and contain `summary_uri`

After the process exits:

1) parse NDJSON from stdout
2) find the last `run_finished`
3) read `run.summary.json` from `summary_uri` (local-only in MVP)
4) emit `MaterializeResult(metadata=...)` with:
   - run id, status, exit_code
   - files/rows/accepted/rejected/warnings/errors (best-effort per entity)
   - summary uri and per-entity report file path

## Runners

The connector supports two runners:

- `LocalRunner`: calls a local `floe` binary via `subprocess`
  - can be configured with multi-token commands (e.g. `cargo run -p floe-cli --`)
- `DockerRunner`: calls `docker run --rm <image> floe ...`
  - intended for future use with `ghcr.io/<owner>/floe:<tag>`

## Non-goals (MVP)

- No cloud credentials management in Python.
- No Dagster Checks mapping from Floe validation rules.
- No sensors/schedules/resources.
- No “single Floe run for multiple assets” orchestration (each asset calls Floe today).

## Future work

- **Batch execution**: one Dagster run materializing multiple assets should be able to trigger a *single* Floe run and then fan-out results.
- **Dagster checks**: map Floe check results (cast/not_null/unique/mismatch) into Dagster `AssetCheckResult`.
- **Cloud summary loading**: support reading `summary_uri` from `s3://` / `gs://` / `abfs://` using cloud SDKs or Floe helpers.
- **Config-as-asset**: represent the config itself as a Dagster asset and generate downstream entity assets from it.
