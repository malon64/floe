# Floe x Dagster Integration Spec

## 1. Goal

Dagster consumes a static Floe manifest (`floe.manifest.v1`) and executes Floe runs without parsing Floe YAML configs directly at definitions parse-time.

The connector must:

- load topology from manifest at parse-time
- execute via manifest execution contract at run-time
- resolve runner from manifest runner contract
- attach normalized run metadata to Dagster materializations

## 2. Source of truth

### 2.1 Manifest schema

- Schema id: `floe.manifest.v1`
- Canonical schema file: `orchestrators/schemas/floe.manifest.v1.json`

### 2.2 Manifest generation

Current CLI command:

```bash
floe manifest generate -c <config> --output <manifest_path>
```

Recommended deployment flow:

1. Generate manifest in CI/CD.
2. Validate manifest against JSON schema.
3. Deploy Dagster code + manifest together.

## 3. Parse-time behavior in Dagster

At definitions load time, Dagster connector:

1. loads manifest file
2. builds one asset per `entities[]` entry
3. resolves config path/URI from `config_uri` (supports `local://`, local paths, and remote URIs)

No Floe subprocess is invoked during parse-time.

Code organization:

- connector logic is packaged under `src/floe_dagster/`
- repository example wiring lives in `example/definitions.py`

## 4. Asset mapping

- 1 Floe entity = 1 Dagster asset
- key comes from `entity.asset_key`
- group comes from `entity.group_name`

This keeps asset keys stable across orchestrators.

## 4.1 Job model (production target)

Target operating model:

- 1 Floe config file per domain
- 1 generated manifest per config
- 1 Dagster job per manifest

In Dagster terms:

- assets remain entity-level (`1 entity = 1 asset`)
- one `define_asset_job(...)` is generated for each manifest/config boundary
- each job selects assets belonging to its manifest only

This gives explicit run boundaries and operational control (schedule, retries, concurrency, ownership) per domain/config.

Current state:

- single-manifest wiring already generates one job for that manifest
- multi-manifest directory loading is tracked as next phase work

## 5. Run-time execution contract

The connector executes commands from `manifest.execution`, not hardcoded CLI args.

Final command shape:

`[entrypoint] + base_args(rendered) + per_entity_args(rendered)`

Placeholders:

- `{config_uri}` -> resolved config path/URI
- `{entity_name}` -> current entity name

Current requirements:

- `execution.log_format` must be `json`
- `execution.result_contract.run_finished_event` must be true
- run stdout must contain Floe NDJSON and a final `run_finished`

## 6. Runner contract

Runner name selection:

- `entity.runner` if set
- else `runners.default`

Runner resolution:

- selected name must exist in `runners.definitions`

Supported now:

- `type: local_process`

Fail-fast behavior:

- any unsupported runner type (`docker`, `kubernetes_*`, `ecs_task`, etc.) raises explicit error

## 7. Materialization metadata

Per asset run:

1. parse NDJSON stdout
2. read last `run_finished`
3. load summary from `summary_uri` for local targets when available
4. emit `MaterializeResult(metadata=...)` including:
   - run id, status, exit code
   - files/rows/accepted/rejected/warnings/errors
   - summary URI and optional per-entity report pointer

## 8. Migration plan (validate -> manifest)

1. Replace validate-plan loader with manifest loader (`floe.manifest.v1`).
2. Build Dagster assets directly from `manifest.entities`.
3. Replace runner hardcoded args with `manifest.execution` rendering.
4. Add runner resolution (`entity.runner` / `runners.default`) and enforce `local_process` only.
5. Switch definitions example from config YAML to generated manifest JSON.
6. Update tests/fixtures to manifest-first flow.

No backward-compatibility bridge from `floe.plan.v1` is planned in Dagster connector.

## 9. Non-goals (current phase)

- No cloud summary fetch in connector runtime.
- No Dagster `AssetCheckResult` mapping from Floe checks yet.
- No multi-entity single-process optimization in this phase.

## 10. Next phase (implementation backlog)

1. Multi-manifest loading at parse-time.
   Scope: support loading a directory of manifests (`*.manifest.json`) to generate assets from multiple Floe configs/domains in one Dagster code location.
2. Generate one Dagster job per manifest/config.
   Scope: when multiple manifests are loaded, build one `define_asset_job` per manifest and expose stable job names (for schedule/sensor targeting).
3. Support execution defaults from manifest.
   Scope: apply `execution.defaults.env` and `execution.defaults.workdir` in `LocalRunner` when launching Floe subprocesses.
4. Strict manifest schema validation in connector.
   Scope: validate loaded JSON payloads against `orchestrators/schemas/floe.manifest.v1.json` before building assets or running tasks.
5. Runner adapters for non-local execution.
   Scope: keep `local_process` as baseline and add adapter structure for `kubernetes_*` and `ecs_task` runner types without changing manifest contract.
6. Remote `summary_uri` loading.
   Scope: support reading run summaries from cloud URIs (`s3://`, `gs://`, `abfs://`) to enrich Dagster metadata outside local-only mode.
7. Single Floe run for multiple selected assets.
   Scope: optional optimization mode where one Floe run materializes a selected entity set, then connector fans out metadata to corresponding assets.
