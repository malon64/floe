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

## 10. Next phase

1. Add optional multi-manifest directory loading (`*.manifest.json`) for multi-config deployments.
2. Add pluggable runner adapters (Kubernetes/ECS) under same manifest contract.
3. Add summary loading for remote URIs (`s3://`, `gs://`, `abfs://`).
