# Floe x Airflow Integration Spec

## 1. Goal

Airflow consumes a static Floe manifest (`floe.manifest.v1`) and executes Floe runs without parsing Floe YAML configs directly at DAG parse-time.

The connector must:

- load topology from manifest at parse-time
- execute using manifest execution contract at run-time
- select runner from manifest runner contract
- publish normalized run payloads and asset materialization metadata

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
3. Deploy DAGs + manifest together.

## 3. Parse-time behavior in Airflow

At import time, Airflow connector:

1. loads manifest (`FloeManifestHook`)
2. builds static entity list and assets from `entities[]`
3. resolves config path/URI from `config_uri`

No Floe subprocess is invoked during DAG parse.

Fallback mode is supported:

- if manifest is missing/invalid, DAG can still run with `FLOE_CONFIG` (no assets loaded)

## 4. Run-time execution contract

The connector executes commands from `manifest.execution`, not from hardcoded CLI args.

### 4.1 Command assembly

For a run task, final command is built as:

`[entrypoint] + base_args(rendered) + per_entity_args(rendered if entities selected)`

Placeholder support:

- `{config_uri}` -> resolved config path/URI used by the task
- `{entity_name}` -> comma-joined selected entities for current task

Current requirement:

- `execution.log_format` must be `json` (connector parses NDJSON `floe.log.v1`)

### 4.2 Result parsing

Connector expects `run_finished` event from NDJSON stream and extracts summary URI from:

- `execution.result_contract.summary_uri_field`

The run payload pushed by operator remains:

- schema: `floe.airflow.run.v1`

## 5. Runner contract

Runner selection source:

- `entities[].runner` if provided
- otherwise `runners.default`

Runner lookup:

- selected runner name must exist in `runners.definitions`

### 5.1 Supported now

- `type: local_process`
  - executed by `FloeRunOperator` using local subprocess

### 5.2 Planned

- `type: kubernetes_pod` / `kubernetes_job`
  - map to Airflow Kubernetes operators
- `type: ecs_task`
  - map to Airflow ECS operator

Current fail-fast behavior:

- unsupported runner types raise explicit task error
- mixed runner names in a single `FloeRunOperator` invocation are rejected

## 6. Asset materialization

Static assets are created from manifest at parse-time.

On run completion, operator enriches outlet events with per-entity metrics using:

- `run_finished` payload
- optional `summary_uri` loaded summary file

## 7. Validate payload support

`floe.plan.v1` loader remains available as a legacy compatibility path.

When converting validate payload to `floe.manifest.v1`, connector injects default:

- `execution` contract
- `runners` contract (`local_process`)

This is a bridge path; production should prefer generated manifest files.

## 8. Error policy

Task fails when:

- manifest schema/required fields are invalid
- runner resolution fails
- unsupported runner type is selected
- NDJSON cannot be parsed
- `run_finished` event is missing

## 9. Connector modules

- `src/airflow_floe/manifest.py`
  - manifest models + loaders (`floe.manifest.v1`, legacy `floe.plan.v1` compatibility)
- `src/airflow_floe/runtime.py`
  - context build, URI/path handling, run summary loading
- `src/airflow_floe/operators.py`
  - run operator/hook, execution + runner resolution, payload emission
- `src/airflow_floe/hooks.py`
  - manifest context hook for DAG wiring

## 10. Open roadmap

1. Add Kubernetes/ECS runner implementations behind same runner contract.
2. Optional split operator classes per runner family (`Local`, `K8s`, `ECS`) while keeping one manifest contract.
3. Add schema validation in connector package before runtime use (strict local check against `floe.manifest.v1.json`).
