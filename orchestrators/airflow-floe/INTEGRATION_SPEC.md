# Floe x Airflow Integration Spec (MVP)

## 1. Goal

Provide a stable Airflow integration layer that:
- uses Floe CLI as the single source of truth for planning and execution
- exposes compact, versioned payloads to XCom
- remains decoupled from Floe internal report file growth

This document defines the execution contract and payload schemas for Airflow.

## 2. Scope

In scope:
- run orchestration and asset materialization contract
- manifest-driven parse-time topology loading
- local and docker runner behavior
- NDJSON parsing contract
- XCom payload schema and versioning policy
- task success/failure mapping
- validate contract usage in control-plane/CI

Out of scope for MVP:
- cloud summary fetch helpers inside Airflow package
- scheduler/sensor design
- in-task dedup or incremental semantics

## 3. Upstream Floe Contracts (consumed by Airflow)

### 3.1 Validate command

Command:

```bash
floe validate -c <config_path> --output json
```

Contract:
- output is a single JSON document
- JSON includes `schema: "floe.plan.v1"`
- `valid` indicates validation result
- when invalid, `errors` is present

### 3.2 Run command

Command:

```bash
floe run -c <config_path> [--entities <entity>] --log-format json
```

Contract:
- stdout contains NDJSON lines only
- each JSON line contains `schema: "floe.log.v1"`
- terminal event must include `event: "run_finished"`
- `run_finished` includes:
  - `run_id`
  - `status`
  - `exit_code`
  - `summary_uri` (optional)
  - totals (`files`, `rows`, `accepted`, `rejected`, `warnings`, `errors`)

If NDJSON is malformed or no `run_finished` exists, the Airflow task fails.

### 3.3 Manifest command (next Floe contract)

Target command (to implement in Floe):

```bash
floe plan -c <config_path> --format airflow --output-path <manifest_path>
```

Target purpose:
- pre-parse config outside Airflow runtime
- generate a static manifest consumed by DAG import
- avoid running Floe subprocess in DAG parse phase

Interim implementation status:
- `orchestrators/airflow-floe/dags/floe_manifest.py` can already convert
  `floe validate --output json` payloads (`floe.plan.v1`) into the manifest model.
- Example DAGs now load `floe.airflow.manifest.v1` at parse time and publish
  per-entity asset metadata on run completion.

Target manifest schema (proposal): `floe.airflow.manifest.v1`

Minimum fields:
- `schema`: `floe.airflow.manifest.v1`
- `generated_at_ts_ms`
- `floe_version`
- `config_uri`
- `config_checksum` (or equivalent fingerprint)
- `entities[]` with:
  - `name`
  - `domain` (optional)
  - `group_name` (optional)
  - `source_format`
  - `accepted_sink_uri`
  - `rejected_sink_uri` (optional)
  - `asset_key` (or deterministic derivation inputs)

## 4. Airflow Execution Model

Default model (recommended):
1. **deploy/control-plane step**: generate manifest (`floe plan --format airflow`)
2. DAG import reads manifest only (static entity/asset definitions)
3. `FloeRunOperator`: run full config once
4. optional downstream tasks consume `summary_uri` and publish materializations

Optional model (advanced):
1. extract selected entities from params/manifest
2. dynamically map one run task per entity
3. aggregate outcomes downstream

Execution contract per task:
1. call runner (`local` or `docker`) to execute Floe CLI
2. capture stdout/stderr and process exit code
3. parse contract payload (run NDJSON)
4. build adapter payload (`floe.airflow.*.v1`)
5. push adapter payload to XCom
6. map task status using rules in section 6

Important constraints:
- do not call `floe validate` during DAG file import
- runtime DAG examples are run-only (`floe run`)
- do not mutate asset definitions at runtime
- publish asset materialization results at runtime from run outputs

Control-plane (CI/CD) recommendation:
- run `floe validate --output json` as a pre-deploy gate
- run `floe plan --format airflow --output-path ...` to generate manifest artifacts
- deploy DAGs with static manifest references

## 5. Adapter Payload Schemas (XCom)

Airflow must push only adapter payloads to XCom, not full report files.

### 5.1 Validate payload

Schema id: `floe.airflow.validate.v1`

JSON schema file:
- `orchestrators/airflow-floe/schemas/floe.airflow.validate.v1.json`

Required fields:
- `schema`
- `config_uri`
- `valid`
- `error_count`
- `warning_count`
- `floe_schema`
- `generated_at_ts_ms`

Optional fields:
- `errors` (trimmed list)
- `warnings` (trimmed list)
- `entity_count`
- `selected_entities`

### 5.2 Run payload

Schema id: `floe.airflow.run.v1`

JSON schema file:
- `orchestrators/airflow-floe/schemas/floe.airflow.run.v1.json`

Required fields:
- `schema`
- `run_id`
- `status`
- `exit_code`
- `files`
- `rows`
- `accepted`
- `rejected`
- `warnings`
- `errors`
- `floe_log_schema`
- `finished_at_ts_ms`

Optional fields:
- `summary_uri`
- `entity`
- `config_uri`

## 6. Task Status Mapping

### 6.1 Validate task

- used in control-plane/CI (not required in runtime DAGs)
- success when command exits 0 and payload parses
- failure when command fails, output is not valid JSON, or schema is unexpected

Note: `valid=false` can be treated as a failed pipeline gate in CI policy.

### 6.2 Run task

- success when:
  - process exits 0
  - NDJSON parsing succeeds
  - `run_finished` exists
  - `run_finished.exit_code == 0`
- failure otherwise

## 7. Versioning and Compatibility

- Airflow adapter payloads are versioned independently:
  - `floe.airflow.validate.v1`
  - `floe.airflow.run.v1`
- Floe upstream schemas (`floe.plan.v1`, `floe.log.v1`) are treated as dependencies.
- Unknown optional fields from Floe are ignored.
- Missing required fields fail fast with a clear contract error.

## 8. Security and Size Constraints

- no credentials in payload
- no full run report JSON in XCom
- no raw NDJSON stream in XCom
- optional issue lists (`errors`, `warnings`) should be truncated by adapter policy

## 9. Implementation Checklist

1. create Python models for both payload schemas
2. implement command runners (`LocalRunner`, `DockerRunner`)
3. implement parse/validate for run NDJSON contract
4. keep validate JSON support for control-plane workflows
5. implement XCom push utilities
6. add unit tests for parser and schema validation
7. add integration tests for local and docker happy paths

## 11. Decisions (2026-02-13)

1. Airflow default connector model is DAG/operator-oriented, not asset-first execution.
2. Asset visibility is supported, but execution remains centered on Floe run tasks.
3. Config parsing authority stays in Floe, not in Python connector code.
4. DAG import should consume a static manifest, not invoke Floe CLI directly.
5. Runtime should publish materialization outcomes for entities actually processed.
6. Dynamic entity subsets are supported at runtime, but asset definitions remain static for the deployed DAG version.
7. Runtime DAGs are run-only; `validate` belongs to CI/control-plane.

## 12. Post-Manifest Connector Roadmap

Once `floe.airflow.manifest.v1` is available, connector work is:

1. Manifest loader module
- read/validate `floe.airflow.manifest.v1`
- fail fast on schema mismatch

2. DAG factory from manifest
- build default simple DAG from static manifest
- optional entity-mapped DAG from same manifest

3. Operators
- `FloeRunOperator` (full config or selected entities)
- `FloePublishAssetsOperator` (publish per-entity materialization metadata from summary)
- optional control-plane utility: `FloeValidateOperator` (CI-oriented usage)

4. Runtime parsing
- NDJSON parser for `floe.log.v1`
- summary loader from `summary_uri` (local first, cloud later)

## 13. Current connector status

Implemented in this repo:
- `FloeManifestHook` to load manifest context and assets (with no-manifest fallback context)
- `FloeRunHook` + `FloeRunOperator` for run execution and normalized `floe.airflow.run.v1` payloads
- local summary URI support for `file://...` and `local://...`
- dedicated Airflow connector CI workflow (`.github/workflows/airflow-floe.yml`)

5. Asset publication model
- static definitions from manifest
- runtime materialization events only for executed entities

6. Testing
- unit: manifest schema validation, parser, payload builders
- integration: simple DAG end-to-end using manifest
- integration: entity subset run + correct materialization publication

## 10. Example Payloads

Validate payload example:

```json
{
  "schema": "floe.airflow.validate.v1",
  "config_uri": "./example/config.yml",
  "valid": true,
  "error_count": 0,
  "warning_count": 0,
  "entity_count": 2,
  "selected_entities": ["customer"],
  "floe_schema": "floe.plan.v1",
  "generated_at_ts_ms": 1739500000000
}
```

Run payload example:

```json
{
  "schema": "floe.airflow.run.v1",
  "run_id": "2026-02-13T10-00-00Z",
  "entity": "customer",
  "status": "success",
  "exit_code": 0,
  "files": 3,
  "rows": 1200,
  "accepted": 1188,
  "rejected": 12,
  "warnings": 1,
  "errors": 0,
  "summary_uri": "./report/run_2026-02-13T10-00-00Z/run.summary.json",
  "config_uri": "./example/config.yml",
  "floe_log_schema": "floe.log.v1",
  "finished_at_ts_ms": 1739500005000
}
```
