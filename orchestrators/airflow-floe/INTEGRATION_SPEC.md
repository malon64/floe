# Floe x Airflow Integration Spec (MVP)

## 1. Goal

Provide a stable Airflow integration layer that:
- uses Floe CLI as the single source of truth for planning and execution
- exposes compact, versioned payloads to XCom
- remains decoupled from Floe internal report file growth

This document defines the execution contract and payload schemas for Airflow.

## 2. Scope

In scope:
- validate and run orchestration contract
- local and docker runner behavior
- NDJSON parsing contract
- XCom payload schema and versioning policy
- task success/failure mapping

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

## 4. Airflow Execution Model

For each task:
1. call runner (`local` or `docker`) to execute Floe CLI
2. capture stdout/stderr and process exit code
3. parse contract payload (`validate` JSON or run NDJSON)
4. build adapter payload (`floe.airflow.*.v1`)
5. push adapter payload to XCom
6. map task status using rules in section 6

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

- success when command exits 0 and payload parses
- failure when command fails, output is not valid JSON, or schema is unexpected

Note: `valid=false` is still a successful task execution if the command contract is respected; downstream DAG logic decides whether to stop.

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
3. implement parse/validate for validate JSON contract
4. implement parse/validate for run NDJSON contract
5. implement XCom push utilities
6. add unit tests for parser and schema validation
7. add integration tests for local and docker happy paths

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
