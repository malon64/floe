# OpenLineage Integration

Floe can emit run and entity lifecycle events to any
[OpenLineage](https://openlineage.io)-compatible HTTP endpoint (Marquez,
Atlan, OpenMetadata, Astronomer, etc.).

## Configuration

Add a `lineage` block at the root of your config file:

```yaml
lineage:
  url: "http://marquez:5000"
  namespace: "my-floe-namespace"
  api_key: "{{OPENLINEAGE_API_KEY}}"   # optional — Bearer token
  timeout_secs: 5                       # optional, default 5
  producer: "https://github.com/myorg/floe"  # optional
```

| Field          | Required | Description                                                          |
|----------------|----------|----------------------------------------------------------------------|
| `url`          | yes      | Base URL of the OpenLineage-compatible endpoint                      |
| `namespace`    | yes      | OpenLineage namespace used for all jobs and datasets in this run     |
| `api_key`      | no       | Bearer token sent in the `Authorization` header                      |
| `timeout_secs` | no       | HTTP request timeout in seconds (default: `5`)                       |
| `producer`     | no       | URI identifying this producer (default: the Floe GitHub URL)        |

`api_key` supports `{{VAR}}` placeholder expansion via the same profile and
env-vars mechanism used for the rest of the config.

## Events emitted

For each Floe run, Floe posts OpenLineage `RunEvent` objects to
`POST <url>/api/v1/lineage`:

| Floe lifecycle       | OpenLineage event type | Notes                                                   |
|----------------------|------------------------|---------------------------------------------------------|
| Run begins           | `START`                | Top-level run job                                       |
| Entity begins        | `START`                | Per-entity job `<namespace>.<entity>`                   |
| Entity finishes (ok) | `COMPLETE`             | Includes `DataQualityMetrics`, `FloeQualityRun`, `SchemaDataset` facets |
| Entity finishes (err)| `FAIL`                 | When entity status is `failed` or `aborted`             |
| Run finishes (ok)    | `COMPLETE`             | Top-level run job                                       |
| Run finishes (err)   | `FAIL`                 | Top-level run job                                       |

### Facets on entity COMPLETE events

- **`DataQualityMetrics`** (`dataQualityMetrics`) — `rowCount`, `validCount`, `invalidCount`
- **`FloeQualityRun`** — `entity`, `rejectionRate`, `files`, `rows`, `accepted`, `rejected`, `warnings`, `errors`
- **`SchemaDataset`** — column names and types from `schema.columns`

### ParentRun facet (Airflow / Dagster)

When Floe is invoked from an Airflow task or Dagster job, the parent run
context is auto-detected from environment variables and attached to every
top-level run event:

| Orchestrator | Environment variables read                                                    |
|--------------|-------------------------------------------------------------------------------|
| Airflow      | `AIRFLOW_CTX_DAG_RUN_ID`, `AIRFLOW_CTX_DAG_ID`, `AIRFLOW_CTX_TASK_ID`       |
| Dagster      | `DAGSTER_RUN_ID`, `DAGSTER_JOB_NAME`                                          |

## Fail-silent behaviour

HTTP errors (connection failures, 4xx/5xx responses) are emitted as `warn`-level
log events and do not affect the run outcome. The ingestion continues normally.

Warnings are always surfaced to stderr even when `--log-format off` is used,
so misconfigured endpoints are always visible in operator logs.

## Lifecycle correctness

Floe ensures every run produces a well-formed OpenLineage lifecycle:

- A `START` event is always emitted before any `COMPLETE` or `FAIL` event for
  both the top-level run and each entity job.
- If `run_with_base` fails during config validation or context construction
  (before execution begins), a `START` event is emitted in the failure path so
  consumers never see a bare `FAIL` without a prior `START`.
- If `run_with_base` fails after the run has already started (e.g. during
  input resolution), the existing `START` is not re-emitted.

## Log format and lineage

Lineage emission is independent of `--log-format`. Even with
`--log-format off` (the default), OpenLineage events are posted normally.

## Example config

```yaml
version: "0.3"
lineage:
  url: "http://localhost:5000"
  namespace: "data-platform"
  api_key: "{{MARQUEZ_API_KEY}}"
  timeout_secs: 10

entities:
  - name: orders
    source:
      format: csv
      path: /data/orders/
    sink:
      accepted:
        format: parquet
        path: /lake/orders/
      rejected:
        format: csv
        path: /lake/orders/rejected/
    policy:
      severity: reject
    schema:
      columns:
        - name: order_id
          type: string
        - name: amount
          type: number
```
