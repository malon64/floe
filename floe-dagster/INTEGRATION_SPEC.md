# Floe ↔ Dagster Integration Spec (Domain + Entity Assets + Checks)

## Goal
Expose each Floe entity as a Dagster asset, surface Floe data‑quality results as Dagster asset checks, and attach per‑entity summary metrics (accepted/rejected, rows, files, status) into Dagster metadata.

Treat the **config file as an asset group (domain)** that groups entity assets in the UI. Entity assets run individually with `--entities`.

---

## Scope

### In scope
- Parse a Floe config and generate:
  - one **asset group** (domain) that contains entity assets
  - one **entity asset** per entity
- Execute Floe for entity assets only (single entity) using `--entities <name>`.
- Emit Dagster **AssetCheck** results based on Floe report details (per entity).
- Attach **per‑entity summary metadata** to each entity asset materialization.
- Support `--log-format json` events to enrich Dagster logs.

### Out of scope (v1)
- Remote report storage in cloud (skip for now).
- Dagster I/O managers for Floe outputs.

---

## Desired Dagster UX

Asset group:
- `floe__<domain>` (group name derived from config)

Assets:
- `floe__customer`
- `floe__orders`
- (one per entity)

Checks per entity asset:
- `floe_schema_mismatch`
- `floe_cast_errors`
- `floe_not_null`
- `floe_unique`
- `floe_policy` (abort/reject/warn semantics)

Metadata per entity asset:
- `rows_total`
- `accepted_total`
- `rejected_total`
- `files_total`
- `status`
- `report_uri`
- `run_id`

---

## Architecture Overview

### 1) Config parser → Dagster assets
Implement a parser that reads Floe config YAML and yields:
- domain info (used as `group_name`)
- entity definitions (name, source, sink, policy, schema)

This feeds a **dynamic asset factory**:
```python
@asset(name=f"floe__{entity.name}")
def floe_entity_asset(context):
    run_floe(entity.name)
    parse_entity_report()
    yield AssetCheckResult(...)
    context.add_output_metadata(...)
```

### 2) Execute Floe per entity asset
```
floe run -c <config> --entities <entity_name> --log-format json
```

Capture:
- stdout → NDJSON events (optional)
- exit code → Dagster Failure vs Success
- report JSON → per‑entity metrics

### 3) Parse report for metrics and checks
Floe report contains:
- file list + per‑file status
- entity totals
- error counts
- warning counts

Map this into:
- `AssetMaterialization` metadata
- `AssetCheckResult` for constraints (unique, not_null, etc.)

---

## Event Mapping (Floe → Dagster)

| Floe Event | Dagster Mapping | Notes |
|-----------|------------------|------|
| run_start | log.info | includes run_id, config, report_base |
| entity_start | log.info | used to correlate output |
| file_start | log.debug | optional |
| file_end | log.info | may include status and counts |
| entity_end | summary in metadata | key metrics |
| run_end | log.info | exit code |

---

## Asset Checks Proposal

Based on report fields:

1) **Schema mismatch**
- Fail if any file status = rejected/aborted due to schema mismatch.

2) **Cast errors**
- Fail if `cast_errors > 0`.

3) **Not null**
- Fail if `not_null_errors > 0`.

4) **Unique**
- Fail if unique violations > 0.

5) **Policy**
- Fail if entity status != success.

Note: Check names should be stable and namespace‑prefixed (`floe_`).

---

## Metadata Proposal

Attach to entity materialization:
- `run_id`
- `entity`
- `status`
- `files_total`
- `rows_total`
- `accepted_total`
- `rejected_total`
- `warnings`
- `errors`
- `report_uri`

---

## Implementation Plan

### Phase 1 — Minimal integration
- Implement config parser for entity list
- Generate assets from config (group + entities)
- Run Floe per entity asset
- Attach summary metadata

### Phase 2 — Checks + richer logs
- Parse report for constraint metrics
- Emit Dagster asset checks
- Emit structured logs from NDJSON

### Phase 3 — Quality of life
- Provide config resource + validation
- Add CLI wrapper
- Provide example Dagster project

---

## Files / Modules

Suggested Python package structure:
```
floe_dagster/
  __init__.py
  config.py        # parse floe config → domain + entities
  runner.py        # run floe per entity
  reports.py       # parse run.json
  assets.py        # asset factory
  checks.py        # map report → AssetCheckResult
```

---

## Testing
- Unit tests for config parsing
- Unit tests for report parsing
- Integration test (optional) that shells out to Floe using example config

---

## Open Questions
- Should entity assets share a single run when materializing multiple assets in one job?
- How to handle report storage in cloud (explicitly skipped for v1)?
