# Floe Demo Plan

This demo plan showcases the core features recently implemented in v0.1.x.
It is designed for a live walkthrough or a self-guided run. S3 is optional and
not required for the core demo.

## Goals

- Validate good and bad configs with clear error messages.
- Run single-entity ingestion on one file.
- Run multi-entity ingestion with mixed formats.
- Demonstrate policies: warn, reject, abort.
- Show checks: not_null, unique, cast_error, schema mismatch.
- Show accepted/rejected outputs and JSON reports.
- Show Delta sink output (accepted).

## Proposed demo assets (to add under `demo/`)

```
demo/
  configs/
    bad_config.yml
    single_csv_warn.yml
    single_csv_reject.yml
    single_csv_abort.yml
    multi_entity_mixed.yml
    delta_sink.yml
  in/
    customer/
      customers_valid.csv
      customers_invalid.csv
    orders/
      orders_valid.parquet
      orders_invalid.parquet
    events/
      events_valid.ndjson
      events_invalid.ndjson
  out/
    accepted/
      customer/.gitkeep
      orders/.gitkeep
      events/.gitkeep
    rejected/
      customer/.gitkeep
      orders/.gitkeep
      events/.gitkeep
  report/
    .gitkeep
```

Suggested data issues to cover:
- missing not_null field
- duplicate key for unique
- invalid datetime / type mismatch
- extra column (schema mismatch)
- missing column (schema mismatch)

## Demo script

### 1) Validate a bad config

```
floe validate -c demo/configs/bad_config.yml
```

Expected: schema validation error (unknown field / invalid format / missing path).

### 2) Validate a good config (single CSV)

```
floe validate -c demo/configs/single_csv_warn.yml
```

Expected: success output and entity summary.

### 3) Run single-entity CSV (warn)

```
floe run -c demo/configs/single_csv_warn.yml
```

Expected:
- accepted output written
- warnings reported in run report
- no rejected file

### 4) Run single-entity CSV (reject)

```
floe run -c demo/configs/single_csv_reject.yml
```

Expected:
- accepted + rejected outputs
- rejected rows include `__floe_row_index` and `__floe_errors`

### 5) Run single-entity CSV (abort)

```
floe run -c demo/configs/single_csv_abort.yml
```

Expected:
- full file rejected
- reject errors JSON produced

### 6) Run multi-entity mixed formats

```
floe run -c demo/configs/multi_entity_mixed.yml
```

Expected:
- CSV + Parquet + NDJSON inputs handled
- separate accepted/rejected outputs per entity
- report written under `report.path/run_<run_id>/...`

### 7) Run Delta sink example

```
floe run -c demo/configs/delta_sink.yml
```

Expected:
- accepted output written as Delta table

## Notes

- S3 can be added as an optional appendix: define a filesystem block and swap
  input/output paths to S3, but keep the core demo local for simplicity.
- Keep outputs clean by deleting `demo/out/` and `demo/report/` between runs.
