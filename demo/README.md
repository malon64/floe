# Floe Demo Plan

### Optional: validate a bad config

```
floe validate -c configs/bad_config.yml
```

Expected: schema validation error (missing report.path).

### 1) Multi-entity ingestion (CSV + Parquet + NDJSON)

```
floe validate -c configs/multi_entity_mixed.yml
floe run -c configs/multi_entity_mixed.yml

./clean.sh
```

Expected:
- CSV + Parquet + NDJSON inputs handled
- policies: customer=reject, orders=warn, events=abort
- accepted outputs written as Parquet
- rejected outputs and JSON errors for invalid rows/files
- report written under `report.path/run_<run_id>/...`

### 2) Delta sink output

```
floe run -c configs/delta_sink.yml

./clean.sh
```

Expected:
- accepted output written as a Delta table
