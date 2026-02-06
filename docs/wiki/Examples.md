# Examples

## Local CSV to Delta (example config)

```bash
floe run -c example/config.yml --entities customer
```

Expected outputs (default example config paths):

- Accepted: `example/out/accepted/customer_delta/` (Delta table with `_delta_log` and part files)
- Rejected: `example/out/rejected/customer/part-*.csv`
- Reports: `example/report/run_<run_id>/...`

## JSON NDJSON input

```yaml
source:
  format: "json"
  path: "/data/in/customer.ndjson"
  options:
    json_mode: "ndjson"
```

## Parquet input

```yaml
source:
  format: "parquet"
  path: "/data/in/customer"
```

## Local CSV to Parquet (custom config)

If you want Parquet output, set `sink.accepted.format: parquet` in your config:

```yaml
sink:
  accepted:
    format: "parquet"
    path: "./out/accepted/customer/"
```

## Delta accepted output

```yaml
sink:
  accepted:
    format: "delta"
    path: "s3://my-bucket/floe/accepted/customer"
```

See `docs/config.md` for full reference.
