# Examples

## Local CSV to Parquet

```bash
floe run -c example/config.yml --entities customer
```

Expected outputs (default example paths):

- Accepted: `example/out/accepted/customer/part-*.parquet`
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

## Delta accepted output

```yaml
sink:
  accepted:
    format: "delta"
    path: "s3://my-bucket/floe/accepted/customer"
```

See `docs/config.md` for full reference.
