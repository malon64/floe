# FAQ

## Does Floe support append mode?

Yes. Configure `sink.write_mode: "append"` to add new dataset parts for accepted and rejected outputs.

## What formats are supported?

Inputs: CSV, JSON (array or NDJSON), Parquet.
Outputs: accepted Parquet or Delta, rejected CSV, JSON reports.

## Does Floe support cloud storage?

Yes. S3, ADLS, and GCS are supported. See `docs/support-matrix.md`.

## How do I validate configs in CI?

Use:

```bash
floe validate -c config.yml --output json
```

The command exits non-zero on errors and outputs a single JSON object.

## Where can I find the full reference?

- `docs/config.md`
- `docs/cli.md`
- `docs/how-it-works.md`
