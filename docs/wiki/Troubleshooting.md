# Troubleshooting

## Installation issues

- Homebrew not available: use Cargo install instead.
- Cargo build fails: ensure a recent Rust toolchain and a system C compiler.

## Config validation errors

- Run: `floe validate -c <config> --output json` to get machine-readable errors.
- Check `docs/config.md` for required fields.

## No outputs written

- Confirm `report.path` and `sink` paths are writeable.
- For Docker, make sure your host path is mounted under `/work`.

## Cloud storage

- Verify storage definitions under `storages`.
- Ensure credentials are present for the target cloud.
- Cloud inputs are resolved by prefix listing (no globs).

## Schema mismatch

- If `normalize_columns` is enabled, schema and input columns are normalized before checks.
- Name collisions from normalization cause the run to fail.
