# Logging (Orchestrator-safe)

Floe can emit structured run events for orchestrators like Dagster/Airflow.

## Modes

### Default (no flag)

- Intended for interactive CLI use.
- Prints a human summary at the end of the run.

### `--log-format json`

- Intended for orchestrators.
- **stdout is NDJSON only**: one JSON object per line.
- **stderr contains human output** (summary, warnings, errors).

Each JSON line includes:
- `schema: "floe.log.v1"` (stable)
- `level: "info" | "warn" | "error"` (stable)
- `event: ...` plus event-specific fields

The final line on stdout is always the `run_finished` event.

### `--log-format text`

- Prints the same lifecycle events as plain text lines.
- Also prints the human end-of-run summary.

## Examples

### Orchestrator parsing (conceptual)

```bash
floe run -c config.yml --log-format json | jq -cr 'select(.event == "run_finished")'
```

### Split streams

```bash
# machine events
floe run -c config.yml --log-format json > events.ndjson

# human logs
floe run -c config.yml --log-format json 2> run.log
```

