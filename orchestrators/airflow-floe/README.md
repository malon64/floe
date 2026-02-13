# Floe + Airflow (MVP example)

This folder contains an Airflow-oriented integration spec and a minimal runnable DAG example.

## Contents

- `INTEGRATION_SPEC.md`: contract between Airflow and Floe CLI
- `schemas/`: JSON Schema for XCom payloads
- `example/config.yml`: small Floe config for demo
- `dags/floe_example_dag.py`: demo DAG that:
  - validates config with `floe validate --output json`
  - extracts entity list from validate payload
  - runs one mapped task per entity with `floe run --entities <name> --log-format json`

## Quick usage

1. Make sure `floe` is available in PATH (or set `FLOE_CMD`).
2. Point Airflow DAGs folder to `orchestrators/airflow-floe/dags`.
3. Set optional env vars:

```bash
export FLOE_CMD="floe"
export FLOE_CONFIG="/absolute/path/to/orchestrators/airflow-floe/example/config.yml"
```

4. Trigger DAG `floe_example`.

## Notes

- The DAG uses Floe CLI contracts directly and expects:
  - validate schema: `floe.plan.v1`
  - run log schema: `floe.log.v1`
  - terminal event: `run_finished`
- The returned task payload shape follows `floe.airflow.run.v1`.
