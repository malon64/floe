# Manifest Generation

## Why manifests?

Orchestrators like Dagster and Airflow build their job/DAG graphs *before* any data runs — at parse time. If they read your Floe YAML directly at that moment, they'd have to understand Floe's config format, resolve `{{variables}}`, infer entity names, and guess output paths — all before a single row moves. That's a lot of coupling for a scheduling layer.

**Manifests decouple "what will run" from "when it runs."** You generate a manifest once (after editing your config), commit it alongside your code, and the orchestrator reads the static JSON at parse time. At run time it just calls `floe run --manifest` — no YAML parsing, no variable resolution, no guesswork.

Think of it as: **"resolve everything now so the orchestrator can be dumb at runtime."**

---

## Generating a manifest

```bash
floe manifest generate -c orders.yml --output manifests/orders.json
```

Pipe to stdout for use in CI scripts:

```bash
floe manifest generate -c orders.yml -o - | jq '.entities[].name'
```

If your config uses profile variables (cloud paths, credentials), pass the profile at generation time to bake them in:

```bash
floe manifest generate -c orders.yml -p prod.yml --output manifests/orders.json
```

Only need a subset of entities? Use `--entities`:

```bash
floe manifest generate -c orders.yml --entities orders,returns --output manifests/orders.json
```

---

## What's inside

A manifest is a self-contained JSON document. Here's an annotated excerpt:

```jsonc
{
  "schema": "floe.manifest.v1",          // version sentinel — never changes for v1
  "manifest_id": "orders-2024",          // stable ID for tracking
  "config_uri": "./orders.yml",          // where the source YAML lives
  "report_base_uri": "./report",         // where run reports are written

  // The exact CLI command the orchestrator must call
  "execution": {
    "entrypoint": "floe",
    "base_args": ["run", "--manifest", "{manifest_uri}", "--log-format", "json", "--quiet"],
    "per_entity_args": ["--entities", "{entity_name}"],
    "log_format": "json",
    "result_contract": {
      "exit_codes": {
        "0": "success_or_rejected",  // data ran; some rows may be in the rejected sink
        "1": "technical_failure",    // floe crashed or config error
        "2": "aborted"               // pipeline aborted by policy
      }
    }
  },

  // Runner definitions (local, kubernetes, databricks, etc.)
  "runners": {
    "default": "local",
    "definitions": {
      "local": { "type": "local_process" }
    }
  },

  // One entry per entity — this is what orchestrators iterate over
  "entities": [
    {
      "name": "orders",
      "domain": "sales",
      "asset_key": ["sales", "orders"],      // Dagster asset key
      "group_name": "sales",                 // Dagster asset group
      "source_format": "csv",
      "accepted_sink_uri": "./out/accepted/orders",
      "rejected_sink_uri": "./out/rejected/orders",
      "schema": {
        "columns": [
          { "name": "order_id", "type": "string", "nullable": false },
          { "name": "amount",   "type": "float64", "nullable": true }
        ]
      }
      // ... source/sinks/policy/pii details also present
    }
  ]
}
```

The full JSON Schema for the manifest format is at [`orchestrators/schemas/floe.manifest.v1.json`](../orchestrators/schemas/floe.manifest.v1.json).

---

## The orchestrator flow

```
config.yml ──[floe manifest generate]──► orders.json
                                              │
               ┌──────────────────────────────┘
               │  Parse-time (DAG/job construction)
               ▼
       Orchestrator reads manifest
       → registers one asset / task per entity
       → knows output paths, schema, runner config
               │
               │  Run-time (when triggered)
               ▼
       For each entity, orchestrator calls:
         floe run --manifest orders.json \
                  --entities orders \
                  --run-id $RUN_ID \
                  --log-format json
               │
               ▼
       Orchestrator parses JSON logs (NDJSON on stdout)
       → finds "run_finished" event → extracts summary_uri
       → loads run summary → publishes asset metadata
```

The `{manifest_uri}`, `{entity_name}`, and `{run_id}` placeholders in `execution.base_args` and `per_entity_args` are rendered at run time by the orchestrator connector, not by floe itself.

---

## Keeping manifests fresh

Regenerate the manifest whenever you change:

- entity names, domains, or schema
- source or sink paths
- runner definitions or execution args

A simple CI check that catches drift. Strip `generated_at_ts_ms` before comparing — it changes on every `generate` call regardless of config changes:

```bash
diff \
  <(jq -S 'del(.generated_at_ts_ms)' manifests/orders.json) \
  <(floe manifest generate -c orders.yml -o - | jq -S 'del(.generated_at_ts_ms)')
```

If the diff is non-empty, the committed manifest is stale. Commit manifests in the same PR as config changes so they're always in sync.

---

## See also

- [CLI reference — `floe manifest generate`](cli.md)
- [Dagster connector](../orchestrators/dagster-floe/README.md)
- [Airflow connector](../orchestrators/airflow-floe/README.md)
- [Full manifest JSON Schema](../orchestrators/schemas/floe.manifest.v1.json)
