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

## Deterministic generation

For production deployments where manifests are committed to git and diffed in CI, use `--deterministic`:

```bash
floe manifest generate \
  -c sales.yml \
  -p profiles/prod.yml \
  --deterministic \
  --manifest-name sales.prod \
  --output manifests/sales.prod.json
```

In deterministic mode:

- `generated_at_ts_ms` is set to `0` — the same inputs always produce byte-identical output.
- `config_checksum` and `profile_checksum` (SHA-256) trace the exact source files used.
- `manifest_revision` (SHA-256 of canonical content) provides a stable content fingerprint.
- Map fields (`exit_codes`, `env`, `definitions`, `tags`) use stable alphabetical key ordering.

To verify a committed manifest has not drifted from the source config:

```bash
floe manifest generate -c sales.yml -p profiles/prod.yml --deterministic -o /tmp/check.json
diff manifests/sales.prod.json /tmp/check.json
```

An empty diff means the manifest is up to date.

---

## What's inside

A manifest is a self-contained JSON document. Here's an annotated excerpt:

```jsonc
{
  "schema": "floe.manifest.v1",          // version sentinel — never changes for v1
  "manifest_name": "sales.prod",         // optional stable logical name (--manifest-name)
  "manifest_id": "mfv1-a1b2c3d4...",    // FNV-1a hash of config URI + content
  "manifest_revision": "sha256:...",     // SHA-256 of canonical manifest content
  "config_uri": "./orders.yml",          // where the source YAML lives
  "config_checksum": "sha256:...",       // SHA-256 of the config file
  "profile_uri": "local:///prod.yml",   // profile used at generation time (if any)
  "profile_checksum": "sha256:...",      // SHA-256 of the profile file (if any)
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

```text
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

## Deployment URI overrides

For production deployments where the manifest is uploaded to object storage and consumed by remote runners, you need the manifest to point at its deployed locations rather than the local generation paths.

### Remote config and profile

`-c` and `-p` accept remote URIs directly — the same scheme support as `floe run`:

```bash
floe manifest generate \
  -c s3://my-code-bucket/floe/sales/sales_poc.yml \
  -p s3://my-code-bucket/floe/profiles/prod-k8s.yml \
  --output manifests/sales.manifest.json
```

The file is downloaded to a temp directory for generation. The manifest records the URI as `config_uri` and `profile_uri`, so the contract already points at the deployed source.

### `--output` accepts remote URIs — writes and embeds in one step

`--output` accepts the same remote URI schemes as `-c` and `-p`. When you write the manifest to a remote URI, that URI is also automatically baked into `execution.base_args` (replacing the `{manifest_uri}` placeholder), producing a fully self-contained contract:

```bash
floe manifest generate \
  -c sales.yml \
  --output s3://my-code-bucket/floe/sales/sales.manifest.json
```

The manifest is uploaded to S3 and its `base_args` already contain `s3://my-code-bucket/floe/sales/sales.manifest.json` — no orchestrator-side placeholder rendering needed.

Writing locally still leaves `{manifest_uri}` in `base_args` for the orchestrator connector to render at run time (the existing default behaviour).

### `--default-domain` — namespace entities without an explicit domain

If your config is a domain/data-product contract but individual entities don't repeat `domain: sales` on every entry, set a generation-time default:

```bash
floe manifest generate \
  -c sales.yml \
  --default-domain sales \
  --output manifests/sales.manifest.json
```

Entities that already have `domain:` keep their own value. Entities that don't will get `domain`, `group_name`, and the `asset_key` prefix set to the default domain.

### `--manifest-path-mode resolved-uri` — self-contained paths for remote replay

By default, `source.path` and `sink.path` in each entity reflect the original relative paths from the config. For remote manifest replay — where there is no local config directory to resolve against — you can bake in the fully resolved URIs instead:

```bash
floe manifest generate \
  -c sales.yml \
  -p profiles/prod.yml \
  --manifest-path-mode resolved-uri \
  --output manifests/sales.manifest.json
```

When `resolved=true`, `path` is set to the full resolved `uri` (e.g. `s3://data-bucket/bronze/sales/customers`). This makes the manifest a standalone runtime contract without needing the original config base directory.

### Full production workflow

```bash
floe manifest generate \
  -c domains/sales/sales_poc.yml \
  -p domains/sales/profiles/prod-k8s.yml \
  --manifest-uri s3://my-code-bucket/floe/sales/sales.manifest.json \
  --default-domain sales \
  --manifest-path-mode resolved-uri \
  --deterministic \
  --manifest-name sales.prod \
  --output domains/sales/manifests/sales.manifest.json
```

This produces a manifest where:

- `config_uri` / `profile_uri` reflect the local paths (or remote URIs if you pass `-c s3://...`)
- `execution.base_args` contains the literal deployed manifest URI
- All entities are namespaced under `sales`
- All source/sink `path` fields contain fully resolved URIs
- The output is deterministic and byte-identical across CI runs

---

## Keeping manifests fresh

Regenerate the manifest whenever you change:

- entity names, domains, or schema
- source or sink paths
- runner definitions or execution args

With `--deterministic`, drift detection is a clean diff:

```bash
floe manifest generate -c orders.yml --deterministic -o /tmp/fresh.json
diff manifests/orders.json /tmp/fresh.json
```

If you are not using `--deterministic`, strip the volatile timestamp before comparing:

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
