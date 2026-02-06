# Full Configuration Example

This example is intentionally complete and commented so you can see most of what Floe supports in one place. Use it as a reference and trim it down for your real configs.

```yaml
# Floe config version
version: "0.1"

# Optional project metadata
metadata:
  project: "floe-demo"
  description: "Example config covering many features"
  owner: "data-platform"
  tags: ["demo", "floe"]

# Optional environment variables for templating
env:
  vars:
    incoming_root: "/data/incoming"
    outgoing_root: "/data/outgoing"

# Optional named domains for re-use
# Each entity can reference {{domain.incoming_dir}}
domains:
  - name: "animals"
    incoming_dir: "{{incoming_root}}/animals"
  - name: "sales"
    incoming_dir: "{{incoming_root}}/sales"

# Optional storage clients (local + cloud)
storages:
  default: local
  definitions:
    - name: local
      type: local
    - name: s3_raw
      type: s3
      bucket: my-bucket
      region: eu-west-1
      prefix: raw
    - name: gcs_raw
      type: gcs
      bucket: my-gcs-bucket
      prefix: ingest
    - name: adls_raw
      type: adls
      account: myaccount
      container: raw
      prefix: ingest

# Optional report output
report:
  path: "{{outgoing_root}}/report"
  storage: local

# Entities = datasets
entities:
  # 1) CSV input, Parquet accepted, CSV rejected (append mode)
  - name: "animal"
    domain: "animals"
    source:
      format: "csv"
      path: "{{domain.incoming_dir}}/animal"
      options:
        header: true
        separator: ";"
        null_values: ["", "NULL", "null"]
        recursive: false
        glob: "*.csv"
      cast_mode: "strict"
    sink:
      write_mode: "append"
      accepted:
        format: "parquet"
        path: "{{outgoing_root}}/accepted/animal"
        options:
          compression: "zstd"
          row_group_size: 100000
          max_size_per_file: 268435456 # 256MB
      rejected:
        format: "csv"
        path: "{{outgoing_root}}/rejected/animal"
      archive:
        path: "{{outgoing_root}}/archive/animal"
    policy:
      severity: "reject"
    schema:
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      columns:
        - name: "animal_id"
          type: "string"
          nullable: false
          unique: true
        - name: "species"
          type: "string"
        - name: "age"
          type: "int64"
          nullable: true

  # 2) NDJSON input, Delta accepted, CSV rejected (overwrite mode)
  - name: "orders"
    domain: "sales"
    source:
      format: "json"
      path: "{{domain.incoming_dir}}/orders.ndjson"
      options:
        json_mode: "ndjson"
    sink:
      write_mode: "overwrite"
      accepted:
        format: "delta"
        # Example: local path (could be s3://, gs://, abfs://)
        path: "{{outgoing_root}}/accepted/orders_delta"
      rejected:
        format: "csv"
        path: "{{outgoing_root}}/rejected/orders"
    policy:
      severity: "warn"
    schema:
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      columns:
        - name: "order_id"
          type: "string"
          nullable: false
          unique: true
        - name: "customer_id"
          type: "string"
          nullable: false
        - name: "amount"
          type: "float64"
          nullable: true

  # 3) Parquet input from cloud storage
  - name: "customer"
    source:
      format: "parquet"
      path: "customers"
      storage: "s3_raw"
    sink:
      accepted:
        format: "parquet"
        path: "{{outgoing_root}}/accepted/customer"
      rejected:
        format: "csv"
        path: "{{outgoing_root}}/rejected/customer"
    policy:
      severity: "abort"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
        - name: "email"
          type: "string"
          nullable: true

  # 4) JSON array input (not NDJSON)
  - name: "product"
    source:
      format: "json"
      path: "{{incoming_root}}/product.json"
      options:
        json_mode: "array"
    sink:
      accepted:
        format: "parquet"
        path: "{{outgoing_root}}/accepted/product"
      rejected:
        format: "csv"
        path: "{{outgoing_root}}/rejected/product"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "product_id"
          type: "string"
          nullable: false
          unique: true
        - name: "title"
          type: "string"
          nullable: false
```

Notes:
- `write_mode` is configured at `sink` and applies to both accepted and rejected outputs.
- For cloud paths, use `s3://`, `gs://`, or `abfs://` URIs and set `storage`.
- JSON must be flat objects; nested values are rejected.
- Use `normalize_columns` to align schema and input column names before checks.
