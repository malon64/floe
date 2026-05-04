use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use apache_avro::types::{Record, Value};
use apache_avro::{Schema, Writer};
use floe_core::report::FileStatus;
use floe_core::{run, RunOptions};
use polars::prelude::{ParquetReader, SerReader};

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.push(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_avro(dir: &Path, name: &str, schema: &Schema, build: impl FnOnce(&mut Writer<Vec<u8>>)) {
    let path = dir.join(name);
    let mut writer = Writer::new(schema, Vec::new());
    build(&mut writer);
    let encoded = writer.into_inner().expect("encode avro");
    fs::write(&path, encoded).expect("write avro");
}

fn write_avro_values(dir: &Path, name: &str, schema: &Schema, values: Vec<Value>) {
    write_avro(dir, name, schema, |writer| {
        for value in values {
            writer.append(value).expect("append");
        }
    });
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

fn run_config(path: &Path) -> floe_core::RunOutcome {
    run(
        path,
        RunOptions {
            profile: None,
            run_id: Some("test-run".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config")
}

#[test]
fn avro_ingestion_reads_records() {
    let root = temp_dir("floe-avro-ingest");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let schema = Schema::parse_str(
        r#"{
          "type": "record",
          "name": "customer",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"}
          ]
        }"#,
    )
    .expect("schema");

    write_avro(&input_dir, "input.avro", &schema, |writer| {
        let mut record = Record::new(&schema).expect("record");
        record.put("id", "1");
        record.put("name", "alice");
        writer.append(record).expect("append");

        let mut record = Record::new(&schema).expect("record");
        record.put("id", "2");
        record.put("name", "bob");
        writer.append(record).expect("append");
    });

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "avro"
      path: "{input_dir}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let _outcome = run_config(&config_path);
    let output_path = accepted_dir.join("part-00000.parquet");
    assert!(output_path.exists(), "expected parquet output");

    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    assert_eq!(df.height(), 2);
}

#[test]
fn avro_cast_errors_reject_rows() {
    let root = temp_dir("floe-avro-cast");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let schema = Schema::parse_str(
        r#"{
          "type": "record",
          "name": "payment",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "amount", "type": "string"}
          ]
        }"#,
    )
    .expect("schema");

    write_avro(&input_dir, "input.avro", &schema, |writer| {
        let mut record = Record::new(&schema).expect("record");
        record.put("id", "1");
        record.put("amount", "1");
        writer.append(record).expect("append");

        let mut record = Record::new(&schema).expect("record");
        record.put("id", "2");
        record.put("amount", "bad");
        writer.append(record).expect("append");
    });

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "payments"
    source:
      format: "avro"
      path: "{input_dir}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "amount"
          type: "number"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Rejected);
    assert_eq!(file.accepted_count, 1);
    assert_eq!(file.rejected_count, 1);
}

#[test]
fn avro_strict_ignores_undeclared_complex_extra_fields() {
    let root = temp_dir("floe-avro-extra-complex");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let schema = Schema::parse_str(
        r#"{
          "type": "record",
          "name": "customer",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "tags", "type": {"type": "array", "items": "string"}}
          ]
        }"#,
    )
    .expect("schema");

    write_avro_values(
        &input_dir,
        "input.avro",
        &schema,
        vec![
            Value::Record(vec![
                ("id".to_string(), Value::String("1".to_string())),
                (
                    "tags".to_string(),
                    Value::Array(vec![Value::String("a".to_string())]),
                ),
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::String("2".to_string())),
                (
                    "tags".to_string(),
                    Value::Array(vec![Value::String("b".to_string())]),
                ),
            ]),
        ],
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "avro"
      path: "{input_dir}"
      cast_mode: "strict"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let _outcome = run_config(&config_path);
    let output_path = accepted_dir.join("part-00000.parquet");
    assert!(output_path.exists(), "expected parquet output");

    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    assert_eq!(df.height(), 2);
}

#[test]
fn avro_nullable_root_union_allows_null_records() {
    let root = temp_dir("floe-avro-root-union");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let schema = Schema::parse_str(
        r#"[
          "null",
          {
            "type": "record",
            "name": "customer",
            "fields": [
              {"name": "id", "type": "string"},
              {"name": "name", "type": "string"}
            ]
          }
        ]"#,
    )
    .expect("schema");

    write_avro_values(
        &input_dir,
        "input.avro",
        &schema,
        vec![
            Value::Union(0, Box::new(Value::Null)),
            Value::Union(
                1,
                Box::new(Value::Record(vec![
                    ("id".to_string(), Value::String("1".to_string())),
                    ("name".to_string(), Value::String("alice".to_string())),
                ])),
            ),
        ],
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "avro"
      path: "{input_dir}"
      cast_mode: "strict"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let _outcome = run_config(&config_path);
    let output_path = accepted_dir.join("part-00000.parquet");
    assert!(output_path.exists(), "expected parquet output");

    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    assert_eq!(df.height(), 2);
}
