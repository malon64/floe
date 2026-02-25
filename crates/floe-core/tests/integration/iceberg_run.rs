use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use floe_core::{run, RunOptions};
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::Transform;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

fn write_csv(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write csv");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

#[test]
fn local_run_writes_iceberg_table_and_report_fields() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_iceberg");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id,name\n1,alice\n2,bob\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "append"
      accepted:
        format: "iceberg"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-iceberg".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    assert!(accepted_dir.join("metadata").exists());
    assert!(accepted_dir.join("data").exists());

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "iceberg");
    assert_eq!(report.accepted_output.path, report.sink.accepted.path);
    assert_eq!(
        report.accepted_output.table_root_uri.as_deref(),
        Some(report.sink.accepted.path.as_str())
    );
    assert_eq!(report.accepted_output.write_mode.as_deref(), Some("append"));
    assert_eq!(report.accepted_output.files_written, 1);
    assert_eq!(report.accepted_output.parts_written, 1);
    assert!(report.accepted_output.snapshot_id.is_some());
}

#[test]
fn local_run_applies_iceberg_partition_spec_runtime() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_iceberg");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id,name\n1,alice\n2,bob\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "overwrite"
      accepted:
        format: "iceberg"
        path: "{accepted_dir}"
        partition_spec:
          - column: "id"
            transform: "identity"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "int"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-iceberg-partition-spec".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let data_entries = fs::read_dir(accepted_dir.join("data"))
        .expect("data dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert!(data_entries.iter().any(|name| name.starts_with("id=")));

    let table = load_local_iceberg_table(&accepted_dir, "customer").expect("load iceberg table");
    let fields = table.metadata().default_partition_spec().fields();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name, "id");
    assert_eq!(fields[0].transform, Transform::Identity);
}

fn load_local_iceberg_table(
    table_path: &Path,
    table_name: &str,
) -> floe_core::FloeResult<iceberg::table::Table> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let metadata_location = latest_metadata_location(table_path).ok_or_else(|| {
            Box::new(floe_core::errors::RunError(
                "missing iceberg metadata file".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "floe_test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    table_path.display().to_string(),
                )]),
            )
            .await
            .map_err(|err| {
                Box::new(floe_core::errors::RunError(format!(
                    "iceberg test catalog init failed: {err}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
        let namespace = NamespaceIdent::new("floe".to_string());
        if !catalog.namespace_exists(&namespace).await.map_err(|err| {
            Box::new(floe_core::errors::RunError(format!(
                "iceberg test namespace exists failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })? {
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .map_err(|err| {
                    Box::new(floe_core::errors::RunError(format!(
                        "iceberg test namespace create failed: {err}"
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?;
        }
        let ident = TableIdent::new(namespace, table_name.to_string());
        catalog
            .register_table(&ident, metadata_location)
            .await
            .map_err(|err| {
                Box::new(floe_core::errors::RunError(format!(
                    "iceberg test register table failed: {err}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })
    })
}

fn latest_metadata_location(table_path: &Path) -> Option<String> {
    let metadata_dir = table_path.join("metadata");
    if !metadata_dir.exists() {
        return None;
    }
    let mut files = fs::read_dir(metadata_dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.ends_with(".metadata.json"))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    files.sort();
    files.last().map(|path| path.display().to_string())
}
