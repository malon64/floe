use std::fs;
use std::path::{Path, PathBuf};

use deltalake::table::builder::DeltaTableBuilder;
use floe_core::{run, validate, RunOptions, ValidateOptions};
use polars::prelude::DataFrame;
use url::Url;

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

fn read_local_delta_table(path: &Path) -> DataFrame {
    let url = Url::from_directory_path(path).expect("delta table path url");
    let builder = DeltaTableBuilder::from_url(url)
        .expect("delta table builder")
        .with_storage_options(std::collections::HashMap::new());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("delta runtime");
    let table = runtime
        .block_on(async move { builder.load().await })
        .expect("load delta table");
    let mut merged = DataFrame::default();
    for file_uri in table
        .get_file_uris()
        .expect("list table uris")
        .collect::<Vec<_>>()
    {
        let path = Url::parse(&file_uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .unwrap_or_else(|| PathBuf::from(&file_uri));
        let frame = floe_core::io::read::parquet::read_parquet_lazy(&path, None)
            .expect("read delta parquet file");
        if merged.height() == 0 && merged.width() == 0 {
            merged = frame;
        } else {
            merged
                .vstack_mut(&frame)
                .expect("append delta parquet rows");
        }
    }
    merged
}

#[test]
fn local_delta_run_respects_partition_by_and_reports_metrics() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/orders_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "orders.csv", "id;country\n1;us\n2;ca\n3;us\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "append"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
        partition_by: ["country"]
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "country"
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
            run_id: Some("it-delta-partitioned".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let entry_names = fs::read_dir(&accepted_dir)
        .expect("read delta table dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(entry_names.iter().any(|name| name == "_delta_log"));
    assert!(entry_names.iter().any(|name| name == "country=us"));
    assert!(entry_names.iter().any(|name| name == "country=ca"));

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "delta");
    assert!(report.accepted_output.files_written > 0);
    assert_eq!(report.accepted_output.parts_written, 1);
    assert!(report.accepted_output.total_bytes_written.is_some());
    assert!(report.accepted_output.avg_file_size_mb.is_some());
    assert!(report.accepted_output.small_files_count.is_some());
}

#[test]
fn local_delta_run_without_partitioning_preserves_existing_behavior() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/orders_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "orders.csv", "id;country\n1;us\n2;ca\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      accepted:
        format: "delta"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "country"
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
            run_id: Some("it-delta-unpartitioned".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let entry_names = fs::read_dir(&accepted_dir)
        .expect("read delta table dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(entry_names.iter().any(|name| name == "_delta_log"));
    assert!(!entry_names.iter().any(|name| name.starts_with("country=")));

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "delta");
    assert!(report.accepted_output.files_written > 0);
}

#[test]
fn local_delta_merge_scd1_upserts_and_reports_metrics() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch1.csv",
        "id;country;name\n1;fr;alice\n2;ca;bob\n",
    );

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
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "reject"
    schema:
      primary_key: ["id", "country"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name\n1;fr;alice-updated\n3;us;carol\n",
    );

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge upsert run");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(
        report.accepted_output.write_mode.as_deref(),
        Some("merge_scd1")
    );
    assert_eq!(
        report.accepted_output.merge_key,
        vec!["id".to_string(), "country".to_string()]
    );
    assert_eq!(report.accepted_output.updated_count, Some(1));
    assert_eq!(report.accepted_output.inserted_count, Some(1));
    assert_eq!(report.accepted_output.target_rows_before, Some(2));
    assert_eq!(report.accepted_output.target_rows_after, Some(3));

    let df = read_local_delta_table(&accepted_dir);
    assert_eq!(df.height(), 3);
    let id = df
        .column("id")
        .expect("id")
        .as_materialized_series()
        .str()
        .expect("id string");
    let country = df
        .column("country")
        .expect("country")
        .as_materialized_series()
        .str()
        .expect("country string");
    let name = df
        .column("name")
        .expect("name")
        .as_materialized_series()
        .str()
        .expect("name string");
    let rows = (0..df.height())
        .map(|idx| {
            (
                id.get(idx).unwrap_or_default().to_string(),
                country.get(idx).unwrap_or_default().to_string(),
                name.get(idx).unwrap_or_default().to_string(),
            )
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(rows.contains(&(
        "1".to_string(),
        "fr".to_string(),
        "alice-updated".to_string()
    )));
    assert!(rows.contains(&("2".to_string(), "ca".to_string(), "bob".to_string())));
    assert!(rows.contains(&("3".to_string(), "us".to_string(), "carol".to_string())));
}

#[test]
fn local_delta_merge_scd1_warn_drops_duplicate_keys_before_merge() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch.csv",
        "id;country;name\n1;fr;alice\n1;fr;alice-dup\n",
    );

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
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      primary_key: ["id", "country"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
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
            run_id: Some("it-delta-merge-dup-source".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge_scd1 should reject duplicate merge-key rows before merge in warn mode");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.rows_total, 2);
    assert_eq!(report.results.accepted_total, 1);
    assert_eq!(report.results.rejected_total, 1);

    let df = read_local_delta_table(&accepted_dir);
    assert_eq!(df.height(), 1);
    let name = df
        .column("name")
        .expect("name")
        .as_materialized_series()
        .str()
        .expect("name string");
    assert_eq!(name.get(0), Some("alice"));
}

#[test]
fn local_delta_merge_scd2_closes_changed_rows_and_inserts_new_versions() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch1.csv",
        "id;country;name\n1;fr;alice\n2;ca;bob\n",
    );

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
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      primary_key: ["id", "country"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
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
            run_id: Some("it-delta-merge-scd2-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd2 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first batch");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name\n1;fr;alice-v2\n2;ca;bob\n3;us;carol\n",
    );

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge_scd2 upsert run");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(
        report.accepted_output.write_mode.as_deref(),
        Some("merge_scd2")
    );
    assert_eq!(report.accepted_output.inserted_count, Some(2));
    assert_eq!(report.accepted_output.updated_count, Some(1));
    assert_eq!(report.accepted_output.target_rows_before, Some(2));
    assert_eq!(report.accepted_output.target_rows_after, Some(4));

    let df = read_local_delta_table(&accepted_dir);
    assert_eq!(df.height(), 4);
    assert!(df.column("__floe_is_current").is_ok());
    assert!(df.column("__floe_valid_from").is_ok());
    assert!(df.column("__floe_valid_to").is_ok());

    let id = df
        .column("id")
        .expect("id")
        .as_materialized_series()
        .str()
        .expect("id string");
    let country = df
        .column("country")
        .expect("country")
        .as_materialized_series()
        .str()
        .expect("country string");
    let name = df
        .column("name")
        .expect("name")
        .as_materialized_series()
        .str()
        .expect("name string");
    let is_current = df
        .column("__floe_is_current")
        .expect("is current")
        .as_materialized_series()
        .bool()
        .expect("is_current bool");
    let rows = (0..df.height())
        .map(|idx| {
            (
                id.get(idx).unwrap_or_default().to_string(),
                country.get(idx).unwrap_or_default().to_string(),
                name.get(idx).unwrap_or_default().to_string(),
                is_current.get(idx).unwrap_or(false),
            )
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(rows.contains(&(
        "1".to_string(),
        "fr".to_string(),
        "alice".to_string(),
        false
    )));
    assert!(rows.contains(&(
        "1".to_string(),
        "fr".to_string(),
        "alice-v2".to_string(),
        true
    )));
    assert!(rows.contains(&("2".to_string(), "ca".to_string(), "bob".to_string(), true)));
    assert!(rows.contains(&("3".to_string(), "us".to_string(), "carol".to_string(), true)));
}

#[test]
fn validate_merge_scd1_requires_primary_key() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let config_path = write_config(
        root,
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
    );

    let err = validate(&config_path, ValidateOptions::default())
        .expect_err("merge_scd1 config without primary key should fail");
    assert!(err.to_string().contains("schema.primary_key"));
}

#[test]
fn validate_merge_scd1_fails_on_non_delta_sink() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let config_path = write_config(
        root,
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      primary_key: ["id"]
      columns:
        - name: "id"
          type: "string"
"#,
    );

    let err = validate(&config_path, ValidateOptions::default())
        .expect_err("merge_scd1 on non-delta sink should fail");
    assert!(err.to_string().contains("sink.accepted.format=delta"));
}

#[test]
fn validate_merge_scd2_requires_primary_key() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let config_path = write_config(
        root,
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
    );

    let err = validate(&config_path, ValidateOptions::default())
        .expect_err("merge_scd2 config without primary key should fail");
    assert!(err.to_string().contains("schema.primary_key"));
}

#[test]
fn validate_merge_scd2_fails_on_non_delta_sink() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let config_path = write_config(
        root,
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      primary_key: ["id"]
      columns:
        - name: "id"
          type: "string"
"#,
    );

    let err = validate(&config_path, ValidateOptions::default())
        .expect_err("merge_scd2 on non-delta sink should fail");
    assert!(err.to_string().contains("sink.accepted.format=delta"));
}
