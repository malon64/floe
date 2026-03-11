use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use deltalake::datafusion::datasource::TableProvider;
use deltalake::table::builder::DeltaTableBuilder;
use floe_core::{run, set_observer, validate, RunEvent, RunObserver, RunOptions, ValidateOptions};
use polars::prelude::{DataFrame, DataType, Series};
use url::Url;

#[derive(Default)]
struct TestObserver {
    events: Mutex<Vec<RunEvent>>,
}

impl TestObserver {
    fn reset(&self) {
        self.events.lock().expect("observer lock").clear();
    }

    fn events_for_run(&self, run_id: &str) -> Vec<RunEvent> {
        self.events
            .lock()
            .expect("observer lock")
            .iter()
            .filter(|event| match event {
                RunEvent::Log {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::RunStarted {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::EntityStarted {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::FileStarted {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::FileFinished {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::SchemaEvolutionApplied {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::EntityFinished {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::RunFinished {
                    run_id: event_run_id,
                    ..
                } => event_run_id == run_id,
            })
            .cloned()
            .collect()
    }
}

impl RunObserver for TestObserver {
    fn on_event(&self, event: RunEvent) {
        self.events.lock().expect("observer lock").push(event);
    }
}

fn test_observer() -> &'static TestObserver {
    static OBSERVER: OnceLock<Arc<TestObserver>> = OnceLock::new();
    let observer = OBSERVER.get_or_init(|| {
        let observer = Arc::new(TestObserver::default());
        let _ = set_observer(observer.clone());
        observer
    });
    observer.as_ref()
}

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
    let ordered_columns = table
        .schema()
        .fields()
        .into_iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut frames = table
        .get_file_uris()
        .expect("list table uris")
        .map(|file_uri| {
            let path = Url::parse(&file_uri)
                .ok()
                .and_then(|url| url.to_file_path().ok())
                .unwrap_or_else(|| PathBuf::from(&file_uri));
            floe_core::io::read::parquet::read_parquet_lazy(&path, None)
                .expect("read delta parquet file")
        })
        .collect::<Vec<_>>();
    let mut column_dtypes = std::collections::HashMap::<String, DataType>::new();
    for frame in &frames {
        for series in frame.materialized_column_iter() {
            column_dtypes
                .entry(series.name().as_str().to_string())
                .or_insert_with(|| series.dtype().clone());
        }
    }
    let mut merged = DataFrame::default();
    for frame in &mut frames {
        for column in &ordered_columns {
            if frame.column(column).is_ok() {
                continue;
            }
            let dtype = column_dtypes.get(column).cloned().unwrap_or(DataType::Null);
            frame
                .with_column(Series::full_null(
                    column.clone().into(),
                    frame.height(),
                    &dtype,
                ))
                .expect("append missing delta column");
        }
        let aligned = frame
            .select(ordered_columns.iter().map(String::as_str))
            .expect("align delta columns");
        if merged.height() == 0 && merged.width() == 0 {
            merged = aligned;
        } else {
            merged
                .vstack_mut(&aligned)
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

    let initial_yaml = format!(
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
    let config_path = write_config(root, &initial_yaml);

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
fn local_delta_append_add_columns_mode_reports_schema_evolution_and_event() {
    let observer = test_observer();
    observer.reset();

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/orders_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "batch1.csv", "id\n1\n2\n");

    let initial_yaml = format!(
        r#"version: "0.2"
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
    policy:
      severity: "warn"
    schema:
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "int64"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &initial_yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-schema-evolution-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(&input_dir, "batch2.csv", "id;email\n3;a@example.com\n4;\n");
    let evolved_yaml = format!(
        r#"version: "0.2"
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
    policy:
      severity: "warn"
    schema:
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "int64"
        - name: "email"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    write_config(root, &evolved_yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-schema-evolution-append".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("schema evolution append run");

    let report = &outcome.entity_outcomes[0].report;
    assert!(report.schema_evolution.enabled);
    assert_eq!(report.schema_evolution.mode, "add_columns");
    assert!(report.schema_evolution.applied);
    assert_eq!(
        report.schema_evolution.added_columns,
        vec!["email".to_string()]
    );
    assert!(!report.schema_evolution.incompatible_changes_detected);

    let events = observer.events_for_run("it-delta-schema-evolution-append");
    let event = events
        .iter()
        .find_map(|event| match event {
            RunEvent::SchemaEvolutionApplied {
                entity,
                mode,
                added_columns,
                ..
            } => Some((entity, mode, added_columns)),
            _ => None,
        })
        .expect("schema evolution event");
    assert_eq!(event.0, "orders");
    assert_eq!(event.1, "add_columns");
    assert_eq!(event.2, &vec!["email".to_string()]);

    let url = Url::from_directory_path(&accepted_dir).expect("delta table path url");
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
    let field_names = table
        .snapshot()
        .expect("table snapshot")
        .schema()
        .fields()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert!(field_names.contains(&"email".to_string()));
}

#[test]
fn local_delta_overwrite_add_columns_mode_reports_noop_when_unchanged() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/orders_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "orders.csv",
        "id;email\n1;a@example.com\n2;b@example.com\n",
    );

    let initial_yaml = format!(
        r#"version: "0.2"
report:
  path: "{report_dir}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "overwrite"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "int64"
        - name: "email"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &initial_yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-schema-evolution-overwrite-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial overwrite run");

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-schema-evolution-overwrite-noop".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("overwrite no-op run");

    let report = &outcome.entity_outcomes[0].report;
    assert!(report.schema_evolution.enabled);
    assert_eq!(report.schema_evolution.mode, "add_columns");
    assert!(!report.schema_evolution.applied);
    assert!(report.schema_evolution.added_columns.is_empty());
    assert!(!report.schema_evolution.incompatible_changes_detected);
}

#[test]
fn local_delta_strict_mode_rejects_added_columns() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/orders_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "batch1.csv", "id\n1\n2\n");

    let initial_yaml = format!(
        r#"version: "0.2"
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
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "int64"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &initial_yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-strict-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial strict run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(&input_dir, "batch2.csv", "id;email\n3;a@example.com\n");
    let evolved_yaml = format!(
        r#"version: "0.2"
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
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "int64"
        - name: "email"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    write_config(root, &evolved_yaml);

    let err = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-strict-extra-column".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect_err("strict mode should reject added columns");
    assert!(err.to_string().contains("delta write failed"));
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
fn local_delta_merge_scd1_add_columns_evolves_target_and_reports_event() {
    let observer = test_observer();
    observer.reset();
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

    let initial_yaml = format!(
        r#"version: "0.2"
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
      schema_evolution:
        mode: "add_columns"
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
    let config_path = write_config(root, &initial_yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd1-evolution-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd1 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name;city\n1;fr;alice-updated;paris\n3;us;carol;new-york\n",
    );

    let evolved_yaml = format!(
        r#"version: "0.2"
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
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "name"
          type: "string"
        - name: "city"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    write_config(root, &evolved_yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd1-evolution-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge_scd1 run with additive column");

    let report = &outcome.entity_outcomes[0].report;
    assert!(report.schema_evolution.enabled);
    assert_eq!(report.schema_evolution.mode, "add_columns");
    assert!(report.schema_evolution.applied);
    assert_eq!(
        report.schema_evolution.added_columns,
        vec!["city".to_string()]
    );
    assert!(!report.schema_evolution.incompatible_changes_detected);

    let events = observer.events_for_run("it-delta-merge-scd1-evolution-upsert");
    let event = events
        .iter()
        .find_map(|event| match event {
            RunEvent::SchemaEvolutionApplied {
                entity,
                mode,
                added_columns,
                ..
            } => Some((entity, mode, added_columns)),
            _ => None,
        })
        .expect("schema evolution event");
    assert_eq!(event.0, "customer");
    assert_eq!(event.1, "add_columns");
    assert_eq!(event.2, &vec!["city".to_string()]);

    let df = read_local_delta_table(&accepted_dir);
    assert!(df.column("city").is_ok());
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
    let city = df
        .column("city")
        .expect("city")
        .as_materialized_series()
        .str()
        .expect("city string");
    let rows = (0..df.height())
        .map(|idx| {
            (
                id.get(idx).unwrap_or_default().to_string(),
                country.get(idx).unwrap_or_default().to_string(),
                city.get(idx).map(str::to_string),
            )
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(rows.contains(&("1".to_string(), "fr".to_string(), Some("paris".to_string()))));
    assert!(rows.contains(&("2".to_string(), "ca".to_string(), None)));
    assert!(rows.contains(&(
        "3".to_string(),
        "us".to_string(),
        Some("new-york".to_string())
    )));
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
    assert_eq!(report.accepted_output.closed_count, Some(1));
    assert_eq!(report.accepted_output.unchanged_count, Some(1));
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
fn local_delta_merge_scd2_add_columns_preserves_system_columns() {
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

    let initial_yaml = format!(
        r#"version: "0.2"
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
      schema_evolution:
        mode: "add_columns"
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
    let config_path = write_config(root, &initial_yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-evolution-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd2 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first batch");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name;city\n1;fr;alice-v2;paris\n2;ca;bob;toronto\n3;us;carol;austin\n",
    );

    let evolved_yaml = format!(
        r#"version: "0.2"
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
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "name"
          type: "string"
        - name: "city"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    write_config(root, &evolved_yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-evolution-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge_scd2 run with additive column");

    let report = &outcome.entity_outcomes[0].report;
    assert!(report.schema_evolution.enabled);
    assert_eq!(report.schema_evolution.mode, "add_columns");
    assert!(report.schema_evolution.applied);
    assert_eq!(
        report.schema_evolution.added_columns,
        vec!["city".to_string()]
    );
    assert_eq!(report.accepted_output.closed_count, Some(2));
    assert_eq!(report.accepted_output.inserted_count, Some(3));

    let df = read_local_delta_table(&accepted_dir);
    assert_eq!(df.height(), 5);
    assert!(df.column("city").is_ok());
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
    let city = df
        .column("city")
        .expect("city")
        .as_materialized_series()
        .str()
        .expect("city string");
    let is_current = df
        .column("__floe_is_current")
        .expect("is current")
        .as_materialized_series()
        .bool()
        .expect("is_current bool");

    let current_rows = (0..df.height())
        .filter(|idx| is_current.get(*idx) == Some(true))
        .map(|idx| {
            (
                id.get(idx).unwrap_or_default().to_string(),
                country.get(idx).unwrap_or_default().to_string(),
                city.get(idx).map(str::to_string),
            )
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(current_rows.len(), 3);
    assert!(current_rows.contains(&("1".to_string(), "fr".to_string(), Some("paris".to_string()))));
    assert!(current_rows.contains(&(
        "2".to_string(),
        "ca".to_string(),
        Some("toronto".to_string())
    )));
    assert!(current_rows.contains(&(
        "3".to_string(),
        "us".to_string(),
        Some("austin".to_string())
    )));

    let closed_historical_rows = (0..df.height())
        .filter(|idx| {
            matches!(
                (
                    id.get(*idx),
                    country.get(*idx),
                    is_current.get(*idx),
                    city.get(*idx)
                ),
                (Some("1"), Some("fr"), Some(false), None)
                    | (Some("2"), Some("ca"), Some(false), None)
            )
        })
        .count();
    assert_eq!(closed_historical_rows, 2);
}

#[test]
fn local_delta_merge_add_columns_mode_reports_schema_evolution_disabled_when_all_rows_rejected() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let rejected_dir = root.join("out/rejected/customer_csv");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "batch1.csv", "id;name\n1;alice\n2;bob\n");

    let yaml = format!(
        r#"version: "0.2"
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
      primary_key: ["id"]
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "int64"
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
            run_id: Some("it-delta-merge-schema-evolution-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(&input_dir, "batch2.csv", "id;name\nx;carol\ny;dave\n");

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-schema-evolution-empty".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge run with zero accepted rows");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.rows_total, 2);
    assert_eq!(report.results.accepted_total, 0);
    assert_eq!(report.results.rejected_total, 2);
    assert!(report.schema_evolution.enabled);
    assert_eq!(report.schema_evolution.mode, "add_columns");
    assert!(!report.schema_evolution.applied);
    assert!(report.schema_evolution.added_columns.is_empty());
    assert!(!report.schema_evolution.incompatible_changes_detected);

    let df = read_local_delta_table(&accepted_dir);
    assert_eq!(df.height(), 2);
}

#[test]
fn local_delta_merge_scd2_supports_custom_system_column_names() {
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
        merge:
          scd2:
            current_flag_column: "__is_current"
            valid_from_column: "__valid_from"
            valid_to_column: "__valid_to"
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
            run_id: Some("it-delta-merge-scd2-custom-cols-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd2 run with custom system columns");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first batch");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name\n1;fr;alice-v2\n2;ca;bob\n",
    );

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-custom-cols-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge_scd2 run with custom system columns");

    let df = read_local_delta_table(&accepted_dir);
    assert!(df.column("__is_current").is_ok());
    assert!(df.column("__valid_from").is_ok());
    assert!(df.column("__valid_to").is_ok());
    assert!(df.column("__floe_is_current").is_err());
    assert!(df.column("__floe_valid_from").is_err());
    assert!(df.column("__floe_valid_to").is_err());
}

#[test]
fn local_delta_merge_scd2_compare_columns_and_ignore_columns_control_change_detection() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch1.csv",
        "id;country;name;status;ingested_at\n1;fr;alice;active;2024-01-01T00:00:00Z\n",
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
        merge:
          ignore_columns: ["ingested_at"]
          compare_columns: ["name"]
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
        - name: "status"
          type: "string"
        - name: "ingested_at"
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
            run_id: Some("it-delta-merge-scd2-compare-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd2 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first batch");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name;status;ingested_at\n1;fr;alice;inactive;2024-02-01T00:00:00Z\n",
    );

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-compare-second".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("second merge_scd2 run");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.accepted_output.updated_count, Some(0));
    assert_eq!(report.accepted_output.inserted_count, Some(0));

    let df = read_local_delta_table(&accepted_dir);
    assert_eq!(df.height(), 1);
    let is_current = df
        .column("__floe_is_current")
        .expect("is current")
        .as_materialized_series()
        .bool()
        .expect("is_current bool");
    assert_eq!(is_current.get(0), Some(true));
    let status = df
        .column("status")
        .expect("status")
        .as_materialized_series()
        .str()
        .expect("status string");
    assert_eq!(status.get(0), Some("active"));
}

#[test]
fn local_delta_merge_scd2_compare_columns_map_to_normalized_output_names() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "batch1.csv", "id;name;status\n1;Alice;active\n");

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
        merge:
          compare_columns: ["Name"]
    policy:
      severity: "warn"
    schema:
      primary_key: ["id"]
      normalize_columns:
        enabled: true
        strategy: "lower"
      columns:
        - name: "id"
          type: "string"
        - name: "Name"
          type: "string"
        - name: "status"
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
            run_id: Some("it-delta-merge-scd2-compare-normalized-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd2 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first batch");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;name;status\n1;Alice Updated;active\n",
    );

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-compare-normalized-second".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("second merge_scd2 run");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.accepted_output.updated_count, Some(1));
    assert_eq!(report.accepted_output.inserted_count, Some(1));

    let df = read_local_delta_table(&accepted_dir);
    assert!(df.column("name").is_ok());
}

#[test]
fn local_delta_merge_scd2_bootstrap_preserves_configured_nullable_columns() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "batch1.csv", "id;country;name\n1;fr;alice\n");

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
          nullable: true
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-nullable-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd2 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first batch");
    write_csv(&input_dir, "batch2.csv", "id;country;name\n1;fr;\n");

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-nullable-second".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("second merge_scd2 run with null in configured nullable column");

    let df = read_local_delta_table(&accepted_dir);
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

    let has_current_null_name = (0..df.height()).any(|idx| {
        id.get(idx) == Some("1")
            && country.get(idx) == Some("fr")
            && is_current.get(idx) == Some(true)
            && name.get(idx).is_none()
    });
    assert!(has_current_null_name);
}

#[test]
fn local_delta_merge_scd2_fails_when_target_has_extra_business_columns() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch1.csv",
        "id;country;name;city\n1;fr;alice;paris\n",
    );

    let yaml_with_city = format!(
        r#"version: "0.1"
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
        - name: "city"
          type: "string"
"#,
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml_with_city);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-city-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd2 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first batch");
    write_csv(&input_dir, "batch2.csv", "id;country;name\n1;fr;alice-v2\n");

    let yaml_without_city = format!(
        r#"version: "0.1"
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
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    write_config(root, &yaml_without_city);

    let err = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd2-city-drift".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect_err("merge_scd2 with target-only business columns should fail");
    assert!(err
        .to_string()
        .contains("source schema missing target column city"));
}

#[test]
fn local_delta_merge_scd1_add_columns_rejects_non_additive_schema_change() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "batch1.csv", "id;country;score\n1;fr;10\n");

    let initial_yaml = format!(
        r#"version: "0.2"
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
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "score"
          type: "int64"
"#,
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &initial_yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd1-nonadditive-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd1 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(&input_dir, "batch2.csv", "id;country;score\n1;fr;ten\n");

    let evolved_yaml = format!(
        r#"version: "0.2"
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
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "score"
          type: "string"
"#,
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    write_config(root, &evolved_yaml);

    let err = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd1-nonadditive-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect_err("merge_scd1 with type change should fail");
    assert!(err.to_string().contains("incompatible changes detected"));
    assert!(err.to_string().contains("column score type changed"));
}

#[test]
fn local_delta_merge_scd1_add_columns_rejects_merge_key_evolution() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "batch1.csv", "id;name\n1;alice\n");

    let initial_yaml = format!(
        r#"version: "0.2"
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
      primary_key: ["id"]
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &initial_yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd1-merge-key-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge_scd1 run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove batch1");
    write_csv(&input_dir, "batch2.csv", "id;country;name\n1;fr;alice\n");

    let evolved_yaml = format!(
        r#"version: "0.2"
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
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "name"
          type: "string"
"#,
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    write_config(root, &evolved_yaml);

    let err = run(
        &config_path,
        RunOptions {
            run_id: Some("it-delta-merge-scd1-merge-key-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect_err("merge_scd1 with additive merge key should fail");
    assert!(err
        .to_string()
        .contains("merge key columns cannot be added: country"));
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
