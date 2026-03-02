use floe_core::io::storage::Target;
use floe_core::io::write::delta::{
    delta_commit_metrics_from_log_bytes, delta_commit_metrics_from_log_bytes_best_effort,
    delta_write_runtime_options, write_delta_table,
};
use floe_core::{config, FloeResult};
use polars::prelude::{df, ParquetReader, SerReader};
use std::path::Path;
use url::Url;

#[test]
fn write_delta_table_overwrite() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let mut df = df!(
        "id" => &[1i64, 2, 3],
        "name" => &["a", "b", "c"]
    )?;
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);
    let version1 = write_delta_table(
        &mut df,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Overwrite,
    )?;

    let runtime = runtime()?;
    let table = open_table(&runtime, &table_path)?;

    let field_names = table
        .snapshot()?
        .schema()
        .fields()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert!(field_names.contains(&"id".to_string()));

    assert_eq!(row_count(&table)?, df.height());

    let mut df_overwrite = df!(
        "id" => &[4i64, 5],
        "name" => &["d", "e"]
    )?;
    let version2 = write_delta_table(
        &mut df_overwrite,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Overwrite,
    )?;
    assert!(version2 > version1);

    let table = open_table(&runtime, &table_path)?;
    assert!(table.version().unwrap_or(0) >= version2);
    assert_eq!(row_count(&table)?, df_overwrite.height());
    assert!(delta_log_json_count(&table_path)? >= 2);

    Ok(())
}

#[test]
fn write_delta_table_append() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Append, Vec::new(), None);

    let mut df_first = df!(
        "id" => &[1i64, 2, 3],
        "name" => &["a", "b", "c"]
    )?;
    let version1 = write_delta_table(
        &mut df_first,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    )?;

    let runtime = runtime()?;
    let table = open_table(&runtime, &table_path)?;
    let files_after_first = table.get_file_uris()?.count();
    assert_eq!(row_count(&table)?, df_first.height());

    let mut df_second = df!(
        "id" => &[4i64, 5],
        "name" => &["d", "e"]
    )?;
    let version2 = write_delta_table(
        &mut df_second,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    )?;
    assert!(version2 > version1);

    let table = open_table(&runtime, &table_path)?;
    assert!(table.version().unwrap_or(0) >= version2);
    assert_eq!(row_count(&table)?, df_first.height() + df_second.height());
    assert!(table.get_file_uris()?.count() > files_after_first);
    assert!(delta_log_json_count(&table_path)? >= 2);

    Ok(())
}

#[test]
fn delta_append_allows_nulls_for_nullable_columns() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(
        &table_path,
        config::WriteMode::Append,
        vec![
            column("id", "int64", Some(false)),
            column("name", "string", Some(true)),
        ],
        None,
    );

    let mut df_first = df!(
        "id" => &[1i64, 2, 3],
        "name" => &["a", "b", "c"]
    )?;
    write_delta_table(
        &mut df_first,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    )?;

    let mut df_second = df!(
        "id" => &[4i64, 5],
        "name" => &[Some("d"), None]
    )?;
    write_delta_table(
        &mut df_second,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    )?;

    let runtime = runtime()?;
    let table = open_table(&runtime, &table_path)?;
    assert_eq!(row_count(&table)?, df_first.height() + df_second.height());

    let schema_fields = table
        .snapshot()?
        .schema()
        .fields()
        .map(|field| (field.name.clone(), field.nullable))
        .collect::<Vec<_>>();
    assert!(schema_fields
        .iter()
        .any(|(name, nullable)| { name == "name" && *nullable }));

    Ok(())
}

#[test]
fn delta_append_rejects_nulls_for_non_nullable_columns() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(
        &table_path,
        config::WriteMode::Append,
        vec![
            column("id", "int64", Some(false)),
            column("name", "string", Some(false)),
        ],
        None,
    );

    let mut df_first = df!(
        "id" => &[1i64, 2],
        "name" => &["a", "b"]
    )?;
    write_delta_table(
        &mut df_first,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    )?;

    let mut df_second = df!(
        "id" => &[3i64, 4],
        "name" => &[Some("c"), None]
    )?;
    let append_result = write_delta_table(
        &mut df_second,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    );
    assert!(append_result.is_err());

    let runtime = runtime()?;
    let table = open_table(&runtime, &table_path)?;
    assert_eq!(row_count(&table)?, df_first.height());

    Ok(())
}

#[test]
fn delta_write_uses_normalized_schema_names() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(
        &table_path,
        config::WriteMode::Append,
        vec![
            column("User Id", "int64", Some(false)),
            column("Full Name", "string", Some(true)),
        ],
        Some(normalize_config("snake_case")),
    );

    let mut df = df!(
        "user_id" => &[1i64, 2],
        "full_name" => &["alice", "bob"]
    )?;
    write_delta_table(
        &mut df,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    )?;

    let runtime = runtime()?;
    let table = open_table(&runtime, &table_path)?;
    assert_eq!(row_count(&table)?, df.height());

    let field_names = table
        .snapshot()?
        .schema()
        .fields()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert!(field_names.contains(&"user_id".to_string()));
    assert!(field_names.contains(&"full_name".to_string()));

    Ok(())
}

#[test]
fn delta_runtime_options_map_partitioning_and_target_file_size() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let mut entity = build_entity(&table_path, config::WriteMode::Append, Vec::new(), None);
    entity.sink.accepted.partition_by = Some(vec!["country".to_string(), "event_date".to_string()]);
    entity.sink.accepted.options = Some(config::SinkOptions {
        compression: None,
        row_group_size: None,
        max_size_per_file: Some(8 * 1024 * 1024),
    });

    let options = delta_write_runtime_options(&entity)?;
    assert_eq!(
        options.partition_by,
        Some(vec!["country".to_string(), "event_date".to_string()])
    );
    assert_eq!(options.target_file_size_bytes, Some(8 * 1024 * 1024));
    assert_eq!(options.small_file_threshold_bytes, 4 * 1024 * 1024);
    Ok(())
}

#[test]
fn write_delta_table_respects_partition_by_columns() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let mut entity = build_entity(&table_path, config::WriteMode::Append, Vec::new(), None);
    entity.sink.accepted.partition_by = Some(vec!["country".to_string()]);

    let mut df = df!(
        "id" => &[1i64, 2, 3],
        "country" => &["us", "ca", "us"]
    )?;
    write_delta_table(
        &mut df,
        &target,
        &resolver,
        &entity,
        config::WriteMode::Append,
    )?;

    let runtime = runtime()?;
    let table = open_table(&runtime, &table_path)?;
    let partition_columns = table.snapshot()?.metadata().partition_columns().clone();
    assert_eq!(partition_columns, vec!["country".to_string()]);

    let entries = std::fs::read_dir(&table_path)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(entries.iter().any(|name| name == "country=us"));
    assert!(entries.iter().any(|name| name == "country=ca"));

    Ok(())
}

#[test]
fn delta_commit_metrics_from_log_bytes_counts_add_actions_and_caps_part_files() -> FloeResult<()> {
    let mut content = String::new();
    for i in 0..55_u64 {
        content.push_str(
            format!(
                "{{\"add\":{{\"path\":\"country=us/part-{i:05}.parquet\",\"size\":{}}}}}\n",
                i + 10
            )
            .as_str(),
        );
    }
    content.push_str("{\"commitInfo\":{\"operation\":\"WRITE\"}}\n");

    let (files_written, part_files, metrics) =
        delta_commit_metrics_from_log_bytes(content.as_bytes(), 32)?;

    assert_eq!(files_written, 55);
    assert_eq!(part_files.len(), 50);
    assert_eq!(part_files[0], "part-00000.parquet");
    assert_eq!(part_files[49], "part-00049.parquet");
    assert_eq!(metrics.total_bytes_written, Some((10..65).sum()));
    assert_eq!(metrics.small_files_count, Some(22));
    assert!(metrics.avg_file_size_mb.is_some());
    Ok(())
}

#[test]
fn delta_commit_metrics_from_log_bytes_missing_size_keeps_file_count_but_nulls_metrics(
) -> FloeResult<()> {
    let content = r#"{"add":{"path":"part-00000.parquet","size":10}}
{"add":{"path":"part-00001.parquet"}}
"#;

    let (files_written, part_files, metrics) =
        delta_commit_metrics_from_log_bytes(content.as_bytes(), 16)?;

    assert_eq!(files_written, 2);
    assert_eq!(part_files, vec!["part-00000.parquet", "part-00001.parquet"]);
    assert_eq!(metrics.total_bytes_written, None);
    assert_eq!(metrics.avg_file_size_mb, None);
    assert_eq!(metrics.small_files_count, None);
    Ok(())
}

#[test]
fn delta_commit_metrics_from_log_bytes_best_effort_falls_back_on_malformed_json() {
    let malformed = b"{\"add\":{\"path\":\"part-00000.parquet\",\"size\":10}}\n{\"add\":\n";

    let (files_written, part_files, metrics) =
        delta_commit_metrics_from_log_bytes_best_effort(malformed, 16);

    assert_eq!(files_written, 0);
    assert!(part_files.is_empty());
    assert_eq!(metrics.total_bytes_written, None);
    assert_eq!(metrics.avg_file_size_mb, None);
    assert_eq!(metrics.small_files_count, None);
}

fn empty_root_config() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
        catalogs: None,
        env: None,
        domains: Vec::new(),
        report: None,
        entities: Vec::new(),
    }
}

fn resolve_local_target(
    resolver: &config::StorageResolver,
    table_path: &Path,
) -> FloeResult<Target> {
    let resolved = resolver.resolve_path(
        "orders",
        "sink.accepted.path",
        None,
        table_path.to_str().unwrap(),
    )?;
    Target::from_resolved(&resolved)
}

fn build_entity(
    table_path: &Path,
    write_mode: config::WriteMode,
    columns: Vec<config::ColumnConfig>,
    normalize_columns: Option<config::NormalizeColumnsConfig>,
) -> config::EntityConfig {
    config::EntityConfig {
        name: "orders".to_string(),
        metadata: None,
        domain: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode,
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: table_path.display().to_string(),
                storage: None,
                options: None,
                iceberg: None,
                write_mode,
                partition_by: None,
                partition_spec: None,
            },
            rejected: None,
            archive: None,
        },
        policy: config::PolicyConfig {
            severity: "warn".to_string(),
        },
        schema: config::SchemaConfig {
            normalize_columns,
            mismatch: None,
            primary_key: None,
            unique_keys: None,
            columns,
        },
    }
}

fn column(name: &str, column_type: &str, nullable: Option<bool>) -> config::ColumnConfig {
    config::ColumnConfig {
        name: name.to_string(),
        source: None,
        column_type: column_type.to_string(),
        nullable,
        unique: None,
        width: None,
        trim: None,
    }
}

fn normalize_config(strategy: &str) -> config::NormalizeColumnsConfig {
    config::NormalizeColumnsConfig {
        enabled: Some(true),
        strategy: Some(strategy.to_string()),
    }
}

fn runtime() -> FloeResult<tokio::runtime::Runtime> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            Box::new(floe_core::errors::RunError(format!(
                "delta test runtime init failed: {err}"
            )))
        })?;
    Ok(runtime)
}

fn open_table(
    runtime: &tokio::runtime::Runtime,
    table_path: &Path,
) -> FloeResult<deltalake::DeltaTable> {
    let table_url = Url::from_directory_path(table_path).map_err(|_| {
        Box::new(floe_core::errors::RunError(
            "delta test path is not a valid url".to_string(),
        ))
    })?;
    let table = runtime
        .block_on(async { deltalake::open_table(table_url).await })
        .map_err(|err| {
            Box::new(floe_core::errors::RunError(format!(
                "delta test open failed: {err}"
            )))
        })?;
    Ok(table)
}

fn row_count(table: &deltalake::DeltaTable) -> FloeResult<usize> {
    let mut total = 0usize;
    for uri in table.get_file_uris()? {
        let file = std::fs::File::open(&uri)?;
        let df_read = ParquetReader::new(file).finish()?;
        total += df_read.height();
    }
    Ok(total)
}

fn delta_log_json_count(table_path: &Path) -> FloeResult<usize> {
    let log_dir = table_path.join("_delta_log");
    assert!(log_dir.exists());
    let count = std::fs::read_dir(log_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
        .count();
    Ok(count)
}
