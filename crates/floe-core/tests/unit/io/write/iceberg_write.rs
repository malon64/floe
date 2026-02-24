use std::collections::HashMap;
use std::fs;
use std::path::Path;

use arrow::array::Int64Array;
use floe_core::io::storage::Target;
use floe_core::io::write::iceberg::write_iceberg_table;
use floe_core::{config, FloeResult};
use futures::TryStreamExt;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use polars::prelude::{df, DataFrame, NamedFrom, Series};

#[test]
fn write_iceberg_table_append_creates_new_snapshot_and_metadata_version() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Append, Vec::new(), None);

    let mut df_first = df!(
        "id" => &[1i64, 2, 3],
        "name" => &["a", "b", "c"]
    )?;
    let out1 = write_iceberg_table(&mut df_first, &target, &entity, config::WriteMode::Append)?;
    assert_eq!(out1.parts_written, 1);
    assert!(out1.snapshot_id.is_some());

    let snapshot1 = current_snapshot_id(&table_path)?;
    let version1 = out1.table_version.unwrap_or(-1);
    assert_eq!(snapshot1, out1.snapshot_id.unwrap());

    let mut df_second = df!(
        "id" => &[4i64, 5],
        "name" => &["d", "e"]
    )?;
    let out2 = write_iceberg_table(&mut df_second, &target, &entity, config::WriteMode::Append)?;
    assert_eq!(out2.parts_written, 1);
    assert!(out2.snapshot_id.is_some());
    assert_ne!(out2.snapshot_id, out1.snapshot_id);
    assert!(out2.table_version.unwrap_or(-1) > version1);

    let mut rows = scan_i64_column(&table_path, "id")?;
    rows.sort_unstable();
    assert_eq!(rows, vec![1, 2, 3, 4, 5]);
    assert!(metadata_json_count(&table_path)? >= 3);

    Ok(())
}

#[test]
fn write_iceberg_table_overwrite_replaces_logical_contents() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let mut df_first = df!(
        "id" => &[10i64, 20, 30],
        "name" => &["a", "b", "c"]
    )?;
    let out1 = write_iceberg_table(
        &mut df_first,
        &target,
        &entity,
        config::WriteMode::Overwrite,
    )?;
    assert!(out1.snapshot_id.is_some());

    let mut df_second = df!(
        "id" => &[40i64, 50],
        "name" => &["d", "e"]
    )?;
    let out2 = write_iceberg_table(
        &mut df_second,
        &target,
        &entity,
        config::WriteMode::Overwrite,
    )?;
    assert!(out2.snapshot_id.is_some());

    let rows = scan_i64_column(&table_path, "id")?;
    assert_eq!(rows, vec![40, 50]);
    Ok(())
}

#[test]
fn write_iceberg_table_rejects_unsupported_dtype() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let payload = Series::new("payload".into(), &[Some(&b"ab"[..]), Some(&b"cd"[..])]);
    let mut df = DataFrame::new(vec![payload.into()])?;
    let err = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)
        .expect_err("binary dtype should be rejected");
    let msg = err.to_string();
    assert!(msg.contains("iceberg sink supports scalar types only"));
    assert!(msg.contains("payload"));

    Ok(())
}

#[test]
fn write_iceberg_table_empty_dataframe_creates_table_without_snapshot() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let mut df = df!(
        "id" => Vec::<i64>::new(),
        "name" => Vec::<String>::new()
    )?;
    let out = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)?;
    assert_eq!(out.parts_written, 0);
    assert!(out.snapshot_id.is_none());
    assert!(table_path.join("metadata").exists());
    assert!(!table_path.join("data").exists());
    assert_eq!(metadata_json_count(&table_path)?, 1);

    Ok(())
}

#[test]
fn write_iceberg_table_supports_i8_i16_by_upcasting_to_iceberg_int() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let mut df = df!(
        "tiny" => &[1_i8, 2_i8, 3_i8],
        "small" => &[10_i16, 20_i16, 30_i16]
    )?;
    let out = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)?;
    assert_eq!(out.parts_written, 1);
    assert!(out.snapshot_id.is_some());

    Ok(())
}

#[test]
fn write_iceberg_table_append_without_schema_keeps_nullability_stable() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Append, Vec::new(), None);

    let mut df_first = df!(
        "id" => &[1_i64, 2_i64],
        "name" => &["alice", "bob"]
    )?;
    write_iceberg_table(&mut df_first, &target, &entity, config::WriteMode::Append)?;

    let mut df_second = df!(
        "id" => &[3_i64, 4_i64],
        "name" => &[Some("charlie"), None]
    )?;
    let out = write_iceberg_table(&mut df_second, &target, &entity, config::WriteMode::Append)?;
    assert_eq!(out.parts_written, 1);
    assert!(out.snapshot_id.is_some());

    let mut ids = scan_i64_column(&table_path, "id")?;
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);

    Ok(())
}

fn empty_root_config() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
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
                format: "iceberg".to_string(),
                path: table_path.display().to_string(),
                storage: None,
                options: None,
                write_mode,
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
            columns,
        },
    }
}

fn metadata_json_count(table_path: &Path) -> FloeResult<usize> {
    let metadata_dir = table_path.join("metadata");
    if !metadata_dir.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(metadata_dir)? {
        let entry = entry?;
        if entry
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.ends_with(".metadata.json"))
            .unwrap_or(false)
        {
            count += 1;
        }
    }
    Ok(count)
}

fn current_snapshot_id(table_path: &Path) -> FloeResult<i64> {
    let runtime = test_runtime()?;
    runtime.block_on(async {
        let table = load_table(table_path).await?;
        table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| {
                Box::new(floe_core::errors::RunError(
                    "missing current iceberg snapshot".to_string(),
                )) as Box<dyn std::error::Error + Send + Sync>
            })
    })
}

fn scan_i64_column(table_path: &Path, column: &str) -> FloeResult<Vec<i64>> {
    let runtime = test_runtime()?;
    runtime.block_on(async {
        let table = load_table(table_path).await?;
        let mut stream = table
            .scan()
            .build()
            .map_err(map_iceberg_err("iceberg scan build failed"))?
            .to_arrow()
            .await
            .map_err(map_iceberg_err("iceberg scan to_arrow failed"))?;
        let mut values = Vec::new();
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(map_iceberg_err("iceberg scan read failed"))?
        {
            let idx = batch.schema().index_of(column).map_err(|err| {
                Box::new(floe_core::errors::RunError(format!(
                    "missing column in scan batch: {err}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    Box::new(floe_core::errors::RunError(
                        "expected Int64Array".to_string(),
                    )) as Box<dyn std::error::Error + Send + Sync>
                })?;
            for i in 0..arr.len() {
                values.push(arr.value(i));
            }
        }
        Ok(values)
    })
}

async fn load_table(table_path: &Path) -> FloeResult<iceberg::table::Table> {
    let metadata_location = latest_metadata_location(table_path)?.ok_or_else(|| {
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
        .map_err(map_iceberg_err("iceberg test catalog init failed"))?;
    let namespace = NamespaceIdent::new("floe".to_string());
    if !catalog
        .namespace_exists(&namespace)
        .await
        .map_err(map_iceberg_err("iceberg test namespace exists failed"))?
    {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .map_err(map_iceberg_err("iceberg test namespace create failed"))?;
    }
    let ident = TableIdent::new(namespace, "orders".to_string());
    catalog
        .register_table(&ident, metadata_location)
        .await
        .map_err(map_iceberg_err("iceberg test register table failed"))
}

fn latest_metadata_location(table_path: &Path) -> FloeResult<Option<String>> {
    let metadata_dir = table_path.join("metadata");
    if !metadata_dir.exists() {
        return Ok(None);
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(metadata_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".metadata.json") {
            continue;
        }
        files.push(path);
    }
    files.sort();
    Ok(files.last().map(|p| p.display().to_string()))
}

fn test_runtime() -> FloeResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            Box::new(floe_core::errors::RunError(format!(
                "iceberg test runtime init failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })
}

fn map_iceberg_err(
    context: &'static str,
) -> impl FnOnce(iceberg::Error) -> Box<dyn std::error::Error + Send + Sync> {
    move |err| Box::new(floe_core::errors::RunError(format!("{context}: {err}")))
}
