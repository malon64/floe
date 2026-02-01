use floe_core::io::storage::Target;
use floe_core::io::write::delta::write_delta_table;
use floe_core::{config, FloeResult};
use polars::prelude::{df, ParquetReader, SerReader};
use url::Url;

#[test]
fn write_delta_table_overwrite() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("delta_table");
    let mut df = df!(
        "id" => &[1i64, 2, 3],
        "name" => &["a", "b", "c"]
    )?;
    let config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
        env: None,
        domains: Vec::new(),
        report: None,
        entities: Vec::new(),
    };
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let resolved = resolver.resolve_path(
        "orders",
        "sink.accepted.path",
        None,
        table_path.to_str().unwrap(),
    )?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
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
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: table_path.display().to_string(),
                storage: None,
                options: None,
            },
            rejected: None,
            archive: None,
        },
        policy: config::PolicyConfig {
            severity: "warn".to_string(),
        },
        schema: config::SchemaConfig {
            normalize_columns: None,
            mismatch: None,
            columns: Vec::new(),
        },
    };

    let version1 = write_delta_table(&mut df, &target, &resolver, &entity)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            Box::new(floe_core::errors::RunError(format!(
                "delta test runtime init failed: {err}"
            )))
        })?;
    let table_url = Url::from_directory_path(&table_path).map_err(|_| {
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

    let field_names = table
        .snapshot()?
        .schema()
        .fields()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert!(field_names.contains(&"id".to_string()));

    let mut row_count = 0usize;
    for uri in table.get_file_uris()? {
        let file = std::fs::File::open(&uri)?;
        let df_read = ParquetReader::new(file).finish()?;
        row_count += df_read.height();
    }
    assert_eq!(row_count, df.height());

    let mut df_overwrite = df!(
        "id" => &[4i64, 5],
        "name" => &["d", "e"]
    )?;
    let version2 = write_delta_table(&mut df_overwrite, &target, &resolver, &entity)?;
    assert!(version2 > version1);

    let table_url = Url::from_directory_path(&table_path).map_err(|_| {
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
    assert!(table.version().unwrap_or(0) >= version2);
    let mut row_count = 0usize;
    for uri in table.get_file_uris()? {
        let file = std::fs::File::open(&uri)?;
        let df_read = ParquetReader::new(file).finish()?;
        row_count += df_read.height();
    }
    assert_eq!(row_count, df_overwrite.height());

    let log_dir = table_path.join("_delta_log");
    assert!(log_dir.exists());
    let log_entries = std::fs::read_dir(&log_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
        .count();
    assert!(log_entries >= 2);

    Ok(())
}
