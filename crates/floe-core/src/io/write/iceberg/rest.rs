use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use iceberg::io::LocalFsStorageFactory;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;

use crate::errors::RunError;
use crate::io::format::AcceptedWritePerfBreakdown;
use crate::{config, FloeResult};

use super::context::{create_table, ensure_namespace, sanitize_table_name};
use super::data_files::write_data_files;
use super::metadata::parse_metadata_version_from_location;
use super::schema::{ensure_partition_spec_matches, ensure_schema_matches};
use super::{map_iceberg_err, IcebergWriteResult, PreparedIcebergWrite, RestIcebergCatalogConfig};

pub(super) async fn write_via_rest_catalog(
    rest_cfg: &RestIcebergCatalogConfig,
    table_root_uri: String,
    prepared: PreparedIcebergWrite,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    small_file_threshold_bytes: u64,
) -> FloeResult<IcebergWriteResult> {
    let catalog = build_rest_catalog(rest_cfg).await?;

    let namespace = NamespaceIdent::new(rest_cfg.namespace.clone());
    ensure_namespace(&catalog, &namespace).await?;

    let table_name = sanitize_table_name(&rest_cfg.table);
    let table_ident = TableIdent::new(namespace.clone(), table_name);

    let table_exists = catalog
        .table_exists(&table_ident)
        .await
        .map_err(map_iceberg_err("iceberg rest table_exists check failed"))?;

    let existing_table = if table_exists {
        let t = catalog
            .load_table(&table_ident)
            .await
            .map_err(map_iceberg_err("iceberg rest load_table failed"))?;
        ensure_schema_matches(
            t.metadata().current_schema(),
            &prepared.iceberg_schema,
            entity,
        )?;
        ensure_partition_spec_matches(
            t.metadata().default_partition_spec(),
            prepared.partition_spec.as_ref(),
            &prepared.iceberg_schema,
            entity,
        )?;
        Some(t)
    } else {
        None
    };

    let mut perf = AcceptedWritePerfBreakdown::default();

    let table = match mode {
        config::WriteMode::Append => match existing_table {
            Some(t) => t,
            None => {
                create_table(
                    &catalog,
                    &namespace,
                    &table_ident,
                    table_root_uri.clone(),
                    &prepared.iceberg_schema,
                    prepared.partition_spec.clone(),
                )
                .await?
            }
        },
        config::WriteMode::Overwrite => {
            if existing_table.is_some() {
                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(map_iceberg_err("iceberg rest drop_table failed"))?;
            }
            create_table(
                &catalog,
                &namespace,
                &table_ident,
                table_root_uri.clone(),
                &prepared.iceberg_schema,
                prepared.partition_spec.clone(),
            )
            .await?
        }
        config::WriteMode::MergeScd1 | config::WriteMode::MergeScd2 => {
            return Err(Box::new(RunError(format!(
                "entity.name={} sink.write_mode={} is only supported for delta accepted sinks",
                entity.name,
                mode.as_str()
            ))));
        }
    };

    let mut file_paths = Vec::new();
    let mut file_sizes = Vec::new();
    let mut files_written = 0_u64;
    let mut table_after_write = table;

    if prepared.batch.num_rows() > 0 {
        let data_write_start = Instant::now();
        let data_files = write_data_files(&table_after_write, prepared.batch).await?;
        perf.data_write_ms = Some(data_write_start.elapsed().as_millis() as u64);
        files_written = data_files.len() as u64;
        file_sizes = data_files
            .iter()
            .map(iceberg::spec::DataFile::file_size_in_bytes)
            .collect();
        file_paths = data_files
            .iter()
            .map(|file| {
                let file_path = file.file_path().to_string();
                Path::new(file_path.as_str())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or(file_path)
            })
            .take(50)
            .collect();

        let tx = Transaction::new(&table_after_write);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx).map_err(map_iceberg_err(
            "iceberg rest append transaction apply failed",
        ))?;
        let commit_start = Instant::now();
        table_after_write = tx
            .commit(&catalog)
            .await
            .map_err(map_iceberg_err("iceberg rest commit failed"))?;
        perf.commit_ms = Some(commit_start.elapsed().as_millis() as u64);
    }

    let snapshot_id = table_after_write
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let metadata_version = table_after_write
        .metadata_location()
        .and_then(parse_metadata_version_from_location);

    let metrics_start = Instant::now();
    let metrics = super::super::metrics::summarize_written_file_sizes(
        &file_sizes,
        files_written,
        small_file_threshold_bytes,
    );
    perf.metrics_read_ms = Some(metrics_start.elapsed().as_millis() as u64);

    Ok(IcebergWriteResult {
        files_written,
        snapshot_id,
        metadata_version,
        file_paths,
        metrics,
        table_root_uri,
        iceberg_catalog_name: Some(rest_cfg.catalog_name.clone()),
        iceberg_database: None,
        iceberg_namespace: Some(rest_cfg.namespace.clone()),
        iceberg_table: Some(rest_cfg.table.clone()),
        perf,
    })
}

pub(crate) async fn build_rest_catalog(cfg: &RestIcebergCatalogConfig) -> FloeResult<impl Catalog> {
    let mut props = HashMap::new();
    props.insert("uri".to_string(), cfg.uri.clone());
    if let Some(cred) = &cfg.credential {
        // iceberg-catalog-rest distinguishes static bearer tokens ("token" key) from
        // OAuth2 client credentials ("credential" key). The Unity Catalog convention
        // encodes static PATs as "token:<value>"; detect and remap accordingly.
        if let Some(token) = cred.strip_prefix("token:") {
            props.insert("token".to_string(), token.to_string());
        } else {
            props.insert("credential".to_string(), cred.clone());
        }
    }
    if let Some(wh) = &cfg.warehouse {
        props.insert("warehouse".to_string(), wh.clone());
    }
    if let Some(oauth_uri) = &cfg.oauth2_server_uri {
        props.insert("oauth2-server-uri".to_string(), oauth_uri.clone());
    }
    if let Some(scope) = &cfg.scope {
        props.insert("scope".to_string(), scope.clone());
    }

    // iceberg-catalog-rest 0.9.x requires a StorageFactory for materializing
    // table FileIO. LocalFsStorageFactory covers local and test scenarios;
    // cloud-backed REST catalogs (S3, GCS) need iceberg-storage-opendal.
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(&cfg.catalog_name, props)
        .await
        .map_err(map_iceberg_err("iceberg rest catalog init failed"))
}
