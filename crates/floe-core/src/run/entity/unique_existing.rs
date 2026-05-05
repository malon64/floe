use std::collections::HashMap;
use std::path::Path;

use deltalake::table::builder::DeltaTableBuilder;
use futures::TryStreamExt;
use url::Url;

use crate::errors::{RunError, StorageError};
use crate::io::read::parquet::read_parquet_lazy;
use crate::io::storage::{object_store, Target};
use crate::io::write::iceberg::metadata::{
    latest_gcs_metadata_location, latest_local_metadata_location, latest_s3_metadata_location,
};
use crate::io::write::iceberg::{ICEBERG_CATALOG_NAME, ICEBERG_NAMESPACE};
use crate::io::write::{parts, strategy};
use crate::{check, config, io, FloeResult};

#[allow(clippy::too_many_arguments)]
pub fn seed_unique_tracker_for_append(
    unique_tracker: &mut check::UniqueTracker,
    write_mode: config::WriteMode,
    accepted_format: &str,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    catalogs: &config::CatalogResolver,
    entity: &config::EntityConfig,
) -> FloeResult<()> {
    if write_mode != config::WriteMode::Append || unique_tracker.is_empty() {
        return Ok(());
    }
    let unique_columns = unique_tracker.runtime_columns();
    if unique_columns.is_empty() {
        return Ok(());
    }
    match accepted_format {
        "parquet" => seed_from_parquet(
            unique_tracker,
            target,
            temp_dir,
            cloud,
            resolver,
            entity,
            &unique_columns,
        ),
        "delta" => seed_from_delta(
            unique_tracker,
            target,
            temp_dir,
            cloud,
            resolver,
            entity,
            &unique_columns,
        ),
        "iceberg" => seed_from_iceberg(
            unique_tracker,
            target,
            temp_dir,
            cloud,
            resolver,
            catalogs,
            entity,
            &unique_columns,
        ),
        _ => Ok(()),
    }
}

fn seed_from_parquet(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<()> {
    match target {
        Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let part_files = parts::list_local_part_paths(base_path, "parquet")?;
            for part_path in part_files {
                seed_from_parquet_path(unique_tracker, &part_path, unique_columns)?;
            }
        }
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(StorageError(format!(
                    "entity.name={} missing temp dir for parquet read",
                    entity.name
                )))
            })?;
            let spec = strategy::accepted_parquet_spec();
            let (list_prefix, objects) = {
                let mut ctx = strategy::WriteContext {
                    target,
                    cloud,
                    resolver,
                    entity,
                };
                strategy::list_part_objects(&mut ctx, spec)?
            };
            let client = cloud.client_for(resolver, target.storage(), entity)?;
            for object in objects
                .into_iter()
                .filter(|obj| obj.key.starts_with(&list_prefix))
                .filter(|obj| parts::is_part_key(&obj.key, spec.extension))
            {
                let local_path = client.download_to_temp(&object.uri, temp_dir)?;
                seed_from_parquet_path(unique_tracker, &local_path, unique_columns)?;
            }
        }
    }
    Ok(())
}

fn seed_from_parquet_path(
    unique_tracker: &mut check::UniqueTracker,
    path: &Path,
    unique_columns: &[String],
) -> FloeResult<()> {
    let df = read_parquet_lazy(path, Some(unique_columns))?;
    unique_tracker.seed_from_df(&df)?;
    Ok(())
}

fn seed_from_iceberg(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    catalogs: &config::CatalogResolver,
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<()> {
    let seed_target = resolve_iceberg_seed_target(target, resolver, catalogs, entity)?;
    let store = object_store::iceberg_store_config(&seed_target, resolver, entity)?;

    let metadata_location: Option<String> = match &seed_target {
        Target::Local { base_path, .. } => {
            latest_local_metadata_location(Path::new(base_path))?
        }
        Target::S3 { storage, base_key, .. } => {
            let client = cloud.client_for(resolver, storage, entity)?;
            latest_s3_metadata_location(client, base_key)?
        }
        Target::Gcs { storage, base_key, .. } => {
            let client = cloud.client_for(resolver, storage, entity)?;
            latest_gcs_metadata_location(client, base_key)?
        }
        Target::Adls { .. } => return Ok(()),
    };
    let Some(metadata_location) = metadata_location else {
        return Ok(());
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("iceberg seed runtime init failed: {err}"))))?;

    let file_uris = runtime
        .block_on(seed_iceberg_file_uris(
            metadata_location,
            store.file_io_props,
            store.warehouse_location,
            &entity.name,
        ))
        .map_err(|err| Box::new(RunError(format!("iceberg seed failed: {err}"))))?;

    for uri in file_uris {
        let local_path = match &seed_target {
            Target::Local { .. } => Url::parse(&uri)
                .ok()
                .and_then(|url| url.to_file_path().ok())
                .unwrap_or_else(|| Path::new(&uri).to_path_buf()),
            Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
                let temp_dir = temp_dir.ok_or_else(|| {
                    Box::new(StorageError(format!(
                        "entity.name={} missing temp dir for iceberg seed read",
                        entity.name
                    )))
                })?;
                let client = cloud.client_for(resolver, seed_target.storage(), entity)?;
                client.download_to_temp(&uri, temp_dir)?
            }
        };
        seed_from_parquet_path(unique_tracker, &local_path, unique_columns)?;
    }

    Ok(())
}

fn resolve_iceberg_seed_target(
    target: &Target,
    resolver: &config::StorageResolver,
    catalogs: &config::CatalogResolver,
    entity: &config::EntityConfig,
) -> FloeResult<Target> {
    if !matches!(target, Target::S3 { .. }) {
        return Ok(target.clone());
    }
    if let Some(glue_target) =
        catalogs.resolve_iceberg_target(resolver, entity, &entity.sink.accepted)?
    {
        if glue_target.catalog_type == "glue" {
            return Target::from_resolved(&glue_target.table_location);
        }
    }
    Ok(target.clone())
}

async fn seed_iceberg_file_uris(
    metadata_location: String,
    catalog_props: HashMap<String, String>,
    warehouse_location: String,
    entity_name: &str,
) -> Result<Vec<String>, iceberg::Error> {
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

    let mut props = catalog_props;
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_location);

    let catalog = MemoryCatalogBuilder::default()
        .load(ICEBERG_CATALOG_NAME, props)
        .await?;

    let namespace = NamespaceIdent::new(ICEBERG_NAMESPACE.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await?;
    }

    let table_name = iceberg_table_name(entity_name);
    let table_ident = TableIdent::new(namespace, table_name);

    let table = match catalog.register_table(&table_ident, metadata_location).await {
        Ok(table) => table,
        Err(err) if is_iceberg_not_found_error(&err) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    let scan = table.scan().build()?;
    let mut file_stream = scan.plan_files().await?;
    let mut file_uris = Vec::new();
    while let Some(task) = file_stream.try_next().await? {
        file_uris.push(task.data_file_path().to_string());
    }
    Ok(file_uris)
}


fn is_iceberg_not_found_error(err: &iceberg::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("not found") || msg.contains("does not exist")
}

fn iceberg_table_name(entity_name: &str) -> String {
    let mut out = String::with_capacity(entity_name.len());
    for ch in entity_name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "table".to_string()
    } else {
        out
    }
}

fn seed_from_delta(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    unique_columns: &[String],
) -> FloeResult<()> {
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let table_url = store.table_url;
    let storage_options = store.storage_options;
    let builder = DeltaTableBuilder::from_url(table_url.clone())
        .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
        .with_storage_options(storage_options.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("delta runtime init failed: {err}"))))?;
    let table = runtime.block_on(async move { builder.load().await });
    let table = match table {
        Ok(table) => table,
        Err(err) => match err {
            deltalake::DeltaTableError::NotATable(_) => return Ok(()),
            other => return Err(Box::new(RunError(format!("delta load failed: {other}")))),
        },
    };
    let file_uris = table
        .get_file_uris()
        .map_err(|err| Box::new(RunError(format!("delta list files failed: {err}"))))?
        .collect::<Vec<_>>();
    for uri in file_uris {
        let local_path = match target {
            Target::Local { .. } => {
                let parsed = Url::parse(&uri)
                    .ok()
                    .and_then(|url| url.to_file_path().ok())
                    .unwrap_or_else(|| Path::new(&uri).to_path_buf());
                parsed
            }
            Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
                let temp_dir = temp_dir.ok_or_else(|| {
                    Box::new(StorageError(format!(
                        "entity.name={} missing temp dir for delta read",
                        entity.name
                    )))
                })?;
                let client = cloud.client_for(resolver, target.storage(), entity)?;
                client.download_to_temp(&uri, temp_dir)?
            }
        };
        seed_from_parquet_path(unique_tracker, &local_path, unique_columns)?;
    }

    Ok(())
}
