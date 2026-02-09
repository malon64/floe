use std::path::Path;

use deltalake::table::builder::DeltaTableBuilder;
use url::Url;

use crate::errors::{RunError, StorageError};
use crate::io::read::parquet::read_parquet_lazy;
use crate::io::storage::{object_store, Target};
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
    entity: &config::EntityConfig,
    columns: &[config::ColumnConfig],
) -> FloeResult<()> {
    if write_mode != config::WriteMode::Append || unique_tracker.is_empty() {
        return Ok(());
    }
    let unique_columns = unique_column_names(columns);
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
            columns,
            &unique_columns,
        ),
        "delta" => seed_from_delta(
            unique_tracker,
            target,
            temp_dir,
            cloud,
            resolver,
            entity,
            columns,
            &unique_columns,
        ),
        _ => Ok(()),
    }
}

fn unique_column_names(columns: &[config::ColumnConfig]) -> Vec<String> {
    columns
        .iter()
        .filter(|col| col.unique == Some(true))
        .map(|col| col.name.clone())
        .collect()
}

fn seed_from_parquet(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    columns: &[config::ColumnConfig],
    unique_columns: &[String],
) -> FloeResult<()> {
    match target {
        Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let part_files = parts::list_local_part_paths(base_path, "parquet")?;
            for part_path in part_files {
                seed_from_parquet_path(unique_tracker, &part_path, columns, unique_columns)?;
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
                seed_from_parquet_path(unique_tracker, &local_path, columns, unique_columns)?;
            }
        }
    }
    Ok(())
}

fn seed_from_parquet_path(
    unique_tracker: &mut check::UniqueTracker,
    path: &Path,
    columns: &[config::ColumnConfig],
    unique_columns: &[String],
) -> FloeResult<()> {
    let df = read_parquet_lazy(path, Some(unique_columns))?;
    unique_tracker.seed_from_df(&df, columns)?;
    Ok(())
}

fn seed_from_delta(
    unique_tracker: &mut check::UniqueTracker,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    columns: &[config::ColumnConfig],
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
        seed_from_parquet_path(unique_tracker, &local_path, columns, unique_columns)?;
    }

    Ok(())
}
