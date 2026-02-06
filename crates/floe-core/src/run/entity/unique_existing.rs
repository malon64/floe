use std::path::Path;

use deltalake::table::builder::DeltaTableBuilder;

use crate::errors::{RunError, StorageError};
use crate::io::read::parquet::read_parquet_lazy;
use crate::io::storage::{object_store, Target};
use crate::io::write::parts;
use crate::{check, config, io, ConfigError, FloeResult};

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
            let part_files = parts::list_local_part_files(base_path, "parquet")?;
            for part in part_files {
                seed_from_parquet_path(unique_tracker, &part.path, columns, unique_columns)?;
            }
        }
        Target::S3 {
            storage, base_key, ..
        } => {
            let list_prefix = s3_list_prefix(entity, base_key)?;
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(StorageError(format!(
                    "entity.name={} missing temp dir for s3 parquet read",
                    entity.name
                )))
            })?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let objects = client.list(&list_prefix)?;
            for object in objects {
                if !is_part_parquet_key(&object.key) {
                    continue;
                }
                let local_path = client.download_to_temp(&object.uri, temp_dir)?;
                seed_from_parquet_path(unique_tracker, &local_path, columns, unique_columns)?;
            }
        }
        Target::Gcs {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let list_prefix = gcs_list_prefix(entity, bucket, base_key)?;
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(StorageError(format!(
                    "entity.name={} missing temp dir for gcs parquet read",
                    entity.name
                )))
            })?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let objects = client.list(&list_prefix)?;
            for object in objects {
                if !is_part_parquet_key(&object.key) {
                    continue;
                }
                let local_path = client.download_to_temp(&object.uri, temp_dir)?;
                seed_from_parquet_path(unique_tracker, &local_path, columns, unique_columns)?;
            }
        }
        Target::Adls {
            storage,
            container,
            account,
            base_path,
            ..
        } => {
            let list_prefix = adls_list_prefix(entity, container, account, base_path)?;
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(StorageError(format!(
                    "entity.name={} missing temp dir for adls parquet read",
                    entity.name
                )))
            })?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let objects = client.list(&list_prefix)?;
            for object in objects {
                if !is_part_parquet_key(&object.key) {
                    continue;
                }
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
            Target::Local { .. } => Path::new(&uri).to_path_buf(),
            Target::S3 { storage, .. }
            | Target::Gcs { storage, .. }
            | Target::Adls { storage, .. } => {
                let temp_dir = temp_dir.ok_or_else(|| {
                    Box::new(StorageError(format!(
                        "entity.name={} missing temp dir for delta read",
                        entity.name
                    )))
                })?;
                let client = cloud.client_for(resolver, storage, entity)?;
                client.download_to_temp(&uri, temp_dir)?
            }
        };
        seed_from_parquet_path(unique_tracker, &local_path, columns, unique_columns)?;
    }

    Ok(())
}

fn is_part_parquet_key(key: &str) -> bool {
    let Some(file_name) = Path::new(key).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let path = Path::new(file_name);
    if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
        return false;
    }
    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return false;
    };
    let Some(digits) = stem.strip_prefix("part-") else {
        return false;
    };
    if digits.len() < 5 || !digits.bytes().all(|value| value.is_ascii_digit()) {
        return false;
    }
    true
}

fn s3_list_prefix(entity: &config::EntityConfig, base_key: &str) -> FloeResult<String> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for s3 parquet outputs",
            entity.name
        ))));
    }
    Ok(format!("{prefix}/"))
}

fn gcs_list_prefix(
    entity: &config::EntityConfig,
    bucket: &str,
    base_key: &str,
) -> FloeResult<String> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for gcs parquet outputs (bucket={})",
            entity.name, bucket
        ))));
    }
    Ok(format!("{prefix}/"))
}

fn adls_list_prefix(
    entity: &config::EntityConfig,
    container: &str,
    account: &str,
    base_path: &str,
) -> FloeResult<String> {
    let prefix = base_path.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be container root for adls parquet outputs (container={}, account={})",
            entity.name, container, account
        ))));
    }
    Ok(format!("{prefix}/"))
}
