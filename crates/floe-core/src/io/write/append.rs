use std::path::Path;

use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

use super::parts;

pub fn parquet_part_allocator(
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<parts::PartNameAllocator> {
    match target {
        Target::Local { base_path, .. } => {
            parts::PartNameAllocator::from_local_path(Path::new(base_path), "parquet")
        }
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            let next_index = next_parquet_cloud_part_index(target, cloud, resolver, entity)?;
            Ok(parts::PartNameAllocator::from_next_index(
                next_index, "parquet",
            ))
        }
    }
}

pub fn rejected_csv_part_allocator(
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<parts::PartNameAllocator> {
    match target {
        Target::Local { base_path, .. } => {
            parts::PartNameAllocator::from_local_path(Path::new(base_path), "csv")
        }
        Target::S3 {
            storage, base_key, ..
        } => {
            let next_index = next_rejected_csv_cloud_part_index(
                cloud, resolver, entity, storage, base_key, "s3", "bucket",
            )?;
            Ok(parts::PartNameAllocator::from_next_index(next_index, "csv"))
        }
        Target::Gcs {
            storage, base_key, ..
        } => {
            let next_index = next_rejected_csv_cloud_part_index(
                cloud, resolver, entity, storage, base_key, "gcs", "bucket",
            )?;
            Ok(parts::PartNameAllocator::from_next_index(next_index, "csv"))
        }
        Target::Adls {
            storage, base_path, ..
        } => {
            let next_index = next_rejected_csv_cloud_part_index(
                cloud,
                resolver,
                entity,
                storage,
                base_path,
                "adls",
                "container",
            )?;
            Ok(parts::PartNameAllocator::from_next_index(next_index, "csv"))
        }
    }
}

fn next_parquet_cloud_part_index(
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<usize> {
    match target {
        Target::Local { .. } => Ok(0),
        Target::S3 {
            storage, base_key, ..
        } => {
            let list_prefix = s3_parquet_list_prefix(entity, base_key)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let keys = client.list(&list_prefix)?;
            next_parquet_part_index_from_objects(keys.into_iter().filter_map(|obj| {
                if obj.key.starts_with(&list_prefix) {
                    parts::parse_part_index_from_key(&obj.key, "parquet", 1)
                } else {
                    None
                }
            }))
        }
        Target::Gcs {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let list_prefix = gcs_parquet_list_prefix(entity, bucket, base_key)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let keys = client.list(&list_prefix)?;
            next_parquet_part_index_from_objects(keys.into_iter().filter_map(|obj| {
                if obj.key.starts_with(&list_prefix) {
                    parts::parse_part_index_from_key(&obj.key, "parquet", 1)
                } else {
                    None
                }
            }))
        }
        Target::Adls {
            storage,
            container,
            account,
            base_path,
            ..
        } => {
            let list_prefix = adls_parquet_list_prefix(entity, container, account, base_path)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let keys = client.list(&list_prefix)?;
            next_parquet_part_index_from_objects(keys.into_iter().filter_map(|obj| {
                if obj.key.starts_with(&list_prefix) {
                    parts::parse_part_index_from_key(&obj.key, "parquet", 1)
                } else {
                    None
                }
            }))
        }
    }
}

fn next_parquet_part_index_from_objects(
    indexes: impl IntoIterator<Item = usize>,
) -> FloeResult<usize> {
    match indexes.into_iter().max() {
        Some(index) => index.checked_add(1).ok_or_else(|| {
            Box::new(ConfigError(
                "parquet part index overflow while preparing append write".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        }),
        None => Ok(0),
    }
}

fn next_rejected_csv_cloud_part_index(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    base_key: &str,
    target_type: &str,
    root_label: &str,
) -> FloeResult<usize> {
    let list_prefix = rejected_list_prefix(entity, base_key, target_type, root_label)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let objects = client.list(&list_prefix)?;
    let mut next_index = 0usize;
    for object in objects {
        if !object.key.starts_with(&list_prefix) || object.key.is_empty() {
            continue;
        }
        let Some(index) = parts::parse_part_index_from_key(&object.key, "csv", 5) else {
            continue;
        };
        next_index = next_index.max(index.saturating_add(1));
    }
    Ok(next_index)
}

fn rejected_list_prefix(
    entity: &config::EntityConfig,
    base_key: &str,
    target_type: &str,
    root_label: &str,
) -> FloeResult<String> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.rejected.path must not be {} root for {} outputs",
            entity.name, root_label, target_type
        ))));
    }
    Ok(format!("{prefix}/"))
}

fn s3_parquet_list_prefix(entity: &config::EntityConfig, base_key: &str) -> FloeResult<String> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for s3 parquet outputs",
            entity.name
        ))));
    }
    Ok(format!("{prefix}/"))
}

fn gcs_parquet_list_prefix(
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

fn adls_parquet_list_prefix(
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
