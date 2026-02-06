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
    clear_parquet_output_parts(target, cloud, resolver, entity)?;
    Ok(parts::PartNameAllocator::from_next_index(0, "parquet"))
}

pub fn rejected_csv_part_allocator(
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<parts::PartNameAllocator> {
    match target {
        Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let _ = parts::clear_local_part_files(base_path, "csv")?;
            parts::PartNameAllocator::from_local_path(base_path, "csv")
        }
        Target::S3 {
            storage, base_key, ..
        } => {
            clear_rejected_csv_remote_parts(
                cloud, resolver, entity, storage, base_key, "s3", "bucket",
            )?;
            Ok(parts::PartNameAllocator::from_next_index(0, "csv"))
        }
        Target::Gcs {
            storage, base_key, ..
        } => {
            clear_rejected_csv_remote_parts(
                cloud, resolver, entity, storage, base_key, "gcs", "bucket",
            )?;
            Ok(parts::PartNameAllocator::from_next_index(0, "csv"))
        }
        Target::Adls {
            storage, base_path, ..
        } => {
            clear_rejected_csv_remote_parts(
                cloud,
                resolver,
                entity,
                storage,
                base_path,
                "adls",
                "container",
            )?;
            Ok(parts::PartNameAllocator::from_next_index(0, "csv"))
        }
    }
}

fn clear_parquet_output_parts(
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<()> {
    match target {
        Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let _ = parts::clear_local_part_files(base_path, "parquet")?;
        }
        Target::S3 {
            storage, base_key, ..
        } => {
            clear_s3_parquet_output_prefix(cloud, resolver, entity, storage, base_key)?;
        }
        Target::Gcs {
            storage,
            bucket,
            base_key,
            ..
        } => {
            clear_gcs_parquet_output_prefix(cloud, resolver, entity, storage, bucket, base_key)?;
        }
        Target::Adls {
            storage,
            container,
            account,
            base_path,
            ..
        } => {
            clear_adls_parquet_output_prefix(
                cloud, resolver, entity, storage, container, account, base_path,
            )?;
        }
    }
    Ok(())
}

fn clear_rejected_csv_remote_parts(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    base_key: &str,
    target_type: &str,
    root_label: &str,
) -> FloeResult<()> {
    let list_prefix = rejected_list_prefix(entity, base_key, target_type, root_label)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let objects = client.list(&list_prefix)?;
    for object in objects
        .into_iter()
        .filter(|obj| obj.key.starts_with(&list_prefix))
        .filter(|obj| parts::parse_part_index_from_key(&obj.key, "csv", 5).is_some())
    {
        client.delete_object(&object.uri)?;
    }
    Ok(())
}

fn clear_s3_parquet_output_prefix(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    base_key: &str,
) -> FloeResult<()> {
    let list_prefix = s3_parquet_list_prefix(entity, base_key)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(&list_prefix)?;
    for object in keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(&list_prefix))
        .filter(|obj| parts::parse_part_index_from_key(&obj.key, "parquet", 1).is_some())
    {
        client.delete_object(&object.uri)?;
    }
    Ok(())
}

fn clear_gcs_parquet_output_prefix(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    bucket: &str,
    base_key: &str,
) -> FloeResult<()> {
    let list_prefix = gcs_parquet_list_prefix(entity, bucket, base_key)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(&list_prefix)?;
    for object in keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(&list_prefix))
        .filter(|obj| parts::parse_part_index_from_key(&obj.key, "parquet", 1).is_some())
    {
        client.delete_object(&object.uri)?;
    }
    Ok(())
}

fn clear_adls_parquet_output_prefix(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    container: &str,
    account: &str,
    base_path: &str,
) -> FloeResult<()> {
    let list_prefix = adls_parquet_list_prefix(entity, container, account, base_path)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(&list_prefix)?;
    for object in keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(&list_prefix))
        .filter(|obj| parts::parse_part_index_from_key(&obj.key, "parquet", 1).is_some())
    {
        client.delete_object(&object.uri)?;
    }
    Ok(())
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
