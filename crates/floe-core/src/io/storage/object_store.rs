use std::collections::HashMap;

use iceberg::io::{CLIENT_REGION, S3_REGION};
use url::Url;

use crate::{config, ConfigError, FloeResult};

use super::Target;

pub struct DeltaStoreConfig {
    pub table_url: Url,
    pub storage_options: HashMap<String, String>,
}

#[derive(Debug)]
pub struct IcebergStoreConfig {
    pub warehouse_location: String,
    pub file_io_props: HashMap<String, String>,
}

pub fn delta_store_config(
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<DeltaStoreConfig> {
    match target {
        Target::Local { base_path, .. } => {
            let url = Url::from_directory_path(base_path).map_err(|_| {
                Box::new(ConfigError(format!(
                    "entity.name={} delta table path is not a valid url: {}",
                    entity.name, base_path
                )))
            })?;
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options: HashMap::new(),
            })
        }
        Target::S3 {
            storage,
            uri,
            bucket,
            ..
        } => {
            let url = Url::parse(uri).map_err(|err| {
                Box::new(ConfigError(format!(
                    "entity.name={} delta s3 path is invalid: {} ({err})",
                    entity.name, uri
                )))
            })?;
            let mut storage_options = HashMap::new();
            if let Some(definition) = resolver.definition(storage) {
                if let Some(region) = definition.region {
                    storage_options.insert("region".to_string(), region);
                }
            }
            storage_options.insert("bucket".to_string(), bucket.to_string());
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options,
            })
        }
        Target::Adls {
            uri,
            account,
            container,
            ..
        } => {
            let url = Url::parse(uri).map_err(|err| {
                Box::new(ConfigError(format!(
                    "entity.name={} delta adls path is invalid: {} ({err})",
                    entity.name, uri
                )))
            })?;
            let mut storage_options = HashMap::new();
            storage_options.insert(
                "azure_storage_account_name".to_string(),
                account.to_string(),
            );
            storage_options.insert("azure_container_name".to_string(), container.to_string());
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options,
            })
        }
        Target::Gcs { uri, .. } => {
            let url = Url::parse(uri).map_err(|err| {
                Box::new(ConfigError(format!(
                    "entity.name={} delta gcs path is invalid: {} ({err})",
                    entity.name, uri
                )))
            })?;
            Ok(DeltaStoreConfig {
                table_url: url,
                storage_options: HashMap::new(),
            })
        }
    }
}

pub fn iceberg_store_config(
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<IcebergStoreConfig> {
    match target {
        Target::Local { base_path, .. } => Ok(IcebergStoreConfig {
            warehouse_location: base_path.to_string(),
            file_io_props: HashMap::new(),
        }),
        Target::S3 { storage, uri, .. } => {
            let mut file_io_props = HashMap::new();
            if let Some(definition) = resolver.definition(storage) {
                if let Some(region) = definition.region {
                    file_io_props.insert(S3_REGION.to_string(), region.clone());
                    file_io_props.insert(CLIENT_REGION.to_string(), region);
                }
            }
            Ok(IcebergStoreConfig {
                warehouse_location: uri.to_string(),
                file_io_props,
            })
        }
        Target::Adls { .. } | Target::Gcs { .. } => Err(Box::new(ConfigError(format!(
            "entity.name={} iceberg sink is only supported on local or s3 storage for now",
            entity.name
        )))),
    }
}
