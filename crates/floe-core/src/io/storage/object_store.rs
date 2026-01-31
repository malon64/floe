use std::collections::HashMap;

use url::Url;

use crate::{config, ConfigError, FloeResult};

use super::Target;

pub struct DeltaStoreConfig {
    pub table_url: Url,
    pub storage_options: HashMap<String, String>,
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
