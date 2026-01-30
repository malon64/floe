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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> config::RootConfig {
        config::RootConfig {
            version: "0.1".to_string(),
            metadata: None,
            storages: Some(config::StoragesConfig {
                default: Some("s3_raw".to_string()),
                definitions: vec![config::StorageDefinition {
                    name: "s3_raw".to_string(),
                    fs_type: "s3".to_string(),
                    bucket: Some("my-bucket".to_string()),
                    region: Some("eu-west-1".to_string()),
                    prefix: Some("data".to_string()),
                }],
            }),
            env: None,
            domains: Vec::new(),
            report: None,
            entities: Vec::new(),
        }
    }

    #[test]
    fn delta_store_config_builds_s3_url_and_options() -> FloeResult<()> {
        let config = sample_config();
        let resolver = config::StorageResolver::new(&config, std::path::Path::new("."))?;
        let resolved =
            resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
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
                    path: "delta/orders".to_string(),
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

        let store = delta_store_config(&target, &resolver, &entity)?;
        assert_eq!(store.table_url.as_str(), "s3://my-bucket/data/delta/orders");
        assert_eq!(
            store.storage_options.get("region").map(String::as_str),
            Some("eu-west-1")
        );
        assert_eq!(
            store.storage_options.get("bucket").map(String::as_str),
            Some("my-bucket")
        );
        Ok(())
    }

    #[test]
    fn delta_store_config_builds_local_url() -> FloeResult<()> {
        let config = config::RootConfig {
            version: "0.1".to_string(),
            metadata: None,
            storages: None,
            env: None,
            domains: Vec::new(),
            report: None,
            entities: Vec::new(),
        };
        let temp_dir = tempfile::TempDir::new()?;
        let resolver = config::StorageResolver::new(&config, temp_dir.path())?;
        let resolved =
            resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
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
                    path: "delta/orders".to_string(),
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
        let store = delta_store_config(&target, &resolver, &entity)?;
        assert_eq!(store.storage_options.len(), 0);
        assert_eq!(store.table_url.scheme(), "file");
        Ok(())
    }
}
