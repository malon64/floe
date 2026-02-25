use std::collections::HashMap;

use crate::config::{
    CatalogDefinition, EntityConfig, ResolvedPath, RootConfig, SinkTarget, StorageResolver,
};
use crate::{ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub struct CatalogResolver {
    has_config: bool,
    default_name: Option<String>,
    definitions: HashMap<String, CatalogDefinition>,
}

#[derive(Debug, Clone)]
pub struct ResolvedIcebergCatalogTarget {
    pub catalog_name: String,
    pub catalog_type: String,
    pub region: String,
    pub database: String,
    pub namespace: String,
    pub table: String,
    pub table_location: ResolvedPath,
}

impl CatalogResolver {
    pub fn new(config: &RootConfig) -> FloeResult<Self> {
        let Some(catalogs) = &config.catalogs else {
            return Ok(Self {
                has_config: false,
                default_name: None,
                definitions: HashMap::new(),
            });
        };

        let mut definitions = HashMap::new();
        for definition in &catalogs.definitions {
            if definitions
                .insert(definition.name.clone(), definition.clone())
                .is_some()
            {
                return Err(Box::new(ConfigError(format!(
                    "catalogs.definitions name={} is duplicated",
                    definition.name
                ))));
            }
        }

        Ok(Self {
            has_config: true,
            default_name: catalogs.default.clone(),
            definitions,
        })
    }

    pub fn definition(&self, name: &str) -> Option<CatalogDefinition> {
        if !self.has_config {
            return None;
        }
        self.definitions.get(name).cloned()
    }

    pub fn resolve_iceberg_target(
        &self,
        storage_resolver: &StorageResolver,
        entity: &EntityConfig,
        sink: &SinkTarget,
    ) -> FloeResult<Option<ResolvedIcebergCatalogTarget>> {
        let Some(iceberg_cfg) = sink.iceberg.as_ref() else {
            return Ok(None);
        };

        let catalog_name = match iceberg_cfg.catalog.as_deref() {
            Some(name) => name.to_string(),
            None => self.default_name.clone().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.iceberg.catalog is required when no catalogs.default is set",
                    entity.name
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?,
        };
        let definition = self.definition(&catalog_name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.iceberg.catalog references unknown catalog {}",
                entity.name, catalog_name
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let region = definition.region.clone().ok_or_else(|| {
            Box::new(ConfigError(format!(
                "catalogs.definitions name={} requires region for type {}",
                definition.name, definition.catalog_type
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let database = normalize_glue_ident(definition.database.as_deref().ok_or_else(|| {
            Box::new(ConfigError(format!(
                "catalogs.definitions name={} requires database for type {}",
                definition.name, definition.catalog_type
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?);

        let namespace_source = iceberg_cfg
            .namespace
            .as_deref()
            .or(entity.domain.as_deref())
            .unwrap_or(database.as_str());
        let namespace = normalize_glue_ident(namespace_source);
        let table_source = iceberg_cfg.table.as_deref().unwrap_or(entity.name.as_str());
        let table = normalize_glue_ident(table_source);

        let table_location = if let Some(location) = iceberg_cfg.location.as_deref() {
            let storage_name = definition
                .warehouse_storage
                .as_deref()
                .or(sink.storage.as_deref());
            storage_resolver.resolve_path(
                &entity.name,
                "sink.accepted.iceberg.location",
                storage_name,
                location,
            )?
        } else if definition.warehouse_storage.is_some() || definition.warehouse_prefix.is_some() {
            let mut relative = String::new();
            if let Some(prefix) = definition.warehouse_prefix.as_deref() {
                relative.push_str(prefix.trim_matches('/'));
            }
            if !namespace.is_empty() {
                if !relative.is_empty() {
                    relative.push('/');
                }
                relative.push_str(namespace.as_str());
            }
            if !table.is_empty() {
                if !relative.is_empty() {
                    relative.push('/');
                }
                relative.push_str(table.as_str());
            }
            let storage_name = definition
                .warehouse_storage
                .as_deref()
                .or(sink.storage.as_deref());
            storage_resolver.resolve_path(
                &entity.name,
                "sink.accepted.iceberg.location",
                storage_name,
                &relative,
            )?
        } else {
            storage_resolver.resolve_path(
                &entity.name,
                "sink.accepted.storage",
                sink.storage.as_deref(),
                &sink.path,
            )?
        };

        Ok(Some(ResolvedIcebergCatalogTarget {
            catalog_name,
            catalog_type: definition.catalog_type,
            region,
            database,
            namespace,
            table,
            table_location,
        }))
    }
}

fn normalize_glue_ident(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        out.push(mapped);
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}
