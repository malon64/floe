use std::collections::HashMap;

use crate::config::{
    CatalogDefinition, CatalogTypeConfig, EntityConfig, ResolvedPath, RootConfig, SinkTarget,
    StorageResolver,
};
use crate::{ConfigError, FloeResult};

/// Normalizes an identifier for use as a catalog namespace, table, or database name.
/// Lowercases, replaces non-alphanumeric/non-underscore/non-hyphen chars with `_`,
/// and trims leading/trailing underscores.
pub(crate) fn normalize_catalog_ident(value: &str) -> String {
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

#[derive(Debug, Clone)]
pub struct CatalogResolver {
    has_config: bool,
    default_name: Option<String>,
    definitions: HashMap<String, CatalogDefinition>,
}

/// Fully resolved Iceberg catalog target for a single entity write/seed operation.
/// Type-specific fields (region, database, etc.) are carried in `type_config`.
#[derive(Debug, Clone)]
pub struct ResolvedIcebergCatalogTarget {
    pub catalog_name: String,
    pub type_config: CatalogTypeConfig,
    pub namespace: String,
    pub table: String,
    pub table_location: ResolvedPath,
}

/// Fully resolved Unity Catalog target for a single Delta write operation.
/// The table storage location is taken directly from the write `Target` — Unity Catalog
/// is a post-write registration step and does not influence the write path.
#[derive(Debug, Clone)]
pub struct ResolvedDeltaCatalogTarget {
    pub catalog_name: String,
    pub type_config: CatalogTypeConfig,
    pub schema: String,
    pub table: String,
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

    pub fn default_name(&self) -> Option<String> {
        self.default_name.clone()
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

        // namespace and table names use the shared normalizer — same rules for all catalog types
        let database_for_namespace = match &definition.type_config {
            CatalogTypeConfig::Glue { database, .. } => database.as_str(),
            // REST warehouse is a catalog/bucket identifier (e.g. "my_catalog.my_schema"),
            // not a namespace — use "default" so callers always set namespace/domain explicitly.
            CatalogTypeConfig::Rest { .. } => "default",
            // Unity catalogs are for Delta, not Iceberg — validate.rs prevents this path.
            CatalogTypeConfig::Unity { .. } => {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.iceberg.catalog references a unity catalog, which only supports Delta Lake",
                    entity.name
                ))) as Box<dyn std::error::Error + Send + Sync>);
            }
        };
        let namespace_source = iceberg_cfg
            .namespace
            .as_deref()
            .or(entity.domain.as_deref())
            .unwrap_or(database_for_namespace);
        let namespace = normalize_catalog_ident(namespace_source);
        let table_source = iceberg_cfg.table.as_deref().unwrap_or(entity.name.as_str());
        let table = normalize_catalog_ident(table_source);

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
            type_config: definition.type_config,
            namespace,
            table,
            table_location,
        }))
    }

    /// Resolves a Unity Catalog target for a Delta Lake write.
    ///
    /// Returns `Ok(None)` when `sink.delta` is absent (no catalog registration requested).
    pub fn resolve_delta_target(
        &self,
        entity: &EntityConfig,
        sink: &SinkTarget,
    ) -> FloeResult<Option<ResolvedDeltaCatalogTarget>> {
        let Some(delta_cfg) = sink.delta.as_ref() else {
            return Ok(None);
        };

        let catalog_name = match delta_cfg.catalog.as_deref() {
            Some(name) => name.to_string(),
            None => self.default_name.clone().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.delta.catalog is required when no catalogs.default is set",
                    entity.name
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?,
        };

        let definition = self.definition(&catalog_name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} sink.accepted.delta.catalog references unknown catalog {}",
                entity.name, catalog_name
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        // Schema resolution: entity override → entity domain → catalog definition schema field.
        let schema_source = delta_cfg.schema.as_deref().or(entity.domain.as_deref());
        let schema = match (schema_source, &definition.type_config) {
            (Some(s), _) => normalize_catalog_ident(s),
            (None, CatalogTypeConfig::Unity { schema, .. }) => normalize_catalog_ident(schema),
            (None, _) => "default".to_string(),
        };

        let table_source = delta_cfg.table.as_deref().unwrap_or(entity.name.as_str());
        let table = normalize_catalog_ident(table_source);

        Ok(Some(ResolvedDeltaCatalogTarget {
            catalog_name,
            type_config: definition.type_config,
            schema,
            table,
        }))
    }
}
