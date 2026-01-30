use std::collections::HashSet;

use crate::config::{EntityConfig, RootConfig, StorageDefinition};
use crate::io::format;
use crate::{ConfigError, FloeResult};

const ALLOWED_COLUMN_TYPES: &[&str] = &["string", "number", "boolean", "datetime", "date", "time"];
const ALLOWED_CAST_MODES: &[&str] = &["strict", "coerce"];
const ALLOWED_NORMALIZE_STRATEGIES: &[&str] = &["snake_case", "lower", "camel_case", "none"];
const ALLOWED_POLICY_SEVERITIES: &[&str] = &["warn", "reject", "abort"];
const ALLOWED_MISSING_POLICIES: &[&str] = &["reject_file", "fill_nulls"];
const ALLOWED_EXTRA_POLICIES: &[&str] = &["reject_file", "ignore"];
const ALLOWED_STORAGE_TYPES: &[&str] = &["local", "s3", "adls"];

pub(crate) fn validate_config(config: &RootConfig) -> FloeResult<()> {
    if config.entities.is_empty() {
        return Err(Box::new(ConfigError(
            "entities list is empty (at least one entity is required)".to_string(),
        )));
    }

    let storage_registry = StorageRegistry::new(config)?;

    let mut names = HashSet::new();
    for entity in &config.entities {
        validate_entity(entity, &storage_registry)?;
        if !names.insert(entity.name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} is duplicated in config",
                entity.name
            ))));
        }
    }

    Ok(())
}

fn validate_entity(entity: &EntityConfig, storages: &StorageRegistry) -> FloeResult<()> {
    validate_source(entity, storages)?;
    validate_policy(entity)?;
    validate_sink(entity, storages)?;
    validate_schema(entity)?;
    Ok(())
}

fn validate_source(entity: &EntityConfig, storages: &StorageRegistry) -> FloeResult<()> {
    format::ensure_input_format(&entity.name, entity.source.format.as_str())?;

    if let Some(cast_mode) = &entity.source.cast_mode {
        if !ALLOWED_CAST_MODES.contains(&cast_mode.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} source.cast_mode={} is unsupported (allowed: {})",
                entity.name,
                cast_mode,
                ALLOWED_CAST_MODES.join(", ")
            ))));
        }
    }

    let storage_name =
        storages.resolve_name(entity, "source.storage", entity.source.storage.as_deref())?;
    storages.validate_reference(entity, "source.storage", &storage_name)?;

    if entity.source.format == "json" {
        let options = entity.source.options.as_ref();
        let mode = options
            .and_then(|options| options.json_mode.as_deref())
            .unwrap_or("array");
        match mode {
            "array" | "ndjson" => {}
            _ => {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} source.options.json_mode={} is unsupported (allowed: array, ndjson)",
                    entity.name, mode
                ))));
            }
        }
    }

    if entity.source.format == "parquet" {
        if let Some(storage_type) = storages.definition_type(&storage_name) {
            if storage_type != "local" {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} source.format=parquet is only supported on local storage (got {})",
                    entity.name, storage_type
                ))));
            }
        }
    }

    let _ = storages.definition_type(&storage_name);

    Ok(())
}

fn validate_sink(entity: &EntityConfig, storages: &StorageRegistry) -> FloeResult<()> {
    format::ensure_accepted_sink_format(&entity.name, entity.sink.accepted.format.as_str())?;
    if entity.sink.accepted.format == "iceberg" {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.format=iceberg is not supported yet (storage catalog writer not implemented)",
            entity.name
        ))));
    }
    format::validate_sink_options(
        &entity.name,
        entity.sink.accepted.format.as_str(),
        entity.sink.accepted.options.as_ref(),
    )?;

    if entity.policy.severity == "reject" && entity.sink.rejected.is_none() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.rejected is required when policy.severity=reject",
            entity.name
        ))));
    }

    if let Some(rejected) = &entity.sink.rejected {
        format::ensure_rejected_sink_format(&entity.name, rejected.format.as_str())?;
    }

    let accepted_storage = storages.resolve_name(
        entity,
        "sink.accepted.storage",
        entity.sink.accepted.storage.as_deref(),
    )?;
    storages.validate_reference(entity, "sink.accepted.storage", &accepted_storage)?;
    if entity.sink.accepted.format == "delta" {
        if let Some(storage_type) = storages.definition_type(&accepted_storage) {
            if storage_type != "local" && storage_type != "s3" {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} sink.accepted.format=delta is only supported on local or s3 storage (got {})",
                    entity.name, storage_type
                ))));
            }
        }
    }

    let _ = storages.definition_type(&accepted_storage);

    if let Some(rejected) = &entity.sink.rejected {
        let rejected_storage =
            storages.resolve_name(entity, "sink.rejected.storage", rejected.storage.as_deref())?;
        storages.validate_reference(entity, "sink.rejected.storage", &rejected_storage)?;
        let _ = storages.definition_type(&rejected_storage);
    }

    Ok(())
}

fn validate_policy(entity: &EntityConfig) -> FloeResult<()> {
    if !ALLOWED_POLICY_SEVERITIES.contains(&entity.policy.severity.as_str()) {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} policy.severity={} is unsupported (allowed: {})",
            entity.name,
            entity.policy.severity,
            ALLOWED_POLICY_SEVERITIES.join(", ")
        ))));
    }
    Ok(())
}

fn validate_schema(entity: &EntityConfig) -> FloeResult<()> {
    if let Some(normalize) = &entity.schema.normalize_columns {
        if let Some(strategy) = &normalize.strategy {
            if !is_allowed_normalize_strategy(strategy) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.normalize_columns.strategy={} is unsupported (allowed: {})",
                    entity.name,
                    strategy,
                    ALLOWED_NORMALIZE_STRATEGIES.join(", ")
                ))));
            }
        }
    }

    if let Some(mismatch) = &entity.schema.mismatch {
        if let Some(policy) = &mismatch.missing_columns {
            if !ALLOWED_MISSING_POLICIES.contains(&policy.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.mismatch.missing_columns={} is unsupported (allowed: {})",
                    entity.name,
                    policy,
                    ALLOWED_MISSING_POLICIES.join(", ")
                ))));
            }
        }
        if let Some(policy) = &mismatch.extra_columns {
            if !ALLOWED_EXTRA_POLICIES.contains(&policy.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} schema.mismatch.extra_columns={} is unsupported (allowed: {})",
                    entity.name,
                    policy,
                    ALLOWED_EXTRA_POLICIES.join(", ")
                ))));
            }
        }
    }

    for (index, column) in entity.schema.columns.iter().enumerate() {
        if canonical_column_type(&column.column_type).is_none() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} schema.columns[{}].type={} is unsupported for column {} (allowed: {})",
                entity.name,
                index,
                column.column_type,
                column.name,
                ALLOWED_COLUMN_TYPES.join(", ")
            ))));
        }
    }

    Ok(())
}

fn is_allowed_normalize_strategy(value: &str) -> bool {
    let normalized = normalize_value(value);
    ALLOWED_NORMALIZE_STRATEGIES
        .iter()
        .any(|allowed| normalize_value(allowed).as_str() == normalized)
}

fn canonical_column_type(value: &str) -> Option<&'static str> {
    let normalized = normalize_value(value);
    match normalized.as_str() {
        "string" | "str" | "text" => Some("string"),
        "number" | "float" | "float32" | "float64" | "double" | "decimal" | "int" | "int8"
        | "int16" | "int32" | "int64" | "integer" | "long" | "uint8" | "uint16" | "uint32"
        | "uint64" => Some("number"),
        "boolean" | "bool" => Some("boolean"),
        "datetime" | "timestamp" => Some("datetime"),
        "date" => Some("date"),
        "time" => Some("time"),
        _ => None,
    }
}

fn normalize_value(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['-', '_'], "")
}

struct StorageRegistry {
    has_config: bool,
    default_name: Option<String>,
    definitions: std::collections::HashMap<String, StorageDefinition>,
}

impl StorageRegistry {
    fn new(config: &RootConfig) -> FloeResult<Self> {
        let Some(storages) = &config.storages else {
            return Ok(Self {
                has_config: false,
                default_name: None,
                definitions: std::collections::HashMap::new(),
            });
        };

        if storages.definitions.is_empty() {
            return Err(Box::new(ConfigError(
                "storages.definitions must not be empty".to_string(),
            )));
        }

        let mut definitions = std::collections::HashMap::new();
        for definition in &storages.definitions {
            if !ALLOWED_STORAGE_TYPES.contains(&definition.fs_type.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "storages.definitions name={} type={} is unsupported (allowed: {})",
                    definition.name,
                    definition.fs_type,
                    ALLOWED_STORAGE_TYPES.join(", ")
                ))));
            }
            if definition.fs_type == "s3" {
                if definition.bucket.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires bucket for type s3",
                        definition.name
                    ))));
                }
                if definition.region.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires region for type s3",
                        definition.name
                    ))));
                }
            }
            if definition.fs_type == "adls" {
                if definition.account.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires account for type adls",
                        definition.name
                    ))));
                }
                if definition.container.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} requires container for type adls",
                        definition.name
                    ))));
                }
            }
            if definitions
                .insert(definition.name.clone(), definition.clone())
                .is_some()
            {
                return Err(Box::new(ConfigError(format!(
                    "storages.definitions name={} is duplicated",
                    definition.name
                ))));
            }
        }

        if let Some(default_name) = &storages.default {
            if !definitions.contains_key(default_name) {
                return Err(Box::new(ConfigError(format!(
                    "storages.default={} does not match any definition",
                    default_name
                ))));
            }
        } else {
            return Err(Box::new(ConfigError(
                "storages.default is required when storages is set".to_string(),
            )));
        }

        Ok(Self {
            has_config: true,
            default_name: storages.default.clone(),
            definitions,
        })
    }

    fn resolve_name(
        &self,
        entity: &EntityConfig,
        field: &str,
        override_name: Option<&str>,
    ) -> FloeResult<String> {
        if let Some(name) = override_name {
            return Ok(name.to_string());
        }
        if self.has_config {
            let default_name = self.default_name.clone().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} {field} requires storages.default",
                    entity.name
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            return Ok(default_name);
        }
        Ok("local".to_string())
    }

    fn validate_reference(&self, entity: &EntityConfig, field: &str, name: &str) -> FloeResult<()> {
        if !self.has_config {
            if name != "local" {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} {field} references unknown storage {} (no storages block)",
                    entity.name, name
                ))));
            }
            return Ok(());
        }

        let _definition = self.definitions.get(name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} {field} references unknown storage {}",
                entity.name, name
            )))
        })?;

        Ok(())
    }

    fn definition_type(&self, name: &str) -> Option<&str> {
        if !self.has_config {
            if name == "local" {
                return Some("local");
            }
            return None;
        }
        self.definitions
            .get(name)
            .map(|definition| definition.fs_type.as_str())
    }
}
