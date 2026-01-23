use std::collections::HashSet;

use crate::config::{EntityConfig, FilesystemDefinition, RootConfig};
use crate::format;
use crate::{ConfigError, FloeResult};

const ALLOWED_COLUMN_TYPES: &[&str] = &["string", "number", "boolean", "datetime", "date", "time"];
const ALLOWED_CAST_MODES: &[&str] = &["strict", "coerce"];
const ALLOWED_NORMALIZE_STRATEGIES: &[&str] = &["snake_case", "lower", "camel_case", "none"];
const ALLOWED_POLICY_SEVERITIES: &[&str] = &["warn", "reject", "abort"];
const ALLOWED_MISSING_POLICIES: &[&str] = &["reject_file", "fill_nulls"];
const ALLOWED_EXTRA_POLICIES: &[&str] = &["reject_file", "ignore"];
const ALLOWED_FILESYSTEM_TYPES: &[&str] = &["local", "s3"];

pub(crate) fn validate_config(config: &RootConfig) -> FloeResult<()> {
    if config.entities.is_empty() {
        return Err(Box::new(ConfigError(
            "entities list is empty (at least one entity is required)".to_string(),
        )));
    }

    let filesystem_registry = FilesystemRegistry::new(config)?;

    let mut names = HashSet::new();
    for entity in &config.entities {
        validate_entity(entity, &filesystem_registry)?;
        if !names.insert(entity.name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} is duplicated in config",
                entity.name
            ))));
        }
    }

    Ok(())
}

fn validate_entity(entity: &EntityConfig, filesystems: &FilesystemRegistry) -> FloeResult<()> {
    validate_source(entity, filesystems)?;
    validate_policy(entity)?;
    validate_sink(entity, filesystems)?;
    validate_schema(entity)?;
    Ok(())
}

fn validate_source(entity: &EntityConfig, filesystems: &FilesystemRegistry) -> FloeResult<()> {
    if format::input_adapter(entity.source.format.as_str()).is_err() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} source.format={} is unsupported",
            entity.name, entity.source.format,
        ))));
    }

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

    let fs_name = filesystems.resolve_name(
        entity,
        "source.filesystem",
        entity.source.filesystem.as_deref(),
    )?;
    filesystems.validate_reference(entity, "source.filesystem", &fs_name)?;

    if entity.source.format == "json" {
        let ndjson = entity
            .source
            .options
            .as_ref()
            .and_then(|options| options.ndjson)
            .unwrap_or(false);
        if !ndjson {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} source.format=json requires source.options.ndjson=true (json array mode not supported yet)",
                entity.name
            ))));
        }
    }

    if entity.source.format == "parquet" {
        if let Some(fs_type) = filesystems.definition_type(&fs_name) {
            if fs_type != "local" {
                return Err(Box::new(ConfigError(format!(
                    "entity.name={} source.format=parquet is only supported on local filesystem (got {})",
                    entity.name, fs_type
                ))));
            }
        }
    }

    Ok(())
}

fn validate_sink(entity: &EntityConfig, filesystems: &FilesystemRegistry) -> FloeResult<()> {
    if format::accepted_sink_adapter(entity.sink.accepted.format.as_str()).is_err() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.format={} is unsupported",
            entity.name, entity.sink.accepted.format,
        ))));
    }

    if entity.policy.severity == "reject" && entity.sink.rejected.is_none() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.rejected is required when policy.severity=reject",
            entity.name
        ))));
    }

    if let Some(rejected) = &entity.sink.rejected {
        if format::rejected_sink_adapter(rejected.format.as_str()).is_err() {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.rejected.format={} is unsupported",
                entity.name, rejected.format
            ))));
        }
    }

    let accepted_fs = filesystems.resolve_name(
        entity,
        "sink.accepted.filesystem",
        entity.sink.accepted.filesystem.as_deref(),
    )?;
    filesystems.validate_reference(entity, "sink.accepted.filesystem", &accepted_fs)?;

    if let Some(rejected) = &entity.sink.rejected {
        let rejected_fs = filesystems.resolve_name(
            entity,
            "sink.rejected.filesystem",
            rejected.filesystem.as_deref(),
        )?;
        filesystems.validate_reference(entity, "sink.rejected.filesystem", &rejected_fs)?;
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

struct FilesystemRegistry {
    has_config: bool,
    default_name: Option<String>,
    definitions: std::collections::HashMap<String, FilesystemDefinition>,
}

impl FilesystemRegistry {
    fn new(config: &RootConfig) -> FloeResult<Self> {
        let Some(filesystems) = &config.filesystems else {
            return Ok(Self {
                has_config: false,
                default_name: None,
                definitions: std::collections::HashMap::new(),
            });
        };

        if filesystems.definitions.is_empty() {
            return Err(Box::new(ConfigError(
                "filesystems.definitions must not be empty".to_string(),
            )));
        }

        let mut definitions = std::collections::HashMap::new();
        for definition in &filesystems.definitions {
            if !ALLOWED_FILESYSTEM_TYPES.contains(&definition.fs_type.as_str()) {
                return Err(Box::new(ConfigError(format!(
                    "filesystems.definitions name={} type={} is unsupported (allowed: {})",
                    definition.name,
                    definition.fs_type,
                    ALLOWED_FILESYSTEM_TYPES.join(", ")
                ))));
            }
            if definition.fs_type == "s3" {
                if definition.bucket.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "filesystems.definitions name={} requires bucket for type s3",
                        definition.name
                    ))));
                }
                if definition.region.is_none() {
                    return Err(Box::new(ConfigError(format!(
                        "filesystems.definitions name={} requires region for type s3",
                        definition.name
                    ))));
                }
            }
            if definitions
                .insert(definition.name.clone(), definition.clone())
                .is_some()
            {
                return Err(Box::new(ConfigError(format!(
                    "filesystems.definitions name={} is duplicated",
                    definition.name
                ))));
            }
        }

        if let Some(default_name) = &filesystems.default {
            if !definitions.contains_key(default_name) {
                return Err(Box::new(ConfigError(format!(
                    "filesystems.default={} does not match any definition",
                    default_name
                ))));
            }
        } else {
            return Err(Box::new(ConfigError(
                "filesystems.default is required when filesystems is set".to_string(),
            )));
        }

        Ok(Self {
            has_config: true,
            default_name: filesystems.default.clone(),
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
                    "entity.name={} {field} requires filesystems.default",
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
                    "entity.name={} {field} references unknown filesystem {} (no filesystems block)",
                    entity.name, name
                ))));
            }
            return Ok(());
        }

        let _definition = self.definitions.get(name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} {field} references unknown filesystem {}",
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
