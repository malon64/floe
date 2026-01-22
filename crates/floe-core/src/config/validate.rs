use std::collections::HashSet;

use crate::config::{EntityConfig, RootConfig};
use crate::{ConfigError, FloeResult};

const ALLOWED_COLUMN_TYPES: &[&str] = &["string", "number", "boolean", "datetime", "date", "time"];
const ALLOWED_SOURCE_FORMATS: &[&str] = &["csv"];
const ALLOWED_CAST_MODES: &[&str] = &["strict", "coerce"];
const ALLOWED_NORMALIZE_STRATEGIES: &[&str] = &["snake_case", "lower", "camel_case", "none"];
const ALLOWED_ACCEPTED_FORMATS: &[&str] = &["parquet"];
const ALLOWED_REJECTED_FORMATS: &[&str] = &["csv"];
const ALLOWED_POLICY_SEVERITIES: &[&str] = &["warn", "reject", "abort"];
const ALLOWED_MISSING_POLICIES: &[&str] = &["reject_file", "fill_nulls"];
const ALLOWED_EXTRA_POLICIES: &[&str] = &["reject_file", "ignore"];

pub(crate) fn validate_config(config: &RootConfig) -> FloeResult<()> {
    if config.entities.is_empty() {
        return Err(Box::new(ConfigError(
            "entities list is empty (at least one entity is required)".to_string(),
        )));
    }

    let mut names = HashSet::new();
    for entity in &config.entities {
        validate_entity(entity)?;
        if !names.insert(entity.name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} is duplicated in config",
                entity.name
            ))));
        }
    }

    Ok(())
}

fn validate_entity(entity: &EntityConfig) -> FloeResult<()> {
    validate_source(entity)?;
    validate_policy(entity)?;
    validate_sink(entity)?;
    validate_schema(entity)?;
    Ok(())
}

fn validate_source(entity: &EntityConfig) -> FloeResult<()> {
    if !ALLOWED_SOURCE_FORMATS.contains(&entity.source.format.as_str()) {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} source.format={} is unsupported (allowed: {})",
            entity.name,
            entity.source.format,
            ALLOWED_SOURCE_FORMATS.join(", ")
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

    Ok(())
}

fn validate_sink(entity: &EntityConfig) -> FloeResult<()> {
    if !ALLOWED_ACCEPTED_FORMATS.contains(&entity.sink.accepted.format.as_str()) {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.format={} is unsupported (allowed: {})",
            entity.name,
            entity.sink.accepted.format,
            ALLOWED_ACCEPTED_FORMATS.join(", ")
        ))));
    }

    if entity.policy.severity == "reject" && entity.sink.rejected.is_none() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.rejected is required when policy.severity=reject",
            entity.name
        ))));
    }

    if let Some(rejected) = &entity.sink.rejected {
        if !ALLOWED_REJECTED_FORMATS.contains(&rejected.format.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} sink.rejected.format={} is unsupported (allowed: {})",
                entity.name,
                rejected.format,
                ALLOWED_REJECTED_FORMATS.join(", ")
            ))));
        }
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
