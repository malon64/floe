use std::collections::HashMap;

use polars::prelude::DataFrame;

use crate::{config, ConfigError, FloeResult};

pub fn resolve_normalize_strategy(entity: &config::EntityConfig) -> FloeResult<Option<String>> {
    let normalize = match &entity.schema.normalize_columns {
        Some(config) => config.enabled.unwrap_or(false),
        None => false,
    };
    if !normalize {
        return Ok(None);
    }
    let raw = entity
        .schema
        .normalize_columns
        .as_ref()
        .and_then(|config| config.strategy.as_deref())
        .unwrap_or("snake_case");
    let normalized = normalize_strategy_name(raw);
    match normalized.as_str() {
        "snakecase" | "lower" | "camelcase" | "none" => Ok(Some(normalized)),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported normalize_columns.strategy: {raw}"
        )))),
    }
}

pub fn normalize_schema_columns(
    columns: &[config::ColumnConfig],
    strategy: &str,
) -> FloeResult<Vec<config::ColumnConfig>> {
    let mut normalized = Vec::with_capacity(columns.len());
    let mut seen = HashMap::new();
    for column in columns {
        let normalized_name = normalize_name(&column.name, strategy);
        if let Some(existing) = seen.insert(normalized_name.clone(), column.name.clone()) {
            return Err(Box::new(ConfigError(format!(
                "normalized column name collision: {} and {} -> {}",
                existing, column.name, normalized_name
            ))));
        }
        normalized.push(config::ColumnConfig {
            name: normalized_name,
            column_type: column.column_type.clone(),
            nullable: column.nullable,
            unique: column.unique,
        });
    }
    Ok(normalized)
}

pub fn normalize_dataframe_columns(df: &mut DataFrame, strategy: &str) -> FloeResult<()> {
    let names = df.get_column_names();
    let mut normalized_names = Vec::with_capacity(names.len());
    let mut seen = HashMap::new();
    for name in names {
        let normalized = normalize_name(name, strategy);
        if let Some(existing) = seen.insert(normalized.clone(), name.to_string()) {
            return Err(Box::new(ConfigError(format!(
                "normalized input column collision: {} and {} -> {}",
                existing, name, normalized
            ))));
        }
        normalized_names.push(normalized);
    }
    df.set_column_names(normalized_names.iter())
        .map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to normalize column names: {err}"
            )))
        })?;
    Ok(())
}

fn normalize_strategy_name(value: &str) -> String {
    value.to_ascii_lowercase().replace(['-', '_'], "")
}

pub fn normalize_name(value: &str, strategy: &str) -> String {
    match normalize_strategy_name(strategy).as_str() {
        "snakecase" => to_snake_case(value),
        "lower" => value.to_ascii_lowercase(),
        "camelcase" => to_camel_case(value),
        "none" => value.to_string(),
        _ => value.to_string(),
    }
}

fn to_snake_case(value: &str) -> String {
    split_words(value).join("_")
}

fn to_camel_case(value: &str) -> String {
    let words = split_words(value);
    if words.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    out.push_str(&words[0]);
    for word in words.iter().skip(1) {
        out.push_str(&capitalize(word));
    }
    out
}

fn split_words(value: &str) -> Vec<String> {
    let chars: Vec<char> = value.chars().collect();
    let mut words = Vec::new();
    let mut current = String::new();
    for (idx, ch) in chars.iter().copied().enumerate() {
        if !ch.is_ascii_alphanumeric() {
            if !current.is_empty() {
                words.push(current);
                current = String::new();
            }
            continue;
        }

        let is_upper = ch.is_ascii_uppercase();
        let prev = if idx > 0 { Some(chars[idx - 1]) } else { None };
        let next = chars.get(idx + 1).copied();
        let prev_is_lower = prev.map(|c| c.is_ascii_lowercase()).unwrap_or(false);
        let prev_is_digit = prev.map(|c| c.is_ascii_digit()).unwrap_or(false);
        let prev_is_upper = prev.map(|c| c.is_ascii_uppercase()).unwrap_or(false);
        let next_is_lower = next.map(|c| c.is_ascii_lowercase()).unwrap_or(false);

        if !current.is_empty()
            && is_upper
            && ((prev_is_lower || prev_is_digit) || (prev_is_upper && next_is_lower))
        {
            words.push(current);
            current = String::new();
        }

        current.push(ch.to_ascii_lowercase());
    }

    if !current.is_empty() {
        words.push(current);
    }

    words
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}
