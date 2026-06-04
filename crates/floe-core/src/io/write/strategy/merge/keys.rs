//! Merge-key and schema-evolution helpers shared across sinks.
//!
//! These operate purely on `config::EntityConfig` (no `deltalake`/`arrow`
//! dependencies), so they live outside the feature-gated Delta merge code: the
//! DuckDB merge path and the always-compiled run pipeline reuse them regardless
//! of which sink Cargo features are enabled.

#[cfg(any(feature = "delta", feature = "duckdb"))]
use std::collections::{HashMap, HashSet};

use crate::config;
#[cfg(any(feature = "delta", feature = "duckdb"))]
use crate::errors::RunError;
#[cfg(any(feature = "delta", feature = "duckdb"))]
use crate::FloeResult;

#[cfg(any(feature = "delta", feature = "duckdb"))]
#[derive(Debug, Clone)]
pub(crate) struct Scd2SystemColumns {
    pub(crate) is_current: String,
    pub(crate) valid_from: String,
    pub(crate) valid_to: String,
}

pub(crate) fn default_schema_evolution_summary(
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> crate::io::format::AcceptedSchemaEvolution {
    let schema_evolution = entity.schema.resolved_schema_evolution();
    crate::io::format::AcceptedSchemaEvolution {
        enabled: entity.sink.accepted.format == "delta"
            && schema_evolution.mode == config::SchemaEvolutionMode::AddColumns
            && matches!(
                mode,
                config::WriteMode::Append
                    | config::WriteMode::Overwrite
                    | config::WriteMode::MergeScd1
                    | config::WriteMode::MergeScd2
            ),
        mode: schema_evolution.mode.as_str().to_string(),
        applied: false,
        added_columns: Vec::new(),
        incompatible_changes_detected: false,
    }
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
pub(crate) fn resolve_merge_key(entity: &config::EntityConfig) -> FloeResult<Vec<String>> {
    let primary_key = entity.schema.primary_key.as_ref().ok_or_else(|| {
        Box::new(RunError(format!(
            "entity.name={} merge write modes require schema.primary_key",
            entity.name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    if primary_key.is_empty() {
        return Err(Box::new(RunError(format!(
            "entity.name={} merge write modes require non-empty schema.primary_key",
            entity.name
        ))));
    }
    Ok(primary_key.clone())
}

/// Resolve the merge key mapped through the output-column naming
/// (`schema.normalize_columns`), so the key matches the *renamed* DataFrame
/// columns the sink actually writes — mirroring how ignore/compare columns are
/// mapped. A primary key `"Customer ID"` stored as `customer_id` therefore yields
/// `customer_id`, not the raw schema name. Columns without a schema entry pass
/// through unchanged.
#[cfg(feature = "duckdb")]
pub(crate) fn resolve_merge_key_output(entity: &config::EntityConfig) -> FloeResult<Vec<String>> {
    let merge_key = resolve_merge_key(entity)?;
    let schema_to_output = schema_to_output_column_name_map(entity)?;
    Ok(merge_key
        .into_iter()
        .map(|key| schema_to_output.get(key.trim()).cloned().unwrap_or(key))
        .collect())
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
pub(crate) fn resolve_merge_column_mappings(
    entity: &config::EntityConfig,
) -> FloeResult<(HashSet<String>, Option<Vec<String>>)> {
    let Some(merge) = entity.sink.accepted.merge.as_ref() else {
        return Ok((HashSet::new(), None));
    };
    let schema_to_output = schema_to_output_column_name_map(entity)?;
    let ignore_columns = merge
        .ignore_columns
        .as_ref()
        .map(|cols| {
            cols.iter()
                .map(|c| c.trim())
                .filter(|c| !c.is_empty())
                .map(|c| {
                    schema_to_output
                        .get(c)
                        .cloned()
                        .unwrap_or_else(|| c.to_string())
                })
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let compare_columns = merge.compare_columns.as_ref().map(|cols| {
        let mut seen = HashSet::new();
        cols.iter()
            .map(|c| c.trim())
            .filter(|c| !c.is_empty())
            .map(|c| {
                schema_to_output
                    .get(c)
                    .cloned()
                    .unwrap_or_else(|| c.to_string())
            })
            .filter(|c| seen.insert(c.clone()))
            .collect::<Vec<_>>()
    });
    Ok((ignore_columns, compare_columns))
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
pub(crate) fn resolve_merge_ignore_columns(
    entity: &config::EntityConfig,
) -> FloeResult<HashSet<String>> {
    let Some(columns) = entity
        .sink
        .accepted
        .merge
        .as_ref()
        .and_then(|merge| merge.ignore_columns.as_ref())
    else {
        return Ok(HashSet::new());
    };

    let schema_to_output = schema_to_output_column_name_map(entity)?;
    let resolved = columns
        .iter()
        .map(|column| column.trim())
        .filter(|column| !column.is_empty())
        .map(|column| {
            schema_to_output
                .get(column)
                .cloned()
                .unwrap_or_else(|| column.to_string())
        })
        .collect::<HashSet<_>>();
    Ok(resolved)
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
fn schema_to_output_column_name_map(
    entity: &config::EntityConfig,
) -> FloeResult<HashMap<String, String>> {
    let normalize_strategy = crate::checks::normalize::resolve_normalize_strategy(entity)?;
    let output_columns = crate::checks::normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize_strategy.as_deref(),
    );
    let mut mapping = HashMap::with_capacity(entity.schema.columns.len());
    for (schema_column, output_column) in entity.schema.columns.iter().zip(output_columns.iter()) {
        mapping.insert(
            schema_column.name.trim().to_string(),
            output_column.name.clone(),
        );
    }
    Ok(mapping)
}

#[cfg(any(feature = "delta", feature = "duckdb"))]
pub(crate) fn resolve_scd2_system_columns(entity: &config::EntityConfig) -> Scd2SystemColumns {
    let scd2 = entity
        .sink
        .accepted
        .merge
        .as_ref()
        .and_then(|merge| merge.scd2.as_ref());
    let is_current = scd2
        .and_then(|value| value.current_flag_column.as_deref())
        .unwrap_or(config::DEFAULT_SCD2_CURRENT_FLAG_COLUMN)
        .trim()
        .to_string();
    let valid_from = scd2
        .and_then(|value| value.valid_from_column.as_deref())
        .unwrap_or(config::DEFAULT_SCD2_VALID_FROM_COLUMN)
        .trim()
        .to_string();
    let valid_to = scd2
        .and_then(|value| value.valid_to_column.as_deref())
        .unwrap_or(config::DEFAULT_SCD2_VALID_TO_COLUMN)
        .trim()
        .to_string();
    Scd2SystemColumns {
        is_current,
        valid_from,
        valid_to,
    }
}
