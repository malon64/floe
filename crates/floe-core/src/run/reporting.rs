use std::collections::{BTreeMap, HashMap};

use serde_json::{Map, Value};

use crate::{check, config, report};

const RULE_COUNT: usize = 4;
const CAST_ERROR_INDEX: usize = 1;

pub(super) fn summarize_validation(
    errors_per_row: &[Vec<check::RowError>],
    columns: &[config::ColumnConfig],
    severity: report::Severity,
) -> Vec<report::RuleSummary> {
    if errors_per_row.iter().all(|errors| errors.is_empty()) {
        return Vec::new();
    }

    let mut column_types = HashMap::new();
    for column in columns {
        column_types.insert(column.name.clone(), column.column_type.clone());
    }

    let mut accumulators = vec![RuleAccumulator::default(); RULE_COUNT];
    for errors in errors_per_row {
        for error in errors {
            let idx = rule_index(&error.rule);
            let accumulator = &mut accumulators[idx];
            accumulator.violations += 1;
            let target_type = if idx == CAST_ERROR_INDEX {
                column_types.get(&error.column).cloned()
            } else {
                None
            };
            let entry = accumulator
                .columns
                .entry(error.column.clone())
                .or_insert_with(|| ColumnAccumulator {
                    violations: 0,
                    target_type,
                });
            entry.violations += 1;
        }
    }

    let mut rules = Vec::new();
    for (idx, accumulator) in accumulators.iter().enumerate() {
        if accumulator.violations == 0 {
            continue;
        }
        let mut columns = Vec::with_capacity(accumulator.columns.len());
        for (name, column_acc) in &accumulator.columns {
            columns.push(report::ColumnSummary {
                column: name.clone(),
                violations: column_acc.violations,
                target_type: column_acc.target_type.clone(),
            });
        }
        rules.push(report::RuleSummary {
            rule: rule_from_index(idx),
            severity,
            violations: accumulator.violations,
            columns,
        });
    }

    rules
}

#[derive(Debug, Default, Clone)]
struct RuleAccumulator {
    violations: u64,
    columns: BTreeMap<String, ColumnAccumulator>,
}

#[derive(Debug, Default, Clone)]
struct ColumnAccumulator {
    violations: u64,
    target_type: Option<String>,
}

fn rule_index(rule: &str) -> usize {
    match rule {
        "not_null" => 0,
        "cast_error" => 1,
        "unique" => 2,
        "schema_error" => 3,
        _ => 3,
    }
}

fn rule_from_index(idx: usize) -> report::RuleName {
    match idx {
        0 => report::RuleName::NotNull,
        1 => report::RuleName::CastError,
        2 => report::RuleName::Unique,
        _ => report::RuleName::SchemaError,
    }
}

pub(super) fn project_metadata_json(meta: &config::ProjectMetadata) -> Value {
    let mut map = Map::new();
    map.insert("project".to_string(), Value::String(meta.project.clone()));
    if let Some(description) = &meta.description {
        map.insert(
            "description".to_string(),
            Value::String(description.clone()),
        );
    }
    if let Some(owner) = &meta.owner {
        map.insert("owner".to_string(), Value::String(owner.clone()));
    }
    if let Some(tags) = &meta.tags {
        map.insert("tags".to_string(), string_array(tags));
    }
    Value::Object(map)
}

pub(super) fn entity_metadata_json(meta: &config::EntityMetadata) -> Value {
    let mut map = Map::new();
    if let Some(data_product) = &meta.data_product {
        map.insert(
            "data_product".to_string(),
            Value::String(data_product.clone()),
        );
    }
    if let Some(domain) = &meta.domain {
        map.insert("domain".to_string(), Value::String(domain.clone()));
    }
    if let Some(owner) = &meta.owner {
        map.insert("owner".to_string(), Value::String(owner.clone()));
    }
    if let Some(description) = &meta.description {
        map.insert(
            "description".to_string(),
            Value::String(description.clone()),
        );
    }
    if let Some(tags) = &meta.tags {
        map.insert("tags".to_string(), string_array(tags));
    }
    Value::Object(map)
}

pub(super) fn source_options_json(options: &config::SourceOptions) -> Value {
    let mut map = Map::new();
    if let Some(header) = options.header {
        map.insert("header".to_string(), Value::Bool(header));
    }
    if let Some(separator) = &options.separator {
        map.insert("separator".to_string(), Value::String(separator.clone()));
    }
    if let Some(encoding) = &options.encoding {
        map.insert("encoding".to_string(), Value::String(encoding.clone()));
    }
    if let Some(null_values) = &options.null_values {
        map.insert("null_values".to_string(), string_array(null_values));
    }
    if let Some(recursive) = options.recursive {
        map.insert("recursive".to_string(), Value::Bool(recursive));
    }
    if let Some(glob) = &options.glob {
        map.insert("glob".to_string(), Value::String(glob.clone()));
    }
    if let Some(json_mode) = &options.json_mode {
        map.insert("json_mode".to_string(), Value::String(json_mode.clone()));
    }
    Value::Object(map)
}

fn string_array(values: &[String]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|item| Value::String(item.clone()))
            .collect(),
    )
}
