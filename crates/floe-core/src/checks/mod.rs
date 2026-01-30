mod cast;
mod mismatch;
mod not_null;
mod unique;

use polars::prelude::{BooleanChunked, DataFrame, NamedFrom, NewChunkedArray, Series};
use std::collections::HashMap;

use crate::{ConfigError, FloeResult};

pub use cast::{cast_mismatch_counts, cast_mismatch_errors};
pub use mismatch::{
    apply_mismatch_plan, apply_schema_mismatch, plan_schema_mismatch, MismatchOutcome,
};
pub use not_null::{not_null_counts, not_null_errors};
pub use unique::{unique_counts, unique_errors, UniqueTracker};

pub type ColumnIndex = HashMap<String, usize>;

pub fn column_index_map(df: &DataFrame) -> ColumnIndex {
    df.get_column_names()
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.to_string(), idx))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowError {
    pub rule: String,
    pub column: String,
    pub message: String,
}

impl RowError {
    pub fn new(rule: &str, column: &str, message: &str) -> Self {
        Self {
            rule: rule.to_string(),
            column: column.to_string(),
            message: message.to_string(),
        }
    }

    pub fn to_json(&self) -> String {
        format!(
            "{{\"rule\":\"{}\",\"column\":\"{}\",\"message\":\"{}\"}}",
            json_escape(&self.rule),
            json_escape(&self.column),
            json_escape(&self.message)
        )
    }
}

pub trait RowErrorFormatter {
    fn format(&self, errors: &[RowError]) -> String;
}

pub struct JsonRowErrorFormatter;
pub struct CsvRowErrorFormatter;
pub struct TextRowErrorFormatter;

impl RowErrorFormatter for JsonRowErrorFormatter {
    fn format(&self, errors: &[RowError]) -> String {
        let json_items = errors
            .iter()
            .map(RowError::to_json)
            .collect::<Vec<_>>()
            .join(",");
        format!("[{}]", json_items)
    }
}

impl RowErrorFormatter for CsvRowErrorFormatter {
    fn format(&self, errors: &[RowError]) -> String {
        let lines = errors
            .iter()
            .map(|error| {
                format!(
                    "{},{},{}",
                    csv_escape(&error.rule),
                    csv_escape(&error.column),
                    csv_escape(&error.message)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        json_string(&lines)
    }
}

impl RowErrorFormatter for TextRowErrorFormatter {
    fn format(&self, errors: &[RowError]) -> String {
        let text = errors
            .iter()
            .map(|error| format!("{}:{} {}", error.rule, error.column, error.message))
            .collect::<Vec<_>>()
            .join("; ");
        json_string(&text)
    }
}

pub fn row_error_formatter(name: &str) -> FloeResult<Box<dyn RowErrorFormatter>> {
    match name {
        "json" => Ok(Box::new(JsonRowErrorFormatter)),
        "csv" => Ok(Box::new(CsvRowErrorFormatter)),
        "text" => Ok(Box::new(TextRowErrorFormatter)),
        other => Err(Box::new(ConfigError(format!(
            "unsupported report.formatter: {other}"
        )))),
    }
}

pub fn build_accept_rows(errors_per_row: &[Vec<RowError>]) -> Vec<bool> {
    let mut accept_rows = Vec::with_capacity(errors_per_row.len());
    for errors in errors_per_row {
        accept_rows.push(errors.is_empty());
    }
    accept_rows
}

pub fn build_errors_json(
    errors_per_row: &[Vec<RowError>],
    accept_rows: &[bool],
) -> Vec<Option<String>> {
    build_errors_formatted(errors_per_row, accept_rows, &JsonRowErrorFormatter)
}

pub fn build_errors_formatted(
    errors_per_row: &[Vec<RowError>],
    accept_rows: &[bool],
    formatter: &dyn RowErrorFormatter,
) -> Vec<Option<String>> {
    let mut errors_out = Vec::with_capacity(errors_per_row.len());
    for (errors, accepted) in errors_per_row.iter().zip(accept_rows.iter()) {
        if *accepted {
            errors_out.push(None);
            continue;
        }
        errors_out.push(Some(formatter.format(errors)));
    }
    errors_out
}

pub fn build_row_masks(accept_rows: &[bool]) -> (BooleanChunked, BooleanChunked) {
    let reject_rows: Vec<bool> = accept_rows.iter().map(|accepted| !*accepted).collect();
    let accept_mask = BooleanChunked::from_slice("floe_accept".into(), accept_rows);
    let reject_mask = BooleanChunked::from_slice("floe_reject".into(), &reject_rows);
    (accept_mask, reject_mask)
}

pub fn rejected_error_columns(
    errors_per_row: &[Option<String>],
    include_all_rows: bool,
) -> (Series, Series) {
    if include_all_rows {
        let mut row_index = Vec::with_capacity(errors_per_row.len());
        let mut errors = Vec::with_capacity(errors_per_row.len());
        for (idx, err) in errors_per_row.iter().enumerate() {
            row_index.push(idx as u64);
            errors.push(err.clone().unwrap_or_else(|| "[]".to_string()));
        }
        (
            Series::new("__floe_row_index".into(), row_index),
            Series::new("__floe_errors".into(), errors),
        )
    } else {
        let mut row_index = Vec::new();
        let mut errors = Vec::new();
        for (idx, err) in errors_per_row.iter().enumerate() {
            if let Some(err) = err {
                row_index.push(idx as u64);
                errors.push(err.clone());
            }
        }
        (
            Series::new("__floe_row_index".into(), row_index),
            Series::new("__floe_errors".into(), errors),
        )
    }
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

fn csv_escape(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    if escaped.contains(',') || escaped.contains('\n') || escaped.contains('\r') {
        format!("\"{}\"", escaped)
    } else {
        escaped
    }
}
