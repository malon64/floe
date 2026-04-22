mod cast;
mod mismatch;
pub mod normalize;
mod not_null;
mod unique;

use polars::prelude::{BooleanChunked, ChunkFull, DataFrame, NamedFrom, NewChunkedArray, Series};
use std::collections::{BTreeMap, HashMap};

use crate::{ConfigError, FloeResult};

pub use cast::{
    cast_mismatch_counts, cast_mismatch_errors, cast_mismatch_errors_sparse, cast_mismatch_expr,
};
pub use mismatch::{
    apply_mismatch_plan, apply_schema_mismatch, plan_schema_mismatch, resolve_mismatch_columns,
    top_level_declared_columns, MismatchOutcome,
};
pub use not_null::{not_null_counts, not_null_errors, not_null_errors_sparse, not_null_expr};
pub use unique::{
    resolve_schema_unique_keys, unique_counts, unique_errors, unique_errors_sparse,
    UniqueConstraint, UniqueConstraintResult, UniqueTracker,
};

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

#[derive(Debug, Clone, Default)]
pub struct SparseRowErrors {
    row_count: usize,
    rows: BTreeMap<usize, Vec<RowError>>,
}

impl SparseRowErrors {
    pub fn new(row_count: usize) -> Self {
        Self {
            row_count,
            rows: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn add_error(&mut self, row_idx: usize, error: RowError) {
        self.rows.entry(row_idx).or_default().push(error);
    }

    pub fn add_errors(&mut self, row_idx: usize, errors: Vec<RowError>) {
        if errors.is_empty() {
            return;
        }
        self.rows.entry(row_idx).or_default().extend(errors);
    }

    pub fn merge(&mut self, other: SparseRowErrors) {
        for (row_idx, errors) in other.rows {
            self.add_errors(row_idx, errors);
        }
    }

    pub fn accept_rows(&self) -> Vec<bool> {
        let mut accept_rows = vec![true; self.row_count];
        for row_idx in self.rows.keys() {
            if let Some(slot) = accept_rows.get_mut(*row_idx) {
                *slot = false;
            }
        }
        accept_rows
    }

    pub fn build_errors_formatted(&self, formatter: &dyn RowErrorFormatter) -> Vec<Option<String>> {
        let mut errors_out = vec![None; self.row_count];
        for (row_idx, errors) in &self.rows {
            if let Some(slot) = errors_out.get_mut(*row_idx) {
                *slot = Some(formatter.format(errors));
            }
        }
        errors_out
    }

    pub fn iter(&self) -> impl Iterator<Item = (&usize, &Vec<RowError>)> {
        self.rows.iter()
    }

    pub fn error_row_count(&self) -> u64 {
        self.rows.len() as u64
    }

    pub fn violation_count(&self) -> u64 {
        self.rows.values().map(|errors| errors.len() as u64).sum()
    }
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
        self.to_json_with_source(None)
    }

    pub fn to_json_with_source(&self, source: Option<&str>) -> String {
        match source {
            Some(source) => format!(
                "{{\"rule\":\"{}\",\"column\":\"{}\",\"source\":\"{}\",\"message\":\"{}\"}}",
                json_escape(&self.rule),
                json_escape(&self.column),
                json_escape(source),
                json_escape(&self.message)
            ),
            None => format!(
                "{{\"rule\":\"{}\",\"column\":\"{}\",\"message\":\"{}\"}}",
                json_escape(&self.rule),
                json_escape(&self.column),
                json_escape(&self.message)
            ),
        }
    }
}

pub trait RowErrorFormatter {
    fn format(&self, errors: &[RowError]) -> String;
}

#[derive(Default)]
pub struct JsonRowErrorFormatter {
    source_map: Option<HashMap<String, String>>,
}
#[derive(Default)]
pub struct CsvRowErrorFormatter {
    source_map: Option<HashMap<String, String>>,
}
#[derive(Default)]
pub struct TextRowErrorFormatter {
    source_map: Option<HashMap<String, String>>,
}

impl RowErrorFormatter for JsonRowErrorFormatter {
    fn format(&self, errors: &[RowError]) -> String {
        let json_items = errors
            .iter()
            .map(|error| {
                let source = self
                    .source_map
                    .as_ref()
                    .and_then(|map| map.get(&error.column).map(|value| value.as_str()));
                error.to_json_with_source(source)
            })
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
                if let Some(source_map) = self.source_map.as_ref() {
                    let source = source_map
                        .get(&error.column)
                        .map(|value| value.as_str())
                        .unwrap_or("");
                    format!(
                        "{},{},{},{}",
                        csv_escape(&error.rule),
                        csv_escape(&error.column),
                        csv_escape(source),
                        csv_escape(&error.message)
                    )
                } else {
                    format!(
                        "{},{},{}",
                        csv_escape(&error.rule),
                        csv_escape(&error.column),
                        csv_escape(&error.message)
                    )
                }
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
            .map(|error| {
                if let Some(source_map) = self.source_map.as_ref() {
                    if let Some(source) = source_map.get(&error.column) {
                        return format!(
                            "{}:{} source={} {}",
                            error.rule, error.column, source, error.message
                        );
                    }
                }
                format!("{}:{} {}", error.rule, error.column, error.message)
            })
            .collect::<Vec<_>>()
            .join("; ");
        json_string(&text)
    }
}

pub fn row_error_formatter(
    name: &str,
    source_map: Option<&HashMap<String, String>>,
) -> FloeResult<Box<dyn RowErrorFormatter>> {
    match name {
        "json" => Ok(Box::new(JsonRowErrorFormatter {
            source_map: source_map.cloned(),
        })),
        "csv" => Ok(Box::new(CsvRowErrorFormatter {
            source_map: source_map.cloned(),
        })),
        "text" => Ok(Box::new(TextRowErrorFormatter {
            source_map: source_map.cloned(),
        })),
        other => Err(Box::new(ConfigError(format!(
            "unsupported report.formatter: {other}"
        )))),
    }
}

pub fn accept_mask_from_error_cols(
    df: &DataFrame,
    err_cols: &[&str],
) -> FloeResult<BooleanChunked> {
    let mut accept_mask = BooleanChunked::full("floe_accept".into(), true, df.height());
    for err_col in err_cols {
        let errors = df.column(err_col).map_err(|err| {
            Box::new(ConfigError(format!(
                "error column {err_col} not found: {err}"
            )))
        })?;
        let no_error = errors.is_null();
        accept_mask = &accept_mask & &no_error;
    }
    Ok(accept_mask)
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
    let formatter = JsonRowErrorFormatter { source_map: None };
    build_errors_formatted(errors_per_row, accept_rows, &formatter)
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
