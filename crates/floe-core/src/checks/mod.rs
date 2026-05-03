mod cast;
mod mismatch;
pub mod normalize;
mod not_null;
mod unique;

use polars::prelude::{
    BooleanChunked, ChunkFull, DataFrame, Expr, IntoLazy, IntoSeries, NamedFrom, NewChunkedArray,
    Series,
};
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

    pub fn get(&self, row_idx: usize) -> Option<&Vec<RowError>> {
        self.rows.get(&row_idx)
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

/// Result of running expression-based not_null and cast_mismatch checks.
pub struct ExprCheckResult {
    /// True for each row that passed all expression checks.
    pub accept_mask: BooleanChunked,
    /// Per error column: (column_name, is_null mask).
    /// `is_null[i] == Some(false)` means row `i` has an error for that check.
    pub col_masks: Vec<(String, BooleanChunked)>,
    /// Per error column: (column_name, violation_count). Zero-count entries are omitted.
    pub col_violation_counts: Vec<(String, u64)>,
}

impl ExprCheckResult {
    pub fn all_accepted(height: usize) -> Self {
        Self {
            accept_mask: BooleanChunked::full("floe_accept".into(), true, height),
            col_masks: Vec::new(),
            col_violation_counts: Vec::new(),
        }
    }

    pub fn total_violations(&self) -> u64 {
        self.col_violation_counts.iter().map(|(_, c)| c).sum()
    }
}

/// Runs not_null and cast_mismatch checks using Polars columnar operations.
///
/// not_null checks are evaluated via a single lazy expression pass on `df`.
/// cast_mismatch checks are computed directly from `BooleanChunked` masks derived from
/// the raw and typed DataFrames, avoiding the need to combine them into one DataFrame.
///
/// `track_cast` should only be true when cast errors are known to exist (from a prior
/// columnar count pass), so the cast path is skipped entirely on the happy path.
pub fn run_expr_checks(
    df: &DataFrame,
    raw_df: &DataFrame,
    required_cols: &[String],
    columns: &[crate::config::ColumnConfig],
    track_cast: bool,
) -> FloeResult<ExprCheckResult> {
    let mut err_col_names: Vec<String> = Vec::new();

    // Apply not_null expressions in a single lazy pass.
    let not_null_exprs: Vec<Expr> = required_cols
        .iter()
        .map(|col_name| {
            let (err_col, expr) = not_null::not_null_expr(col_name);
            err_col_names.push(err_col);
            expr
        })
        .collect();

    let mut checked = if not_null_exprs.is_empty() {
        df.clone()
    } else {
        df.clone()
            .lazy()
            .with_columns(not_null_exprs)
            .collect()
            .map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "run_expr_checks: not_null evaluation failed: {e}"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?
    };

    // Compute cast error columns directly from BooleanChunked masks, avoiding any
    // need to join or hstack raw and typed DataFrames.
    if track_cast {
        for c in columns {
            if cast::is_string_type(&c.column_type) {
                continue;
            }
            let raw_not_null = raw_df
                .column(&c.name)
                .map_err(|e| {
                    Box::new(crate::errors::RunError(format!(
                        "run_expr_checks: raw column '{}' not found: {e}",
                        c.name
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?
                .is_not_null();

            let typed_null = df
                .column(&c.name)
                .map_err(|e| {
                    Box::new(crate::errors::RunError(format!(
                        "run_expr_checks: typed column '{}' not found: {e}",
                        c.name
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?
                .is_null();

            let error_mask = &typed_null & &raw_not_null;
            let err_col_name = format!("_e_cast_{}", c.name);
            let error_json =
                RowError::new("cast_error", &c.name, "invalid value for target type").to_json();

            let cast_err_series = bool_mask_to_error_series(&err_col_name, error_mask, &error_json);
            checked.with_column(cast_err_series).map_err(|e| {
                Box::new(crate::errors::RunError(format!(
                    "run_expr_checks: could not attach cast error column '{}': {e}",
                    err_col_name
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            err_col_names.push(err_col_name);
        }
    }

    if err_col_names.is_empty() {
        return Ok(ExprCheckResult::all_accepted(df.height()));
    }

    let mut accept_mask = BooleanChunked::full("floe_accept".into(), true, checked.height());
    let mut col_masks: Vec<(String, BooleanChunked)> = Vec::with_capacity(err_col_names.len());
    let mut col_violation_counts: Vec<(String, u64)> = Vec::with_capacity(err_col_names.len());

    for err_col in &err_col_names {
        let col = checked.column(err_col).map_err(|e| {
            Box::new(crate::errors::RunError(format!(
                "run_expr_checks: error column '{err_col}' missing after eval: {e}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let null_mask = col.is_null();
        accept_mask = &accept_mask & &null_mask;
        let violations = (col.len() - col.null_count()) as u64;
        col_violation_counts.push((err_col.clone(), violations));
        col_masks.push((err_col.clone(), null_mask));
    }

    Ok(ExprCheckResult {
        accept_mask,
        col_masks,
        col_violation_counts,
    })
}

/// Converts a boolean error mask into a nullable string Series.
/// Rows where `error_mask` is true get `error_json`; others are null.
fn bool_mask_to_error_series(
    col_name: &str,
    error_mask: BooleanChunked,
    error_json: &str,
) -> Series {
    use polars::prelude::StringChunked;
    let ca: StringChunked = error_mask
        .into_iter()
        .map(|opt_b| {
            if opt_b == Some(true) {
                Some(error_json)
            } else {
                None
            }
        })
        .collect();
    ca.with_name(col_name.into()).into_series()
}

/// Builds the per-row formatted error string for all rejected rows, combining
/// expression-based check errors with unique-check errors.
/// Iterates only rejected rows (the minority), so the happy path is O(1).
pub fn build_errors_formatted_expr(
    height: usize,
    accept_mask: &BooleanChunked,
    col_masks: &[(String, BooleanChunked)],
    unique_errors: &SparseRowErrors,
    formatter: &dyn RowErrorFormatter,
) -> Vec<Option<String>> {
    let mut out = vec![None; height];
    for (row_idx, slot) in out.iter_mut().enumerate() {
        if accept_mask.get(row_idx) == Some(true) {
            continue;
        }
        let mut row_errors: Vec<RowError> = Vec::new();
        for (err_col_name, null_mask) in col_masks {
            if null_mask.get(row_idx) == Some(false) {
                if let Some(col) = err_col_name.strip_prefix("_e_nn_") {
                    row_errors.push(RowError::new("not_null", col, "required value missing"));
                } else if let Some(col) = err_col_name.strip_prefix("_e_cast_") {
                    row_errors.push(RowError::new(
                        "cast_error",
                        col,
                        "invalid value for target type",
                    ));
                }
            }
        }
        if let Some(unique_row_errors) = unique_errors.get(row_idx) {
            row_errors.extend(unique_row_errors.iter().cloned());
        }
        if !row_errors.is_empty() {
            *slot = Some(formatter.format(&row_errors));
        }
    }
    out
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
