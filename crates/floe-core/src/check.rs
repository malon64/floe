use polars::prelude::{
    BooleanChunked, DataFrame, NamedFrom, NewChunkedArray, Series, StringChunked,
};

use crate::{config, ConfigError, FloeResult};

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

pub fn not_null_errors(
    df: &DataFrame,
    required_cols: &[String],
) -> FloeResult<Vec<Vec<RowError>>> {
    let mut errors_per_row = vec![Vec::new(); df.height()];
    if required_cols.is_empty() {
        return Ok(errors_per_row);
    }

    let mut null_masks = Vec::with_capacity(required_cols.len());
    for name in required_cols {
        let mask = df
            .column(name)
            .map_err(|err| {
                Box::new(ConfigError(format!(
                    "required column {name} not found: {err}"
                )))
            })?
            .is_null();
        null_masks.push(mask);
    }

    for row_idx in 0..df.height() {
        for (col, mask) in required_cols.iter().zip(null_masks.iter()) {
            if mask.get(row_idx).unwrap_or(false) {
                errors_per_row[row_idx].push(RowError::new(
                    "not_null",
                    col,
                    "required value missing",
                ));
            }
        }
    }

    Ok(errors_per_row)
}

pub fn cast_error_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    columns: &[config::ColumnConfig],
) -> FloeResult<Vec<Vec<RowError>>> {
    let mut errors_per_row = vec![Vec::new(); typed_df.height()];
    if typed_df.height() == 0 {
        return Ok(errors_per_row);
    }

    for column in columns {
        if is_string_type(&column.column_type) {
            continue;
        }
        let raw = raw_df
            .column(&column.name)
            .map_err(|err| {
                Box::new(ConfigError(format!(
                    "raw column {} not found: {err}",
                    column.name
                )))
            })?
            .str()
            .map_err(|err| {
                Box::new(ConfigError(format!(
                    "raw column {} is not utf8: {err}",
                    column.name
                )))
            })?;
        let typed_nulls = typed_df
            .column(&column.name)
            .map_err(|err| {
                Box::new(ConfigError(format!(
                    "typed column {} not found: {err}",
                    column.name
                )))
            })?
            .is_null();

        append_cast_errors(&mut errors_per_row, &column.name, raw, &typed_nulls)?;
    }

    Ok(errors_per_row)
}

pub fn build_error_state(
    errors_per_row: Vec<Vec<RowError>>,
) -> (Vec<bool>, Vec<Option<String>>) {
    let mut accept_rows = Vec::with_capacity(errors_per_row.len());
    let mut errors_json = Vec::with_capacity(errors_per_row.len());
    for errors in errors_per_row {
        if errors.is_empty() {
            accept_rows.push(true);
            errors_json.push(None);
        } else {
            accept_rows.push(false);
            let json_items = errors
                .iter()
                .map(RowError::to_json)
                .collect::<Vec<_>>()
                .join(",");
            errors_json.push(Some(format!("[{}]", json_items)));
        }
    }
    (accept_rows, errors_json)
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

fn append_cast_errors(
    errors_per_row: &mut [Vec<RowError>],
    column_name: &str,
    raw: &StringChunked,
    typed_nulls: &BooleanChunked,
) -> FloeResult<()> {
    for row_idx in 0..errors_per_row.len() {
        let raw_value = raw.get(row_idx);
        let typed_is_null = typed_nulls.get(row_idx).unwrap_or(false);
        if typed_is_null && raw_value.is_some() {
            errors_per_row[row_idx].push(RowError::new(
                "cast_error",
                column_name,
                "invalid value for target type",
            ));
        }
    }
    Ok(())
}

fn is_string_type(value: &str) -> bool {
    let normalized = value
        .to_ascii_lowercase()
        .replace('-', "")
        .replace('_', "");
    matches!(normalized.as_str(), "string" | "str" | "text")
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\"', "\\\"")
}
