use polars::prelude::{BooleanChunked, DataFrame, NamedFrom, NewChunkedArray, Series};

use crate::{ConfigError, FloeResult};

pub fn not_null_results(
    df: &DataFrame,
    required_cols: &[String],
) -> FloeResult<(Vec<bool>, Vec<Option<String>>)> {
    if required_cols.is_empty() {
        return Ok((vec![true; df.height()], vec![None; df.height()]));
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

    let mut accept_rows = Vec::with_capacity(df.height());
    let mut errors_per_row = Vec::with_capacity(df.height());
    for row_idx in 0..df.height() {
        let mut errors = Vec::new();
        for (col, mask) in required_cols.iter().zip(null_masks.iter()) {
            if mask.get(row_idx).unwrap_or(false) {
                errors.push(format!(
                    "{{\"rule\":\"not_null\",\"column\":\"{}\",\"message\":\"required value missing\"}}",
                    json_escape(col)
                ));
            }
        }
        if errors.is_empty() {
            accept_rows.push(true);
            errors_per_row.push(None);
        } else {
            accept_rows.push(false);
            errors_per_row.push(Some(format!("[{}]", errors.join(","))));
        }
    }

    Ok((accept_rows, errors_per_row))
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
    value.replace('\\', "\\\\").replace('\"', "\\\"")
}
