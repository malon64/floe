use polars::prelude::{AnyValue, DataFrame};

use super::RowError;
use crate::{ConfigError, FloeResult};

pub fn not_null_errors(df: &DataFrame, required_cols: &[String]) -> FloeResult<Vec<Vec<RowError>>> {
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

    for (row_idx, errors) in errors_per_row.iter_mut().enumerate() {
        for (col, mask) in required_cols.iter().zip(null_masks.iter()) {
            if mask.get(row_idx).unwrap_or(false) {
                errors.push(RowError::new("not_null", col, "required value missing"));
            }
        }
    }

    Ok(errors_per_row)
}

pub fn not_null_counts(df: &DataFrame, required_cols: &[String]) -> FloeResult<Vec<(String, u64)>> {
    if required_cols.is_empty() || df.height() == 0 {
        return Ok(Vec::new());
    }

    let null_counts = df.null_count();
    let mut counts = Vec::new();
    for name in required_cols {
        let series = null_counts.column(name).map_err(|err| {
            Box::new(ConfigError(format!(
                "required column {name} not found: {err}"
            )))
        })?;
        let value = series.get(0).unwrap_or(AnyValue::UInt32(0));
        let violations = match value {
            AnyValue::UInt32(value) => value as u64,
            AnyValue::UInt64(value) => value,
            AnyValue::Int64(value) => value.max(0) as u64,
            AnyValue::Int32(value) => value.max(0) as u64,
            AnyValue::Null => 0,
            _ => 0,
        };
        if violations > 0 {
            counts.push((name.clone(), violations));
        }
    }
    Ok(counts)
}
