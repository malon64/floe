use std::collections::HashSet;

use polars::prelude::{AnyValue, DataFrame};

use super::{ColumnIndex, RowError};
use crate::{config, ConfigError, FloeResult};

pub fn unique_errors(
    df: &DataFrame,
    columns: &[config::ColumnConfig],
    indices: &ColumnIndex,
) -> FloeResult<Vec<Vec<RowError>>> {
    let mut errors_per_row = vec![Vec::new(); df.height()];
    let unique_columns: Vec<&config::ColumnConfig> = columns
        .iter()
        .filter(|col| col.unique == Some(true))
        .collect();
    if unique_columns.is_empty() {
        return Ok(errors_per_row);
    }

    for column in unique_columns {
        let index = indices.get(&column.name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "unique column {} not found",
                column.name
            )))
        })?;
        let series = df.select_at_idx(*index).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "unique column {} not found",
                column.name
            )))
        })?;
        let mut seen = HashSet::new();
        for (row_idx, errors) in errors_per_row.iter_mut().enumerate() {
            let value = series.get(row_idx).map_err(|err| {
                Box::new(ConfigError(format!(
                    "unique column {} read failed: {err}",
                    column.name
                )))
            })?;
            if matches!(value, AnyValue::Null) {
                continue;
            }
            let key = value.to_string();
            if !seen.insert(key) {
                errors.push(RowError::new("unique", &column.name, "duplicate value"));
            }
        }
    }

    Ok(errors_per_row)
}

pub fn unique_counts(
    df: &DataFrame,
    columns: &[config::ColumnConfig],
) -> FloeResult<Vec<(String, u64)>> {
    if df.height() == 0 {
        return Ok(Vec::new());
    }

    let unique_columns: Vec<&config::ColumnConfig> = columns
        .iter()
        .filter(|col| col.unique == Some(true))
        .collect();
    if unique_columns.is_empty() {
        return Ok(Vec::new());
    }

    let mut counts = Vec::new();
    for column in unique_columns {
        let series = df.column(&column.name).map_err(|err| {
            Box::new(ConfigError(format!(
                "unique column {} not found: {err}",
                column.name
            )))
        })?;
        let non_null = series.len().saturating_sub(series.null_count());
        if non_null == 0 {
            continue;
        }
        let unique = series.drop_nulls().n_unique().map_err(|err| {
            Box::new(ConfigError(format!(
                "unique column {} read failed: {err}",
                column.name
            )))
        })?;
        let violations = non_null.saturating_sub(unique) as u64;
        if violations > 0 {
            counts.push((column.name.clone(), violations));
        }
    }

    Ok(counts)
}
