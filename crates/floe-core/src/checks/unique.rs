use std::collections::HashSet;

use polars::prelude::{AnyValue, DataFrame};

use super::RowError;
use crate::{config, ConfigError, FloeResult};

pub fn unique_errors(
    df: &DataFrame,
    columns: &[config::ColumnConfig],
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
        let series = df.column(&column.name).map_err(|err| {
            Box::new(ConfigError(format!(
                "unique column {} not found: {err}",
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
