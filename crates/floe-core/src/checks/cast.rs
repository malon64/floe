use polars::prelude::{BooleanChunked, DataFrame, StringChunked};

use crate::{config, ConfigError, FloeResult};
use super::RowError;

/// Detect cast mismatches by comparing raw (string) values to typed values.
/// If a raw value exists but the typed value is null, we treat it as a cast error.
pub fn cast_mismatch_errors(
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
