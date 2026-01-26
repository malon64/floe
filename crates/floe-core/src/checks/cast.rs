use polars::prelude::{BooleanChunked, DataFrame, StringChunked};

use super::RowError;
use crate::{config, ConfigError, FloeResult};

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

pub fn cast_mismatch_counts(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    columns: &[config::ColumnConfig],
) -> FloeResult<Vec<(String, u64, String)>> {
    if typed_df.height() == 0 {
        return Ok(Vec::new());
    }

    let mut counts = Vec::new();
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

        let mut violations = 0u64;
        for idx in 0..typed_df.height() {
            let typed_is_null = typed_nulls.get(idx).unwrap_or(false);
            if typed_is_null && raw.get(idx).is_some() {
                violations += 1;
            }
        }

        if violations > 0 {
            counts.push((column.name.clone(), violations, column.column_type.clone()));
        }
    }

    Ok(counts)
}

fn append_cast_errors(
    errors_per_row: &mut [Vec<RowError>],
    column_name: &str,
    raw: &StringChunked,
    typed_nulls: &BooleanChunked,
) -> FloeResult<()> {
    for (row_idx, errors) in errors_per_row.iter_mut().enumerate() {
        let raw_value = raw.get(row_idx);
        let typed_is_null = typed_nulls.get(row_idx).unwrap_or(false);
        if typed_is_null && raw_value.is_some() {
            errors.push(RowError::new(
                "cast_error",
                column_name,
                "invalid value for target type",
            ));
        }
    }
    Ok(())
}

fn is_string_type(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase().replace(['-', '_'], "");
    matches!(normalized.as_str(), "string" | "str" | "text")
}
