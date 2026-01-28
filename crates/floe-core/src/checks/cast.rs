use polars::prelude::{BooleanChunked, DataFrame, StringChunked};

use super::{ColumnIndex, RowError};
use crate::errors::RunError;
use crate::{config, FloeResult};

/// Detect cast mismatches by comparing raw (string) values to typed values.
/// If a raw value exists but the typed value is null, we treat it as a cast error.
pub fn cast_mismatch_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    columns: &[config::ColumnConfig],
    raw_indices: &ColumnIndex,
    typed_indices: &ColumnIndex,
) -> FloeResult<Vec<Vec<RowError>>> {
    let mut errors_per_row = vec![Vec::new(); typed_df.height()];
    if typed_df.height() == 0 {
        return Ok(errors_per_row);
    }

    for column in columns {
        if is_string_type(&column.column_type) {
            continue;
        }
        let raw_index = raw_indices
            .get(&column.name)
            .ok_or_else(|| Box::new(RunError(format!("raw column {} not found", column.name))))?;
        let typed_index = typed_indices
            .get(&column.name)
            .ok_or_else(|| Box::new(RunError(format!("typed column {} not found", column.name))))?;
        let raw = raw_df
            .select_at_idx(*raw_index)
            .ok_or_else(|| Box::new(RunError(format!("raw column {} not found", column.name))))?
            .str()
            .map_err(|err| {
                Box::new(RunError(format!(
                    "raw column {} is not utf8: {err}",
                    column.name
                )))
            })?;
        let typed_nulls = typed_df
            .select_at_idx(*typed_index)
            .ok_or_else(|| Box::new(RunError(format!("typed column {} not found", column.name))))?
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
                Box::new(RunError(format!(
                    "raw column {} not found: {err}",
                    column.name
                )))
            })?
            .str()
            .map_err(|err| {
                Box::new(RunError(format!(
                    "raw column {} is not utf8: {err}",
                    column.name
                )))
            })?;
        let typed_nulls = typed_df
            .column(&column.name)
            .map_err(|err| {
                Box::new(RunError(format!(
                    "typed column {} not found: {err}",
                    column.name
                )))
            })?
            .is_null();

        let raw_not_null = raw.is_not_null();
        let violations = (&typed_nulls & &raw_not_null).sum().unwrap_or(0) as u64;

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
    let raw_not_null = raw.is_not_null();
    let invalid_mask = typed_nulls & &raw_not_null;
    for (row_idx, invalid) in invalid_mask.into_iter().enumerate() {
        if invalid == Some(true) {
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
    let normalized = value.to_ascii_lowercase().replace(['-', '_'], "");
    matches!(normalized.as_str(), "string" | "str" | "text")
}
