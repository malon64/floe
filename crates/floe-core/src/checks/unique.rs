use polars::prelude::{is_duplicated, is_first_distinct, AnyValue, DataFrame};
use std::collections::{HashMap, HashSet};

use super::{ColumnIndex, RowError, SparseRowErrors};
use crate::errors::RunError;
use crate::{config, FloeResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum UniqueKey {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    String(String),
    Other(String),
}

fn unique_key(value: AnyValue) -> Option<UniqueKey> {
    match value {
        AnyValue::Null => None,
        AnyValue::Boolean(value) => Some(UniqueKey::Bool(value)),
        AnyValue::Int8(value) => Some(UniqueKey::I64(value as i64)),
        AnyValue::Int16(value) => Some(UniqueKey::I64(value as i64)),
        AnyValue::Int32(value) => Some(UniqueKey::I64(value as i64)),
        AnyValue::Int64(value) => Some(UniqueKey::I64(value)),
        AnyValue::Int128(value) => Some(UniqueKey::Other(value.to_string())),
        AnyValue::UInt8(value) => Some(UniqueKey::U64(value as u64)),
        AnyValue::UInt16(value) => Some(UniqueKey::U64(value as u64)),
        AnyValue::UInt32(value) => Some(UniqueKey::U64(value as u64)),
        AnyValue::UInt64(value) => Some(UniqueKey::U64(value)),
        AnyValue::UInt128(value) => Some(UniqueKey::Other(value.to_string())),
        AnyValue::Float32(value) => Some(UniqueKey::F64((value as f64).to_bits())),
        AnyValue::Float64(value) => Some(UniqueKey::F64(value.to_bits())),
        AnyValue::String(value) => Some(UniqueKey::String(value.to_string())),
        AnyValue::StringOwned(value) => Some(UniqueKey::String(value.to_string())),
        other => Some(UniqueKey::Other(other.to_string())),
    }
}

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
            Box::new(RunError(format!("unique column {} not found", column.name)))
        })?;
        let series = df.select_at_idx(*index).ok_or_else(|| {
            Box::new(RunError(format!("unique column {} not found", column.name)))
        })?;
        let series = series.as_materialized_series();
        let non_null = series.len().saturating_sub(series.null_count());
        if non_null == 0 {
            continue;
        }
        let mut duplicate_mask = is_duplicated(series).map_err(|err| {
            Box::new(RunError(format!(
                "unique column {} read failed: {err}",
                column.name
            )))
        })?;
        let not_null = series.is_not_null();
        duplicate_mask = &duplicate_mask & &not_null;
        let mut first_mask = is_first_distinct(series).map_err(|err| {
            Box::new(RunError(format!(
                "unique column {} read failed: {err}",
                column.name
            )))
        })?;
        first_mask = &first_mask & &not_null;
        let mask = duplicate_mask & !first_mask;
        for (row_idx, is_dup) in mask.into_iter().enumerate() {
            if is_dup == Some(true) {
                errors_per_row[row_idx].push(RowError::new(
                    "unique",
                    &column.name,
                    "duplicate value",
                ));
            }
        }
    }

    Ok(errors_per_row)
}

pub fn unique_errors_sparse(
    df: &DataFrame,
    columns: &[config::ColumnConfig],
    indices: &ColumnIndex,
) -> FloeResult<SparseRowErrors> {
    let mut errors = SparseRowErrors::new(df.height());
    if df.height() == 0 {
        return Ok(errors);
    }
    let unique_columns: Vec<&config::ColumnConfig> = columns
        .iter()
        .filter(|col| col.unique == Some(true))
        .collect();
    if unique_columns.is_empty() {
        return Ok(errors);
    }

    for column in unique_columns {
        let index = indices.get(&column.name).ok_or_else(|| {
            Box::new(RunError(format!(
                "unique column {} not found in dataframe",
                column.name
            )))
        })?;
        let series = df
            .select_at_idx(*index)
            .ok_or_else(|| {
                Box::new(RunError(format!(
                    "unique column {} not found in dataframe",
                    column.name
                )))
            })?
            .as_materialized_series()
            .rechunk();
        let mut seen: HashSet<UniqueKey> = HashSet::new();
        for (row_idx, value) in series.iter().enumerate() {
            let key = match unique_key(value) {
                Some(key) => key,
                None => continue,
            };
            if seen.contains(&key) {
                errors.add_error(
                    row_idx,
                    RowError::new("unique", &column.name, "duplicate value"),
                );
            } else {
                seen.insert(key);
            }
        }
    }

    Ok(errors)
}

#[derive(Debug, Default)]
pub struct UniqueTracker {
    seen: HashMap<String, HashSet<UniqueKey>>,
}

impl UniqueTracker {
    pub fn new(columns: &[config::ColumnConfig]) -> Self {
        let mut seen = HashMap::new();
        for column in columns.iter().filter(|col| col.unique == Some(true)) {
            seen.insert(column.name.clone(), HashSet::new());
        }
        Self { seen }
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    pub fn apply(
        &mut self,
        df: &DataFrame,
        columns: &[config::ColumnConfig],
    ) -> FloeResult<Vec<Vec<RowError>>> {
        let mut errors_per_row = vec![Vec::new(); df.height()];
        if df.height() == 0 {
            return Ok(errors_per_row);
        }
        let unique_columns: Vec<&config::ColumnConfig> = columns
            .iter()
            .filter(|col| col.unique == Some(true))
            .collect();
        if unique_columns.is_empty() {
            return Ok(errors_per_row);
        }

        for column in unique_columns {
            let series = df.column(&column.name).map_err(|err| {
                Box::new(RunError(format!(
                    "unique column {} not found: {err}",
                    column.name
                )))
            })?;
            let series = series.as_materialized_series().rechunk();
            let seen = self.seen.get_mut(&column.name).ok_or_else(|| {
                Box::new(RunError(format!(
                    "unique column {} not tracked",
                    column.name
                )))
            })?;
            for (row_idx, value) in series.iter().enumerate() {
                let key = match unique_key(value) {
                    Some(key) => key,
                    None => continue,
                };
                if seen.contains(&key) {
                    errors_per_row[row_idx].push(RowError::new(
                        "unique",
                        &column.name,
                        "duplicate value",
                    ));
                } else {
                    seen.insert(key);
                }
            }
        }

        Ok(errors_per_row)
    }

    pub fn apply_sparse(
        &mut self,
        df: &DataFrame,
        columns: &[config::ColumnConfig],
    ) -> FloeResult<SparseRowErrors> {
        let mut errors = SparseRowErrors::new(df.height());
        if df.height() == 0 {
            return Ok(errors);
        }
        let unique_columns: Vec<&config::ColumnConfig> = columns
            .iter()
            .filter(|col| col.unique == Some(true))
            .collect();
        if unique_columns.is_empty() {
            return Ok(errors);
        }

        for column in unique_columns {
            let series = df.column(&column.name).map_err(|err| {
                Box::new(RunError(format!(
                    "unique column {} not found: {err}",
                    column.name
                )))
            })?;
            let series = series.as_materialized_series().rechunk();
            let seen = self.seen.get_mut(&column.name).ok_or_else(|| {
                Box::new(RunError(format!(
                    "unique column {} not tracked",
                    column.name
                )))
            })?;
            for (row_idx, value) in series.iter().enumerate() {
                let key = match unique_key(value) {
                    Some(key) => key,
                    None => continue,
                };
                if seen.contains(&key) {
                    errors.add_error(
                        row_idx,
                        RowError::new("unique", &column.name, "duplicate value"),
                    );
                } else {
                    seen.insert(key);
                }
            }
        }

        Ok(errors)
    }
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
            Box::new(RunError(format!(
                "unique column {} not found: {err}",
                column.name
            )))
        })?;
        let non_null = series.len().saturating_sub(series.null_count());
        if non_null == 0 {
            continue;
        }
        let unique = series.drop_nulls().n_unique().map_err(|err| {
            Box::new(RunError(format!(
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
