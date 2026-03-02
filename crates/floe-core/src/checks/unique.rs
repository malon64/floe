use polars::prelude::{AnyValue, DataFrame, Series};
use std::collections::{BTreeMap, HashMap, HashSet};

use super::{ColumnIndex, RowError, SparseRowErrors};
use crate::errors::RunError;
use crate::{config, FloeResult};

const UNIQUE_SAMPLE_LIMIT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum UniqueKey {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    String(String),
    Other(String),
}

impl UniqueKey {
    fn as_string(&self) -> String {
        match self {
            UniqueKey::Bool(value) => value.to_string(),
            UniqueKey::I64(value) => value.to_string(),
            UniqueKey::U64(value) => value.to_string(),
            UniqueKey::F64(value) => f64::from_bits(*value).to_string(),
            UniqueKey::String(value) | UniqueKey::Other(value) => value.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompositeKey(Vec<UniqueKey>);

#[derive(Debug, Clone)]
pub struct UniqueConstraint {
    pub runtime_columns: Vec<String>,
    pub report_columns: Vec<String>,
    pub enforce_reject: bool,
}

#[derive(Debug, Clone)]
pub struct UniqueConstraintSample {
    pub values: BTreeMap<String, String>,
    pub count: u64,
}

#[derive(Debug, Clone)]
pub struct UniqueConstraintResult {
    pub columns: Vec<String>,
    pub duplicates_count: u64,
    pub affected_rows_count: u64,
    pub samples: Vec<UniqueConstraintSample>,
}

#[derive(Debug, Clone)]
struct ConstraintState {
    constraint: UniqueConstraint,
    seen: HashSet<CompositeKey>,
    duplicates_count: u64,
    sample_counts: HashMap<CompositeKey, u64>,
}

#[derive(Debug, Default)]
pub struct UniqueTracker {
    states: Vec<ConstraintState>,
}

impl UniqueTracker {
    pub fn new(columns: &[config::ColumnConfig]) -> Self {
        let constraints = legacy_unique_constraints(columns)
            .into_iter()
            .map(|column| UniqueConstraint {
                runtime_columns: vec![column.clone()],
                report_columns: vec![column],
                enforce_reject: false,
            })
            .collect::<Vec<_>>();
        Self::with_constraints(constraints)
    }

    pub fn with_constraints(constraints: Vec<UniqueConstraint>) -> Self {
        let states = constraints
            .into_iter()
            .map(|constraint| ConstraintState {
                constraint,
                seen: HashSet::new(),
                duplicates_count: 0,
                sample_counts: HashMap::new(),
            })
            .collect();
        Self { states }
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub fn runtime_columns(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut columns = Vec::new();
        for state in &self.states {
            for column in &state.constraint.runtime_columns {
                if seen.insert(column.clone()) {
                    columns.push(column.clone());
                }
            }
        }
        columns
    }

    pub fn seed_from_df(&mut self, df: &DataFrame) -> FloeResult<()> {
        if df.height() == 0 || self.states.is_empty() {
            return Ok(());
        }
        for state in &mut self.states {
            let columns = load_constraint_columns(df, &state.constraint.runtime_columns)?;
            for row_idx in 0..df.height() {
                let key = match composite_key_from_row(&columns, row_idx)? {
                    Some(key) => key,
                    None => continue,
                };
                state.seen.insert(key);
            }
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        df: &DataFrame,
        columns: &[config::ColumnConfig],
    ) -> FloeResult<Vec<Vec<RowError>>> {
        let mut errors_per_row = vec![Vec::new(); df.height()];
        let sparse = self.apply_sparse(df, columns)?;
        for (row_idx, row_errors) in sparse.iter() {
            if let Some(slot) = errors_per_row.get_mut(*row_idx) {
                slot.extend(row_errors.clone());
            }
        }
        Ok(errors_per_row)
    }

    pub fn apply_sparse(
        &mut self,
        df: &DataFrame,
        _columns: &[config::ColumnConfig],
    ) -> FloeResult<SparseRowErrors> {
        let mut forced_reject_rows = HashSet::new();
        self.apply_sparse_with_forced_rejects(df, _columns, &mut forced_reject_rows)
    }

    pub fn apply_sparse_with_forced_rejects(
        &mut self,
        df: &DataFrame,
        _columns: &[config::ColumnConfig],
        forced_reject_rows: &mut HashSet<usize>,
    ) -> FloeResult<SparseRowErrors> {
        let mut errors = SparseRowErrors::new(df.height());
        if df.height() == 0 || self.states.is_empty() {
            return Ok(errors);
        }

        for state in &mut self.states {
            let columns = load_constraint_columns(df, &state.constraint.runtime_columns)?;
            let report_columns = state.constraint.report_columns.clone();
            let (constraint_repr, message) = if report_columns.len() == 1 {
                (report_columns[0].clone(), "duplicate value")
            } else {
                (format!("[{}]", report_columns.join(",")), "duplicate key")
            };
            for row_idx in 0..df.height() {
                let key = match composite_key_from_row(&columns, row_idx)? {
                    Some(key) => key,
                    None => continue,
                };
                if state.seen.contains(&key) {
                    errors.add_error(row_idx, RowError::new("unique", &constraint_repr, message));
                    if state.constraint.enforce_reject {
                        forced_reject_rows.insert(row_idx);
                    }
                    state.duplicates_count += 1;
                    let counter = state.sample_counts.entry(key).or_insert(0);
                    *counter += 1;
                } else {
                    state.seen.insert(key);
                }
            }
        }

        Ok(errors)
    }

    pub fn results(&self) -> Vec<UniqueConstraintResult> {
        self.states
            .iter()
            .map(|state| {
                let mut sample_counts = state
                    .sample_counts
                    .iter()
                    .map(|(key, count)| (key, *count))
                    .collect::<Vec<_>>();
                sample_counts.sort_by(|left, right| {
                    right
                        .1
                        .cmp(&left.1)
                        .then_with(|| format!("{:?}", left.0).cmp(&format!("{:?}", right.0)))
                });
                let samples = sample_counts
                    .into_iter()
                    .take(UNIQUE_SAMPLE_LIMIT)
                    .map(|(key, count)| {
                        let mut values = BTreeMap::new();
                        for (idx, value) in key.0.iter().enumerate() {
                            if let Some(column_name) = state.constraint.report_columns.get(idx) {
                                values.insert(column_name.clone(), value.as_string());
                            }
                        }
                        UniqueConstraintSample { values, count }
                    })
                    .collect::<Vec<_>>();
                UniqueConstraintResult {
                    columns: state.constraint.report_columns.clone(),
                    duplicates_count: state.duplicates_count,
                    affected_rows_count: state.duplicates_count,
                    samples,
                }
            })
            .collect()
    }
}

pub fn unique_errors(
    df: &DataFrame,
    columns: &[config::ColumnConfig],
    _indices: &ColumnIndex,
) -> FloeResult<Vec<Vec<RowError>>> {
    let mut tracker = UniqueTracker::new(columns);
    tracker.apply(df, columns)
}

pub fn unique_errors_sparse(
    df: &DataFrame,
    columns: &[config::ColumnConfig],
    _indices: &ColumnIndex,
) -> FloeResult<SparseRowErrors> {
    let mut tracker = UniqueTracker::new(columns);
    tracker.apply_sparse(df, columns)
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

pub fn resolve_schema_unique_keys(schema: &config::SchemaConfig) -> Vec<Vec<String>> {
    let mut seen = HashSet::new();
    let mut constraints = Vec::new();

    if let Some(unique_keys) = schema.unique_keys.as_ref() {
        for key in unique_keys {
            let normalized = key
                .iter()
                .map(|column| column.trim().to_string())
                .collect::<Vec<_>>();
            if normalized.is_empty() {
                continue;
            }
            let signature = normalized.join("\u{1f}");
            if seen.insert(signature) {
                constraints.push(normalized);
            }
        }
    } else {
        for column in legacy_unique_constraints(&schema.columns) {
            let constraint = vec![column];
            let signature = constraint.join("\u{1f}");
            if seen.insert(signature) {
                constraints.push(constraint);
            }
        }
    }

    if let Some(primary_key) = schema.primary_key.as_ref() {
        let normalized = primary_key
            .iter()
            .map(|column| column.trim().to_string())
            .collect::<Vec<_>>();
        if !normalized.is_empty() {
            let signature = normalized.join("\u{1f}");
            if seen.insert(signature) {
                constraints.push(normalized);
            }
        }
    }

    constraints
}

fn legacy_unique_constraints(columns: &[config::ColumnConfig]) -> Vec<String> {
    columns
        .iter()
        .filter(|col| col.unique == Some(true))
        .map(|col| col.name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

fn load_constraint_columns(df: &DataFrame, columns: &[String]) -> FloeResult<Vec<Series>> {
    let mut output = Vec::with_capacity(columns.len());
    for column in columns {
        let series = df.column(column).map_err(|err| {
            Box::new(RunError(format!(
                "unique constraint column {} not found: {err}",
                column
            )))
        })?;
        output.push(series.as_materialized_series().rechunk());
    }
    Ok(output)
}

fn composite_key_from_row(columns: &[Series], row_idx: usize) -> FloeResult<Option<CompositeKey>> {
    let mut key = Vec::with_capacity(columns.len());
    for series in columns {
        let value = series.get(row_idx).map_err(|err| {
            Box::new(RunError(format!(
                "unique constraint read failed at row {}: {err}",
                row_idx
            )))
        })?;
        let Some(value) = unique_key(value) else {
            return Ok(None);
        };
        key.push(value);
    }
    Ok(Some(CompositeKey(key)))
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
