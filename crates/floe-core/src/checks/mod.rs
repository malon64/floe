mod cast;
mod mismatch;
mod not_null;
mod unique;

use polars::prelude::{BooleanChunked, NamedFrom, NewChunkedArray, Series};

pub use cast::{cast_mismatch_counts, cast_mismatch_errors};
pub use mismatch::{apply_schema_mismatch, MismatchOutcome};
pub use not_null::{not_null_counts, not_null_errors};
pub use unique::{unique_counts, unique_errors};

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

pub fn build_error_state(errors_per_row: &[Vec<RowError>]) -> (Vec<bool>, Vec<Option<String>>) {
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

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use polars::df;
    use polars::prelude::{DataFrame, NamedFrom, Series};

    #[test]
    fn not_null_errors_flags_missing_required_values() {
        let df = df!(
            "customer_id" => &[Some("A"), None, Some("B")]
        )
        .expect("create df");
        let required = vec!["customer_id".to_string()];

        let errors = not_null_errors(&df, &required).expect("not_null_errors");

        assert!(errors[0].is_empty());
        assert_eq!(
            errors[1],
            vec![RowError::new(
                "not_null",
                "customer_id",
                "required value missing"
            )]
        );
        assert!(errors[2].is_empty());
    }

    #[test]
    fn cast_mismatch_errors_flags_invalid_casts() {
        let raw_df = df!(
            "created_at" => &["2024-01-01", "bad-date"]
        )
        .expect("raw df");
        let typed_series = Series::new("created_at".into(), &[Some(1i64), None]);
        let typed_df = DataFrame::new(vec![typed_series.into()]).expect("typed df");

        let columns = vec![config::ColumnConfig {
            name: "created_at".to_string(),
            column_type: "datetime".to_string(),
            nullable: Some(true),
            unique: None,
        }];

        let errors = cast_mismatch_errors(&raw_df, &typed_df, &columns).expect("cast errors");

        assert!(errors[0].is_empty());
        assert_eq!(
            errors[1],
            vec![RowError::new(
                "cast_error",
                "created_at",
                "invalid value for target type"
            )]
        );
    }

    #[test]
    fn unique_errors_flags_duplicates_after_first() {
        let df = df!(
            "order_id" => &[Some("o-1"), Some("o-2"), Some("o-1"), None, Some("o-2")]
        )
        .expect("create df");

        let columns = vec![config::ColumnConfig {
            name: "order_id".to_string(),
            column_type: "string".to_string(),
            nullable: Some(true),
            unique: Some(true),
        }];

        let errors = unique_errors(&df, &columns).expect("unique errors");

        assert!(errors[0].is_empty());
        assert!(errors[1].is_empty());
        assert_eq!(
            errors[2],
            vec![RowError::new("unique", "order_id", "duplicate value")]
        );
        assert!(errors[3].is_empty());
        assert_eq!(
            errors[4],
            vec![RowError::new("unique", "order_id", "duplicate value")]
        );
    }

    #[test]
    fn build_error_state_builds_masks() {
        let errors = vec![
            vec![],
            vec![RowError::new(
                "not_null",
                "customer_id",
                "required value missing",
            )],
        ];
        let (accept_rows, errors_json) = build_error_state(&errors);

        assert_eq!(accept_rows, vec![true, false]);
        assert!(errors_json[0].is_none());
        assert!(errors_json[1].as_ref().unwrap().contains("not_null"));
    }
}
