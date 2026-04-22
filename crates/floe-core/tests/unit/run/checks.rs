use std::collections::HashMap;

use floe_core::check::{
    accept_mask_from_error_cols, build_accept_rows, build_errors_json, cast_mismatch_errors,
    cast_mismatch_expr, column_index_map, not_null_errors, not_null_expr, row_error_formatter,
    unique_errors, CsvRowErrorFormatter, RowError, RowErrorFormatter, TextRowErrorFormatter,
};
use floe_core::config;
use polars::df;
use polars::prelude::{DataFrame, IntoLazy, NamedFrom, Series};

#[test]
fn not_null_errors_flags_missing_required_values() {
    let df = df!(
        "customer_id" => &[Some("A"), None, Some("B")]
    )
    .expect("create df");
    let required = vec!["customer_id".to_string()];

    let indices = column_index_map(&df);
    let errors = not_null_errors(&df, &required, &indices).expect("not_null_errors");

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
        source: None,
        column_type: "datetime".to_string(),
        nullable: Some(true),
        unique: None,
        width: None,
        trim: None,
    }];

    let raw_indices = column_index_map(&raw_df);
    let typed_indices = column_index_map(&typed_df);
    let errors = cast_mismatch_errors(&raw_df, &typed_df, &columns, &raw_indices, &typed_indices)
        .expect("cast errors");

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
        source: None,
        column_type: "string".to_string(),
        nullable: Some(true),
        unique: Some(true),
        width: None,
        trim: None,
    }];

    let indices = column_index_map(&df);
    let errors = unique_errors(&df, &columns, &indices).expect("unique errors");

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
    let accept_rows = build_accept_rows(&errors);
    let errors_json = build_errors_json(&errors, &accept_rows);

    assert_eq!(accept_rows, vec![true, false]);
    assert!(errors_json[0].is_none());
    assert!(errors_json[1].as_ref().unwrap().contains("not_null"));
}

#[test]
fn csv_formatter_outputs_json_string() {
    let errors = vec![
        RowError::new("not_null", "customer_id", "missing"),
        RowError::new("unique", "order_id", "duplicate"),
    ];
    let formatter = CsvRowErrorFormatter::default();
    let formatted = formatter.format(&errors);
    assert_eq!(
        formatted,
        "\"not_null,customer_id,missing\\nunique,order_id,duplicate\""
    );
}

#[test]
fn csv_formatter_includes_source_when_provided() {
    let errors = vec![RowError::new("not_null", "customer_id", "missing")];
    let mut sources = HashMap::new();
    sources.insert("customer_id".to_string(), "CUSTOMER-ID".to_string());
    let formatter = row_error_formatter("csv", Some(&sources)).expect("formatter");
    let formatted = formatter.format(&errors);
    assert!(formatted.contains("not_null,customer_id,CUSTOMER-ID,missing"));
}

#[test]
fn json_formatter_includes_source_when_provided() {
    let errors = vec![RowError::new("not_null", "customer_id", "missing")];
    let mut sources = HashMap::new();
    sources.insert("customer_id".to_string(), "CUSTOMER-ID".to_string());
    let formatter = row_error_formatter("json", Some(&sources)).expect("formatter");
    let formatted = formatter.format(&errors);
    assert!(formatted.contains("\"source\":\"CUSTOMER-ID\""));
}

#[test]
fn text_formatter_outputs_json_string() {
    let errors = vec![RowError::new("unique", "order_id", "duplicate")];
    let formatter = TextRowErrorFormatter::default();
    let formatted = formatter.format(&errors);
    assert_eq!(formatted, "\"unique:order_id duplicate\"");
}

#[test]
fn text_formatter_includes_source_when_provided() {
    let errors = vec![RowError::new("unique", "order_id", "duplicate")];
    let mut sources = HashMap::new();
    sources.insert("order_id".to_string(), "ORDER-ID".to_string());
    let formatter = row_error_formatter("text", Some(&sources)).expect("formatter");
    let formatted = formatter.format(&errors);
    assert!(formatted.contains("source=ORDER-ID"));
}

#[test]
fn not_null_expr_outputs_nullable_json_error_column() {
    let df = df!("customer_id" => &[Some("A"), None, Some("B")]).expect("create df");
    let (err_col, expr) = not_null_expr("customer_id");

    let out = df
        .lazy()
        .with_columns([expr])
        .collect()
        .expect("collect with not_null expr");

    let errors = out
        .column(&err_col)
        .expect("error column")
        .str()
        .expect("string errors");
    assert_eq!(errors.get(0), None);
    assert_eq!(
        errors.get(1),
        Some("{\"rule\":\"not_null\",\"column\":\"customer_id\",\"message\":\"required value missing\"}")
    );
    assert_eq!(errors.get(2), None);
}

#[test]
fn cast_mismatch_expr_outputs_nullable_json_error_column() {
    let df = df!(
        "created_at_raw" => &[Some("2024-01-01"), Some("bad-date"), None],
        "created_at" => &[Some(1i64), None, None]
    )
    .expect("create df");
    let (err_col, expr) = cast_mismatch_expr("created_at", "created_at_raw");

    let out = df
        .lazy()
        .with_columns([expr])
        .collect()
        .expect("collect with cast expr");

    let errors = out
        .column(&err_col)
        .expect("error column")
        .str()
        .expect("string errors");
    assert_eq!(errors.get(0), None);
    assert_eq!(
        errors.get(1),
        Some("{\"rule\":\"cast_error\",\"column\":\"created_at\",\"message\":\"invalid value for target type\"}")
    );
    assert_eq!(errors.get(2), None);
}

#[test]
fn accept_mask_from_error_cols_accepts_only_rows_with_all_null_error_columns() {
    let df = df!(
        "_e_nn_customer_id" => &[None, Some("missing"), None],
        "_e_cast_created_at" => &[None, None, Some("bad")]
    )
    .expect("create df");

    let mask = accept_mask_from_error_cols(&df, &["_e_nn_customer_id", "_e_cast_created_at"])
        .expect("accept mask");

    assert_eq!(
        mask.into_iter().collect::<Vec<_>>(),
        vec![Some(true), Some(false), Some(false)]
    );
}
