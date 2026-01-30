use floe_core::check::{
    build_accept_rows, build_errors_json, cast_mismatch_errors, column_index_map, not_null_errors,
    unique_errors, CsvRowErrorFormatter, RowError, RowErrorFormatter, TextRowErrorFormatter,
};
use floe_core::config;
use polars::df;
use polars::prelude::{DataFrame, NamedFrom, Series};

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
        column_type: "datetime".to_string(),
        nullable: Some(true),
        unique: None,
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
        column_type: "string".to_string(),
        nullable: Some(true),
        unique: Some(true),
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
    let formatter = CsvRowErrorFormatter;
    let formatted = formatter.format(&errors);
    assert_eq!(
        formatted,
        "\"not_null,customer_id,missing\\nunique,order_id,duplicate\""
    );
}

#[test]
fn text_formatter_outputs_json_string() {
    let errors = vec![RowError::new("unique", "order_id", "duplicate")];
    let formatter = TextRowErrorFormatter;
    let formatted = formatter.format(&errors);
    assert_eq!(formatted, "\"unique:order_id duplicate\"");
}
