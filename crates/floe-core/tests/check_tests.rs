use floe_core::check::{build_error_state, cast_error_errors, not_null_errors, RowError};
use floe_core::config::ColumnConfig;
use polars::prelude::{DataFrame, NamedFrom, Series};
use polars::df;

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
fn cast_error_errors_flags_invalid_casts() {
    let raw_df = df!(
        "created_at" => &["2024-01-01", "bad-date"]
    )
    .expect("raw df");
    let typed_series = Series::new("created_at".into(), &[Some(1i64), None]);
    let typed_df = DataFrame::new(vec![typed_series.into()]).expect("typed df");

    let columns = vec![ColumnConfig {
        name: "created_at".to_string(),
        column_type: "datetime".to_string(),
        nullable: Some(true),
        unique: None,
    }];

    let errors = cast_error_errors(&raw_df, &typed_df, &columns).expect("cast errors");

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
fn build_error_state_builds_masks() {
    let errors = vec![
        vec![],
        vec![RowError::new("not_null", "customer_id", "required value missing")],
    ];
    let (accept_rows, errors_json) = build_error_state(errors);

    assert_eq!(accept_rows, vec![true, false]);
    assert!(errors_json[0].is_none());
    assert!(errors_json[1].as_ref().unwrap().contains("not_null"));
}
