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

#[test]
fn run_expr_checks_detects_not_null_violations() {
    use floe_core::check::run_expr_checks;

    let df = df!("customer_id" => &[Some("A"), None, Some("B")]).expect("create df");
    let raw_df = df.clone();
    let required = vec!["customer_id".to_string()];
    let columns = vec![config::ColumnConfig {
        name: "customer_id".to_string(),
        source: None,
        column_type: "string".to_string(),
        nullable: None,
        unique: None,
        width: None,
        trim: None,
    }];

    let result =
        run_expr_checks(&df, &raw_df, &required, &columns, false).expect("run_expr_checks");

    assert_eq!(result.col_violation_counts.len(), 1);
    assert_eq!(result.col_violation_counts[0].0, "_e_nn_customer_id");
    assert_eq!(result.col_violation_counts[0].1, 1);
    assert_eq!(result.accept_mask.get(0), Some(true));
    assert_eq!(result.accept_mask.get(1), Some(false));
    assert_eq!(result.accept_mask.get(2), Some(true));
}

#[test]
fn run_expr_checks_detects_cast_mismatch_violations() {
    use floe_core::check::run_expr_checks;

    // raw has a string value but typed is null → cast error on row 1; row 2 raw is null → no error
    let raw_df = df!("score" => &[Some("10"), Some("bad"), None]).expect("raw df");
    let typed_series = Series::new("score".into(), &[Some(10i64), None, None]);
    let typed_df = DataFrame::new(vec![typed_series.into()]).expect("typed df");

    let columns = vec![config::ColumnConfig {
        name: "score".to_string(),
        source: None,
        column_type: "integer".to_string(),
        nullable: Some(true),
        unique: None,
        width: None,
        trim: None,
    }];

    let result = run_expr_checks(&typed_df, &raw_df, &[], &columns, true).expect("run_expr_checks");

    assert_eq!(result.col_violation_counts.len(), 1);
    assert_eq!(result.col_violation_counts[0].0, "_e_cast_score");
    assert_eq!(result.col_violation_counts[0].1, 1);
    assert_eq!(result.accept_mask.get(0), Some(true));
    assert_eq!(result.accept_mask.get(1), Some(false));
    assert_eq!(result.accept_mask.get(2), Some(true));
}

#[test]
fn run_expr_checks_accepts_all_rows_when_no_violations() {
    use floe_core::check::run_expr_checks;

    let df = df!("id" => &[Some("a"), Some("b")]).expect("create df");
    let raw_df = df.clone();
    let required = vec!["id".to_string()];
    let columns = vec![config::ColumnConfig {
        name: "id".to_string(),
        source: None,
        column_type: "string".to_string(),
        nullable: None,
        unique: None,
        width: None,
        trim: None,
    }];

    let result =
        run_expr_checks(&df, &raw_df, &required, &columns, false).expect("run_expr_checks");

    assert_eq!(result.total_violations(), 0);
    assert!(result.accept_mask.into_iter().all(|v| v == Some(true)));
}

#[test]
fn build_errors_formatted_expr_formats_rejected_rows_only() {
    use floe_core::check::{build_errors_formatted_expr, SparseRowErrors};
    use polars::prelude::{BooleanChunked, NewChunkedArray};

    let height = 3;
    let accept_mask = BooleanChunked::from_slice("floe_accept".into(), &[true, false, true]);
    // null_mask: true = no error (error col is null), false = has error (error col has value)
    let null_mask = BooleanChunked::from_slice("_e_nn_col_a".into(), &[true, false, true]);
    let col_masks = vec![("_e_nn_col_a".to_string(), null_mask)];
    let unique_errors = SparseRowErrors::new(height);
    let formatter = TextRowErrorFormatter::default();

    let out =
        build_errors_formatted_expr(height, &accept_mask, &col_masks, &unique_errors, &formatter);

    assert!(out[0].is_none());
    assert!(out[2].is_none());
    let msg = out[1].as_ref().expect("row 1 must have error");
    assert!(msg.contains("not_null"), "expected 'not_null' in: {msg}");
    assert!(msg.contains("col_a"), "expected 'col_a' in: {msg}");
}

#[test]
fn build_errors_formatted_expr_merges_unique_errors() {
    use floe_core::check::{build_errors_formatted_expr, SparseRowErrors};
    use polars::prelude::{BooleanChunked, NewChunkedArray};

    let height = 2;
    let accept_mask = BooleanChunked::from_slice("floe_accept".into(), &[true, false]);
    let col_masks: Vec<(String, BooleanChunked)> = vec![];
    let mut unique_errors = SparseRowErrors::new(height);
    unique_errors.add_error(1, RowError::new("unique", "order_id", "duplicate value"));
    let formatter = TextRowErrorFormatter::default();

    let out =
        build_errors_formatted_expr(height, &accept_mask, &col_masks, &unique_errors, &formatter);

    assert!(out[0].is_none());
    let msg = out[1].as_ref().expect("row 1 must have error");
    assert!(msg.contains("unique"), "expected 'unique' in: {msg}");
    assert!(msg.contains("order_id"), "expected 'order_id' in: {msg}");
}

#[test]
fn summarize_validation_exprs_counts_not_null_and_cast_violations() {
    use floe_core::check::SparseRowErrors;
    use floe_core::report::build::summarize_validation_exprs;
    use floe_core::report::{RuleName, Severity};

    let col_violation_counts = vec![
        ("_e_nn_customer_id".to_string(), 3u64),
        ("_e_cast_created_at".to_string(), 1u64),
    ];
    let unique_errors = SparseRowErrors::new(5);
    let columns = vec![
        config::ColumnConfig {
            name: "customer_id".to_string(),
            source: None,
            column_type: "string".to_string(),
            nullable: None,
            unique: None,
            width: None,
            trim: None,
        },
        config::ColumnConfig {
            name: "created_at".to_string(),
            source: None,
            column_type: "datetime".to_string(),
            nullable: Some(true),
            unique: None,
            width: None,
            trim: None,
        },
    ];

    let rules = summarize_validation_exprs(
        &col_violation_counts,
        &unique_errors,
        &columns,
        Severity::Reject,
        None,
    );

    assert_eq!(rules.len(), 2);
    let not_null_rule = rules
        .iter()
        .find(|r| r.rule == RuleName::NotNull)
        .expect("not_null rule");
    assert_eq!(not_null_rule.violations, 3);
    assert_eq!(not_null_rule.columns.len(), 1);
    assert_eq!(not_null_rule.columns[0].column, "customer_id");

    let cast_rule = rules
        .iter()
        .find(|r| r.rule == RuleName::CastError)
        .expect("cast_error rule");
    assert_eq!(cast_rule.violations, 1);
    assert_eq!(cast_rule.columns[0].column, "created_at");
    assert_eq!(
        cast_rule.columns[0].target_type,
        Some("datetime".to_string())
    );
}

#[test]
fn summarize_validation_exprs_returns_empty_when_no_violations() {
    use floe_core::check::SparseRowErrors;
    use floe_core::report::build::summarize_validation_exprs;
    use floe_core::report::Severity;

    let rules =
        summarize_validation_exprs(&[], &SparseRowErrors::new(0), &[], Severity::Warn, None);
    assert!(rules.is_empty());
}
