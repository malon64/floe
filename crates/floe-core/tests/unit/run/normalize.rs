use floe_core::checks::normalize::{normalize_name, output_column_mapping, resolve_source_columns};
use floe_core::config::ColumnConfig;

#[test]
fn normalize_name_snake_case() {
    assert_eq!(normalize_name("Customer ID", "snake_case"), "customer_id");
    assert_eq!(normalize_name("createdAt", "snake_case"), "created_at");
}

#[test]
fn normalize_name_lower() {
    assert_eq!(normalize_name("CustomerID", "lower"), "customerid");
}

#[test]
fn normalize_name_camel_case() {
    assert_eq!(normalize_name("customer_id", "camel_case"), "customerId");
    assert_eq!(normalize_name("Customer ID", "camel_case"), "customerId");
}

#[test]
fn resolve_source_columns_uses_source_for_matching() {
    let columns = vec![ColumnConfig {
        name: "userId".to_string(),
        source: Some("USER-ID".to_string()),
        column_type: "string".to_string(),
        nullable: Some(true),
        unique: None,
    }];
    let resolved = resolve_source_columns(&columns, Some("snake_case")).expect("resolve columns");
    assert_eq!(resolved[0].name, "user_id");
}

#[test]
fn output_column_mapping_preserves_explicit_target_names() {
    let columns = vec![ColumnConfig {
        name: "userId".to_string(),
        source: Some("USER-ID".to_string()),
        column_type: "string".to_string(),
        nullable: Some(true),
        unique: None,
    }];
    let mapping = output_column_mapping(&columns, Some("snake_case")).expect("build mapping");
    assert_eq!(mapping.get("user_id").map(String::as_str), Some("userId"));
}
