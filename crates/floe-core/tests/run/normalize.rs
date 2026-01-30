use floe_core::run::normalize::normalize_name;

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
