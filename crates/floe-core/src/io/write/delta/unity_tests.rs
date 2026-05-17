use super::*;

#[test]
fn plain_literal_is_returned_unchanged() {
    let result = expand_env_token("dapi1234567890abcdef", "my_catalog").unwrap();
    assert_eq!(result, "dapi1234567890abcdef");
}

#[test]
fn env_ref_is_expanded_when_var_is_set() {
    std::env::set_var("FLOE_TEST_UNITY_TOKEN", "test-secret");
    let result = expand_env_token("${FLOE_TEST_UNITY_TOKEN}", "my_catalog").unwrap();
    std::env::remove_var("FLOE_TEST_UNITY_TOKEN");
    assert_eq!(result, "test-secret");
}

#[test]
fn env_ref_errors_when_var_is_not_set() {
    std::env::remove_var("FLOE_TEST_UNITY_MISSING");
    let err = expand_env_token("${FLOE_TEST_UNITY_MISSING}", "my_catalog").unwrap_err();
    assert!(
        err.to_string().contains("FLOE_TEST_UNITY_MISSING"),
        "error should name the missing var"
    );
}

#[test]
fn mixed_literal_and_ref_is_rejected() {
    let err = expand_env_token("Bearer ${MY_TOKEN}", "my_catalog").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("mixing literal text"),
        "error should explain the constraint, got: {msg}"
    );
}

#[test]
fn empty_placeholder_is_rejected() {
    let err = expand_env_token("${}", "my_catalog").unwrap_err();
    assert!(
        err.to_string().contains("invalid placeholder syntax"),
        "got: {err}"
    );
}

#[test]
fn unclosed_brace_is_rejected() {
    let err = expand_env_token("${UNCLOSED", "my_catalog").unwrap_err();
    assert!(
        err.to_string().contains("mixing literal text"),
        "got: {err}"
    );
}

#[test]
fn parse_unity_error_extracts_code_and_message() {
    let body = r#"{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"Table not found"}"#;
    let result = parse_unity_error(body);
    assert_eq!(result, "RESOURCE_DOES_NOT_EXIST: Table not found");
}

#[test]
fn parse_unity_error_falls_back_to_raw_body_on_invalid_json() {
    let body = "Internal Server Error";
    assert_eq!(parse_unity_error(body), "Internal Server Error");
}

#[test]
fn parse_unity_error_uses_unknown_code_when_field_missing() {
    let body = r#"{"message":"Schema not found"}"#;
    let result = parse_unity_error(body);
    assert_eq!(result, "UNKNOWN: Schema not found");
}

fn test_cfg(server_url: &str) -> UnityCatalogConfig {
    UnityCatalogConfig {
        catalog_name: "test_def".to_string(),
        host: server_url.to_string(),
        unity_catalog: "main".to_string(),
        schema: "bronze".to_string(),
        table: "orders".to_string(),
        token: "test-token".to_string(),
        create_schema_if_missing: false,
    }
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

#[test]
fn table_already_registered_at_same_location_returns_ok() {
    let mut server = mockito::Server::new();
    let table_uri = "s3://my-bucket/bronze/orders";
    server
        .mock("GET", "/api/2.1/unity-catalog/tables/main.bronze.orders")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(format!(r#"{{"storage_location":"{table_uri}"}}"#))
        .create();

    let cfg = test_cfg(&server.url());
    block_on(register_unity_table(&cfg, table_uri)).unwrap();
}

#[test]
fn table_registered_at_different_location_returns_error() {
    let mut server = mockito::Server::new();
    server
        .mock("GET", "/api/2.1/unity-catalog/tables/main.bronze.orders")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"storage_location":"s3://other-bucket/path"}"#)
        .create();

    let cfg = test_cfg(&server.url());
    let err = block_on(register_unity_table(&cfg, "s3://my-bucket/bronze/orders")).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("already registered"),
        "error should flag location mismatch, got: {msg}"
    );
}

#[test]
fn table_200_without_storage_location_is_managed_table_collision() {
    let mut server = mockito::Server::new();
    server
        .mock("GET", "/api/2.1/unity-catalog/tables/main.bronze.orders")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"name":"orders"}"#)
        .create();

    let cfg = test_cfg(&server.url());
    let err = block_on(register_unity_table(&cfg, "s3://my-bucket/bronze/orders")).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("storage_location"),
        "error should mention storage_location could not be read, got: {msg}"
    );
}

#[test]
fn table_not_found_creates_it() {
    let mut server = mockito::Server::new();
    server
        .mock("GET", "/api/2.1/unity-catalog/tables/main.bronze.orders")
        .with_status(404)
        .create();
    server
        .mock("POST", "/api/2.1/unity-catalog/tables")
        .with_status(200)
        .create();

    let cfg = test_cfg(&server.url());
    block_on(register_unity_table(&cfg, "s3://my-bucket/bronze/orders")).unwrap();
}

#[test]
fn table_create_with_schema_creation() {
    let mut server = mockito::Server::new();
    server
        .mock("GET", "/api/2.1/unity-catalog/tables/main.bronze.orders")
        .with_status(404)
        .create();
    server
        .mock("GET", "/api/2.1/unity-catalog/schemas/main.bronze")
        .with_status(404)
        .create();
    server
        .mock("POST", "/api/2.1/unity-catalog/schemas")
        .with_status(200)
        .create();
    server
        .mock("POST", "/api/2.1/unity-catalog/tables")
        .with_status(200)
        .create();

    let mut cfg = test_cfg(&server.url());
    cfg.create_schema_if_missing = true;
    block_on(register_unity_table(&cfg, "s3://my-bucket/bronze/orders")).unwrap();
}

#[test]
fn unexpected_get_status_returns_error() {
    let mut server = mockito::Server::new();
    server
        .mock("GET", "/api/2.1/unity-catalog/tables/main.bronze.orders")
        .with_status(403)
        .with_body(r#"{"error_code":"PERMISSION_DENIED","message":"Access denied"}"#)
        .create();

    let cfg = test_cfg(&server.url());
    let err = block_on(register_unity_table(&cfg, "s3://my-bucket/bronze/orders")).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("403"),
        "error should mention the status, got: {msg}"
    );
    assert!(
        msg.contains("PERMISSION_DENIED"),
        "error should include parsed error code, got: {msg}"
    );
}
