use floe_core::io::write::iceberg::rest::expand_env_refs;

#[test]
fn expands_partial_env_refs_in_client_credentials() {
    std::env::set_var("FLOE_TEST_REST_CLIENT_ID", "client-id");
    std::env::set_var("FLOE_TEST_REST_CLIENT_SECRET", "client-secret");

    let expanded = expand_env_refs(
        "client_credentials:${FLOE_TEST_REST_CLIENT_ID}:${FLOE_TEST_REST_CLIENT_SECRET}",
        "polaris",
    )
    .expect("expand credential");

    assert_eq!(expanded, "client_credentials:client-id:client-secret");
    std::env::remove_var("FLOE_TEST_REST_CLIENT_ID");
    std::env::remove_var("FLOE_TEST_REST_CLIENT_SECRET");
}

#[test]
fn expands_exact_env_ref_in_token_credential() {
    std::env::set_var("FLOE_TEST_REST_TOKEN", "pat-token");

    let expanded =
        expand_env_refs("token:${FLOE_TEST_REST_TOKEN}", "nessie").expect("expand token");

    assert_eq!(expanded, "token:pat-token");
    std::env::remove_var("FLOE_TEST_REST_TOKEN");
}

#[test]
fn preserves_literal_credential_text_that_contains_env_ref_syntax() {
    let expanded =
        expand_env_refs("token:abc${def}ghi", "nessie").expect("preserve literal credential");

    assert_eq!(expanded, "token:abc${def}ghi");
}

#[test]
fn errors_when_env_ref_is_missing() {
    std::env::remove_var("FLOE_TEST_REST_MISSING");

    let err = expand_env_refs(
        "client_credentials:${FLOE_TEST_REST_MISSING}:secret",
        "polaris",
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "rest iceberg catalog polaris credential references env var FLOE_TEST_REST_MISSING which is not set"
    );
}

#[test]
fn errors_on_malformed_env_ref() {
    std::env::set_var("ID", "client-id");

    let err = expand_env_refs(
        "client_credentials:${ID}:literal-secret:${UNCLOSED",
        "polaris",
    )
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "rest iceberg catalog polaris credential has unclosed env placeholder"
    );
    assert!(!err.to_string().contains("literal-secret"));
    std::env::remove_var("ID");
}
