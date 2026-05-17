use serde::{Deserialize, Serialize};

use crate::config::{CatalogTypeConfig, ResolvedDeltaCatalogTarget};
use crate::errors::RunError;
use crate::FloeResult;

/// Runtime config for a single Unity Catalog registration.
#[derive(Debug, Clone)]
pub(crate) struct UnityCatalogConfig {
    /// Floe catalog definition name (for error messages).
    pub(crate) catalog_name: String,
    /// Databricks workspace host, e.g. "https://my-workspace.azuredatabricks.net".
    pub(crate) host: String,
    /// Unity catalog name.
    pub(crate) unity_catalog: String,
    /// Unity schema name.
    pub(crate) schema: String,
    /// Table name.
    pub(crate) table: String,
    /// Personal Access Token.
    pub(crate) token: String,
    /// Create the schema if it does not exist.
    pub(crate) create_schema_if_missing: bool,
}

impl UnityCatalogConfig {
    pub(crate) fn from_resolved(resolved: &ResolvedDeltaCatalogTarget) -> FloeResult<Self> {
        match &resolved.type_config {
            CatalogTypeConfig::Unity {
                host,
                catalog,
                token,
                create_schema_if_missing,
                ..
            } => Ok(Self {
                catalog_name: resolved.catalog_name.clone(),
                host: host.trim_end_matches('/').to_string(),
                unity_catalog: catalog.clone(),
                schema: resolved.schema.clone(),
                table: resolved.table.clone(),
                token: expand_env_token(token, &resolved.catalog_name)?,
                create_schema_if_missing: *create_schema_if_missing,
            }),
            // The resolved target is guaranteed to be Unity by validate_delta_catalog_binding.
            // Glue and Rest variants cannot reach this path.
            other => Err(Box::new(RunError(format!(
                "UnityCatalogConfig::from_resolved called on non-unity catalog type={}",
                other.catalog_type_str()
            )))),
        }
    }
}

/// Expands a single `${VAR_NAME}` reference in the token string using the OS environment.
/// The token must be either a plain literal value or exactly `${VAR_NAME}` — mixing
/// literal text around `${...}` (e.g. `"Bearer ${TOKEN}"`) is not supported and
/// returns an error rather than silently passing the unexpanded string to Databricks.
fn expand_env_token(token: &str, catalog_name: &str) -> FloeResult<String> {
    if !token.contains("${") {
        return Ok(token.to_string());
    }
    let Some(inner) = token.strip_prefix("${").and_then(|s| s.strip_suffix('}')) else {
        return Err(Box::new(RunError(format!(
            "unity catalog {catalog_name} token must be a plain value or a single \
             ${{VAR_NAME}} reference; mixing literal text with ${{...}} is not supported \
             (got: {token:?})"
        ))));
    };
    if inner.is_empty() || inner.contains('{') || inner.contains('}') {
        return Err(Box::new(RunError(format!(
            "unity catalog {catalog_name} token has invalid placeholder syntax: {token}"
        ))));
    }
    std::env::var(inner).map_err(|_| {
        Box::new(RunError(format!(
            "unity catalog {catalog_name} token references env var {inner} which is not set"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

// ---------------------------------------------------------------------------
// REST API payloads
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct CreateTableRequest<'a> {
    name: &'a str,
    catalog_name: &'a str,
    schema_name: &'a str,
    table_type: &'static str,
    data_source_format: &'static str,
    storage_location: &'a str,
}

#[derive(Serialize)]
struct CreateSchemaRequest<'a> {
    name: &'a str,
    catalog_name: &'a str,
}

#[derive(Deserialize)]
struct TableGetResponse {
    storage_location: Option<String>,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error_code: Option<String>,
    message: Option<String>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Register or confirm a Delta table in Unity Catalog.
///
/// Logic:
/// 1. GET the table — if it exists (200) return Ok immediately (Delta log is self-describing).
/// 2. If 404, optionally create the schema, then POST to create the table.
/// 3. Any other HTTP status is returned as an error.
pub(crate) async fn register_unity_table(
    cfg: &UnityCatalogConfig,
    table_uri: &str,
) -> FloeResult<()> {
    let client = reqwest::Client::new();
    let catalog_def = &cfg.catalog_name;
    let full_name = format!("{}.{}.{}", cfg.unity_catalog, cfg.schema, cfg.table);

    // 1. Check if the table already exists.
    let get_resp = client
        .get(format!(
            "{}/api/2.1/unity-catalog/tables/{}",
            cfg.host, full_name
        ))
        .bearer_auth(&cfg.token)
        .send()
        .await
        .map_err(|err| {
            Box::new(RunError(format!(
                "unity catalog GET table {full_name} failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

    match get_resp.status().as_u16() {
        200 => {
            // Table already registered — verify location matches to catch stale config or name
            // collisions with managed tables/views that don't expose storage_location.
            let body = get_resp.text().await.unwrap_or_default();
            let existing_loc = serde_json::from_str::<TableGetResponse>(&body)
                .ok()
                .and_then(|r| r.storage_location)
                .ok_or_else(|| {
                    Box::new(RunError(format!(
                        "unity catalog {catalog_def} table {full_name} already exists but its \
                         storage_location could not be read from the GET response (managed table \
                         or view name collision?): rename the entity or choose a different table name"
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?;
            let norm_existing = existing_loc.trim_end_matches('/');
            let norm_new = table_uri.trim_end_matches('/');
            if norm_existing != norm_new {
                return Err(Box::new(RunError(format!(
                    "unity catalog {catalog_def} table {full_name} is already registered \
                     at {existing_loc} but the current write targets {table_uri}: \
                     update sink.accepted.path or choose a different table name"
                ))));
            }
            return Ok(());
        }
        404 => {
            // Not found — proceed to create.
        }
        status => {
            let body = get_resp.text().await.unwrap_or_default();
            return Err(Box::new(RunError(format!(
                "unity catalog {catalog_def} GET table {full_name} returned unexpected status {status}: {body}"
            ))));
        }
    }

    // 2. Optionally create the schema if missing.
    if cfg.create_schema_if_missing {
        ensure_schema(cfg, &client).await?;
    }

    // 3. Create the table.
    let create_resp = client
        .post(format!("{}/api/2.1/unity-catalog/tables", cfg.host))
        .bearer_auth(&cfg.token)
        .json(&CreateTableRequest {
            name: &cfg.table,
            catalog_name: &cfg.unity_catalog,
            schema_name: &cfg.schema,
            table_type: "EXTERNAL",
            data_source_format: "DELTA",
            storage_location: table_uri,
        })
        .send()
        .await
        .map_err(|err| {
            Box::new(RunError(format!(
                "unity catalog POST table {full_name} failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

    let status = create_resp.status().as_u16();
    if status == 200 || status == 201 {
        return Ok(());
    }

    // Schema may not exist even with create_schema_if_missing=false — give a helpful error.
    let body = create_resp.text().await.unwrap_or_default();
    let detail = parse_unity_error(&body);
    Err(Box::new(RunError(format!(
        "unity catalog {catalog_def} POST table {full_name} returned status {status}: {detail}"
    ))))
}

async fn ensure_schema(cfg: &UnityCatalogConfig, client: &reqwest::Client) -> FloeResult<()> {
    let schema_full = format!("{}.{}", cfg.unity_catalog, cfg.schema);

    let get_resp = client
        .get(format!(
            "{}/api/2.1/unity-catalog/schemas/{}",
            cfg.host, schema_full
        ))
        .bearer_auth(&cfg.token)
        .send()
        .await
        .map_err(|err| {
            Box::new(RunError(format!(
                "unity catalog GET schema {schema_full} failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

    if get_resp.status().as_u16() == 200 {
        return Ok(());
    }

    let create_resp = client
        .post(format!("{}/api/2.1/unity-catalog/schemas", cfg.host))
        .bearer_auth(&cfg.token)
        .json(&CreateSchemaRequest {
            name: &cfg.schema,
            catalog_name: &cfg.unity_catalog,
        })
        .send()
        .await
        .map_err(|err| {
            Box::new(RunError(format!(
                "unity catalog POST schema {schema_full} failed: {err}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

    let status = create_resp.status().as_u16();
    if status == 200 || status == 201 {
        return Ok(());
    }

    let body = create_resp.text().await.unwrap_or_default();
    let detail = parse_unity_error(&body);
    Err(Box::new(RunError(format!(
        "unity catalog POST schema {schema_full} returned status {status}: {detail}"
    ))))
}

fn parse_unity_error(body: &str) -> String {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(body) {
        let code = err.error_code.as_deref().unwrap_or("UNKNOWN");
        let msg = err.message.as_deref().unwrap_or(body);
        format!("{code}: {msg}")
    } else {
        body.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // expand_env_token
    // -----------------------------------------------------------------------

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
        assert!(err.to_string().contains("invalid placeholder syntax"));
    }

    #[test]
    fn unclosed_brace_is_rejected() {
        let err = expand_env_token("${UNCLOSED", "my_catalog").unwrap_err();
        assert!(err.to_string().contains("mixing literal text"));
    }

    // -----------------------------------------------------------------------
    // parse_unity_error
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // register_unity_table — HTTP interaction tests
    // -----------------------------------------------------------------------

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
            "error should mention the unexpected status, got: {msg}"
        );
        assert!(msg.contains("PERMISSION_DENIED") || msg.contains("Access denied"));
    }
}
