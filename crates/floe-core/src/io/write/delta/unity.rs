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
/// The token must be either a literal value or a single `${VAR_NAME}` reference; mixing
/// literal text and references in one string is not supported.
fn expand_env_token(token: &str, catalog_name: &str) -> FloeResult<String> {
    let Some(inner) = token.strip_prefix("${").and_then(|s| s.strip_suffix('}')) else {
        return Ok(token.to_string());
    };
    if inner.is_empty() || inner.contains('{') || inner.contains('}') {
        return Err(Box::new(RunError(format!(
            "unity catalog {catalog_name} token has invalid syntax: {token}"
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
