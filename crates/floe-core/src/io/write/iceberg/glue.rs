use aws_config::meta::region::RegionProviderChain;
use aws_sdk_glue::config::Region as GlueRegion;
use aws_sdk_glue::types::{
    DatabaseInput as GlueDatabaseInput, StorageDescriptor as GlueStorageDescriptor,
    TableInput as GlueTableInput,
};
use aws_sdk_glue::Client as GlueClient;

use crate::errors::RunError;
use crate::{warnings, FloeResult};

use super::GlueIcebergCatalogConfig;

/// Parameter Floe sets on every table it creates so it can identify ownership.
const FLOE_MANAGED_PARAM: &str = "floe.managed";

#[derive(Debug, Clone)]
pub(crate) struct GlueTableState {
    pub(crate) metadata_location: Option<String>,
    pub(crate) version_id: Option<String>,
}

async fn build_glue_client(region: &str) -> FloeResult<GlueClient> {
    let region_provider =
        RegionProviderChain::first_try(GlueRegion::new(region.to_string())).or_default_provider();
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    Ok(GlueClient::new(&config))
}

fn is_access_denied(code: Option<&str>) -> bool {
    code.is_some_and(|c| c.contains("AccessDenied") || c == "UnauthorizedException")
}

pub(crate) async fn load_glue_table_state(
    glue_cfg: &GlueIcebergCatalogConfig,
) -> FloeResult<GlueTableState> {
    let client = build_glue_client(&glue_cfg.region).await?;
    let response = client
        .get_table()
        .database_name(glue_cfg.database.as_str())
        .name(glue_cfg.table.as_str())
        .send()
        .await;
    match response {
        Ok(output) => {
            let table = output.table().ok_or_else(|| {
                Box::new(RunError(format!(
                    "glue get_table returned no table for {}.{}",
                    glue_cfg.database, glue_cfg.table
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            let parameters = table.parameters();
            let metadata_location =
                parameters.and_then(|params| params.get("metadata_location").cloned());
            let is_iceberg = parameters
                .and_then(|params| params.get("table_type"))
                .map(|value| value.eq_ignore_ascii_case("ICEBERG"))
                .unwrap_or(false);
            if !is_iceberg || metadata_location.is_none() {
                return Err(Box::new(RunError(format!(
                    "glue table {}.{} exists but is not an Iceberg table (missing table_type=ICEBERG or metadata_location)",
                    glue_cfg.database, glue_cfg.table
                ))));
            }

            // Ownership check: warn or error if the table was not created by Floe.
            let floe_managed = parameters
                .and_then(|params| params.get(FLOE_MANAGED_PARAM))
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            if !floe_managed {
                if glue_cfg.allow_takeover {
                    warnings::emit(
                        "glue",
                        None,
                        None,
                        Some("glue_takeover"),
                        &format!(
                            "glue table {}.{} was not created by Floe; taking ownership (allow_takeover=true)",
                            glue_cfg.database, glue_cfg.table
                        ),
                    );
                } else {
                    return Err(Box::new(RunError(format!(
                        "glue table {}.{} was not created by Floe; set allow_takeover=true on the catalog definition to take ownership",
                        glue_cfg.database, glue_cfg.table
                    ))));
                }
            }

            Ok(GlueTableState {
                metadata_location,
                version_id: table.version_id().map(ToOwned::to_owned),
            })
        }
        Err(err) => {
            if err
                .as_service_error()
                .is_some_and(|e| e.is_entity_not_found_exception())
            {
                return Ok(GlueTableState {
                    metadata_location: None,
                    version_id: None,
                });
            }
            let code = err.as_service_error().and_then(|e| e.meta().code());
            if is_access_denied(code) {
                return Err(Box::new(RunError(format!(
                    "glue get_table denied for {}.{}: ensure the IAM role/user has glue:GetTable permission: {err}",
                    glue_cfg.database, glue_cfg.table
                ))));
            }
            Err(Box::new(RunError(format!(
                "glue get_table failed for {}.{}: {err}",
                glue_cfg.database, glue_cfg.table
            ))))
        }
    }
}

pub(super) async fn upsert_glue_table(
    glue_cfg: &GlueIcebergCatalogConfig,
    table_root_uri: &str,
    metadata_location: &str,
    version_id: Option<&str>,
) -> FloeResult<()> {
    let client = build_glue_client(&glue_cfg.region).await?;
    ensure_glue_database(
        &client,
        &glue_cfg.database,
        glue_cfg.create_database_if_missing,
    )
    .await?;

    let table_input = build_glue_table_input(glue_cfg, table_root_uri, metadata_location);

    let create_result = client
        .create_table()
        .database_name(glue_cfg.database.as_str())
        .name(glue_cfg.table.as_str())
        .table_input(table_input.clone())
        .send()
        .await;
    match create_result {
        Ok(_) => return Ok(()),
        Err(err) => {
            if !err
                .as_service_error()
                .is_some_and(|e| e.is_already_exists_exception())
            {
                let code = err.as_service_error().and_then(|e| e.meta().code());
                if is_access_denied(code) {
                    return Err(Box::new(RunError(format!(
                        "glue create_table denied for {}.{}: ensure the IAM role/user has glue:CreateTable permission: {err}",
                        glue_cfg.database, glue_cfg.table
                    ))));
                }
                return Err(Box::new(RunError(format!(
                    "glue create_table failed for {}.{}: {err}",
                    glue_cfg.database, glue_cfg.table
                ))));
            }
        }
    }

    // Table already exists — update with retry on concurrent modification.
    const MAX_RETRIES: u32 = 3;
    let mut attempt = 0_u32;
    loop {
        let mut update = client
            .update_table()
            .database_name(glue_cfg.database.as_str())
            .name(glue_cfg.table.as_str())
            .table_input(table_input.clone())
            .skip_archive(true);
        if let Some(vid) = version_id {
            update = update.version_id(vid);
        }
        let result = update.send().await;
        match result {
            Ok(_) => return Ok(()),
            Err(err) => {
                if attempt < MAX_RETRIES
                    && err
                        .as_service_error()
                        .is_some_and(|e| e.is_concurrent_modification_exception())
                {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(200 * attempt as u64))
                        .await;
                    continue;
                }
                let code = err.as_service_error().and_then(|e| e.meta().code());
                if is_access_denied(code) {
                    return Err(Box::new(RunError(format!(
                        "glue update_table denied for {}.{}: ensure the IAM role/user has glue:UpdateTable permission: {err}",
                        glue_cfg.database, glue_cfg.table
                    ))));
                }
                return Err(Box::new(RunError(format!(
                    "glue update_table failed for {}.{}: {err}",
                    glue_cfg.database, glue_cfg.table
                )))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
        }
    }
}

async fn ensure_glue_database(
    client: &GlueClient,
    database: &str,
    create_if_missing: bool,
) -> FloeResult<()> {
    let result = client.get_database().name(database).send().await;
    match result {
        Ok(_) => Ok(()),
        Err(err) => {
            if err
                .as_service_error()
                .is_some_and(|e| e.is_entity_not_found_exception())
            {
                if !create_if_missing {
                    return Err(Box::new(RunError(format!(
                        "glue database {database} does not exist; set create_database_if_missing=true to create it automatically"
                    ))));
                }
                let db_input = GlueDatabaseInput::builder()
                    .name(database)
                    .build()
                    .expect("database input builder validated");
                client
                    .create_database()
                    .database_input(db_input)
                    .send()
                    .await
                    .map_err(|e| {
                        Box::new(RunError(format!(
                            "glue create_database failed for {database}: {e}"
                        ))) as Box<dyn std::error::Error + Send + Sync>
                    })?;
                Ok(())
            } else {
                let code = err.as_service_error().and_then(|e| e.meta().code());
                if is_access_denied(code) {
                    return Err(Box::new(RunError(format!(
                        "glue get_database denied for {database}: ensure the IAM role/user has glue:GetDatabase permission: {err}"
                    ))));
                }
                Err(Box::new(RunError(format!(
                    "glue get_database failed for {database}: {err}"
                ))))
            }
        }
    }
}

fn build_glue_table_input(
    glue_cfg: &GlueIcebergCatalogConfig,
    table_root_uri: &str,
    metadata_location: &str,
) -> GlueTableInput {
    let storage_descriptor = GlueStorageDescriptor::builder()
        .location(table_root_uri)
        .build();

    GlueTableInput::builder()
        .name(glue_cfg.table.as_str())
        .table_type("EXTERNAL_TABLE")
        .storage_descriptor(storage_descriptor)
        .set_partition_keys(Some(Vec::new()))
        .parameters("table_type", "ICEBERG")
        .parameters("EXTERNAL", "TRUE")
        .parameters("metadata_location", metadata_location)
        .parameters("floe.iceberg.namespace", glue_cfg.namespace.as_str())
        .parameters(FLOE_MANAGED_PARAM, "true")
        .build()
        .expect("glue table input builder validated")
}
