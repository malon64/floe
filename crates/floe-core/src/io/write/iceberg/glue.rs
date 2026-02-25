use aws_config::meta::region::RegionProviderChain;
use aws_sdk_glue::config::Region as GlueRegion;
use aws_sdk_glue::types::{
    StorageDescriptor as GlueStorageDescriptor, TableInput as GlueTableInput,
};
use aws_sdk_glue::Client as GlueClient;

use crate::errors::RunError;
use crate::FloeResult;

use super::GlueIcebergCatalogConfig;

#[derive(Debug, Clone)]
pub(super) struct GlueTableState {
    pub(super) metadata_location: Option<String>,
    pub(super) version_id: Option<String>,
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

pub(super) async fn load_glue_table_state(
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
            let iceberg_param = parameters
                .and_then(|params| params.get("table_type"))
                .map(|value| value.eq_ignore_ascii_case("ICEBERG"))
                .unwrap_or(false);
            if !iceberg_param || metadata_location.is_none() {
                return Err(Box::new(RunError(format!(
                    "glue table {}.{} exists but is not an Iceberg table managed by Floe (missing Iceberg parameters/metadata_location)",
                    glue_cfg.database, glue_cfg.table
                ))));
            }
            Ok(GlueTableState {
                metadata_location,
                version_id: table.version_id().map(ToOwned::to_owned),
            })
        }
        Err(err) => {
            if err
                .as_service_error()
                .is_some_and(|service_err| service_err.is_entity_not_found_exception())
            {
                return Ok(GlueTableState {
                    metadata_location: None,
                    version_id: None,
                });
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
                .is_some_and(|service_err| service_err.is_already_exists_exception())
            {
                return Err(Box::new(RunError(format!(
                    "glue create_table failed for {}.{}: {err}",
                    glue_cfg.database, glue_cfg.table
                ))));
            }
        }
    }

    let mut update = client
        .update_table()
        .database_name(glue_cfg.database.as_str())
        .name(glue_cfg.table.as_str())
        .table_input(table_input)
        .skip_archive(true);
    if let Some(version_id) = version_id {
        update = update.version_id(version_id);
    }
    update.send().await.map_err(|err| {
        Box::new(RunError(format!(
            "glue update_table failed for {}.{}: {err}",
            glue_cfg.database, glue_cfg.table
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    Ok(())
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
        .build()
        .expect("glue table input builder validated")
}
