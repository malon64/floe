use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use iceberg::spec::{Schema, UnboundPartitionSpec};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use crate::errors::RunError;
use crate::io::storage::{object_store::iceberg_store_config, Target};
use crate::{config, io, FloeResult};

use super::metadata::{
    latest_gcs_metadata_location, latest_local_metadata_location, latest_s3_metadata_location,
};
use super::{map_iceberg_err, GlueIcebergCatalogConfig, IcebergRemoteContext, IcebergWriteContext};

pub(super) fn build_iceberg_write_context(
    target: &Target,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    mut remote: Option<&mut IcebergRemoteContext<'_>>,
) -> FloeResult<IcebergWriteContext> {
    if entity.sink.accepted.iceberg.is_some() && remote.is_none() {
        return Err(Box::new(RunError(format!(
            "iceberg catalog writes require runtime catalog context for entity {}",
            entity.name
        ))));
    }
    match target {
        Target::Local { base_path, .. } => {
            let table_root = PathBuf::from(base_path);
            fs::create_dir_all(&table_root)?;
            let metadata_location = latest_local_metadata_location(&table_root)?;
            Ok(IcebergWriteContext {
                table_root_uri: base_path.to_string(),
                catalog_name: "floe_iceberg",
                catalog_props: HashMap::new(),
                metadata_location,
                glue_catalog: None,
            })
        }
        Target::S3 {
            storage,
            uri,
            bucket,
            base_key,
        } => {
            if let Some(ctx) = remote.as_mut() {
                let ctx = &mut **ctx;
                if let Some(glue_target) = ctx.catalogs.resolve_iceberg_target(
                    ctx.resolver,
                    entity,
                    &entity.sink.accepted,
                )? {
                    if glue_target.catalog_type == "glue" {
                        let catalog_target = Target::from_resolved(&glue_target.table_location)?;
                        let store = iceberg_store_config(&catalog_target, ctx.resolver, entity)?;
                        return Ok(IcebergWriteContext {
                            table_root_uri: store.warehouse_location,
                            catalog_name: "floe_iceberg",
                            catalog_props: store.file_io_props,
                            metadata_location: None,
                            glue_catalog: Some(GlueIcebergCatalogConfig {
                                catalog_name: glue_target.catalog_name,
                                region: glue_target.region,
                                database: glue_target.database,
                                namespace: glue_target.namespace,
                                table: glue_target.table,
                            }),
                        });
                    }
                }
            }

            let metadata_location = if matches!(mode, config::WriteMode::Append) {
                match remote.as_mut() {
                    Some(ctx) => {
                        let ctx = &mut **ctx;
                        let client = ctx.cloud.client_for(ctx.resolver, storage, entity)?;
                        latest_s3_metadata_location(client, base_key)?
                    }
                    None => {
                        let mut client = io::storage::s3::S3Client::new(bucket.clone(), None)?;
                        latest_s3_metadata_location(&mut client, base_key)?
                    }
                }
            } else {
                None
            };

            match remote.as_mut() {
                Some(ctx) => {
                    let ctx = &mut **ctx;
                    let store = iceberg_store_config(target, ctx.resolver, entity)?;
                    Ok(IcebergWriteContext {
                        table_root_uri: store.warehouse_location,
                        catalog_name: "floe_iceberg",
                        catalog_props: store.file_io_props,
                        metadata_location,
                        glue_catalog: None,
                    })
                }
                None => Ok(IcebergWriteContext {
                    table_root_uri: uri.clone(),
                    catalog_name: "floe_iceberg",
                    catalog_props: HashMap::new(),
                    metadata_location,
                    glue_catalog: None,
                }),
            }
        }
        Target::Gcs {
            storage,
            uri,
            bucket,
            base_key,
        } => {
            let metadata_location = if matches!(mode, config::WriteMode::Append) {
                match remote.as_mut() {
                    Some(ctx) => {
                        let ctx = &mut **ctx;
                        let client = ctx.cloud.client_for(ctx.resolver, storage, entity)?;
                        latest_gcs_metadata_location(client, base_key)?
                    }
                    None => {
                        let mut client = io::storage::gcs::GcsClient::new(bucket.clone())?;
                        latest_gcs_metadata_location(&mut client, base_key)?
                    }
                }
            } else {
                None
            };

            match remote.as_mut() {
                Some(ctx) => {
                    let ctx = &mut **ctx;
                    let store = iceberg_store_config(target, ctx.resolver, entity)?;
                    Ok(IcebergWriteContext {
                        table_root_uri: store.warehouse_location,
                        catalog_name: "floe_iceberg",
                        catalog_props: store.file_io_props,
                        metadata_location,
                        glue_catalog: None,
                    })
                }
                None => Ok(IcebergWriteContext {
                    table_root_uri: uri.clone(),
                    catalog_name: "floe_iceberg",
                    catalog_props: HashMap::new(),
                    metadata_location,
                    glue_catalog: None,
                }),
            }
        }
        Target::Adls { .. } => Err(Box::new(RunError(format!(
            "iceberg sink currently supports local, s3, or gcs storage only for entity {}",
            entity.name
        )))),
    }
}

pub(super) async fn ensure_namespace(
    catalog: &iceberg::MemoryCatalog,
    namespace: &NamespaceIdent,
) -> FloeResult<()> {
    let exists = catalog
        .namespace_exists(namespace)
        .await
        .map_err(map_iceberg_err("iceberg namespace exists check failed"))?;
    if !exists {
        catalog
            .create_namespace(namespace, HashMap::new())
            .await
            .map_err(map_iceberg_err("iceberg namespace create failed"))?;
    }
    Ok(())
}

pub(super) async fn create_table(
    catalog: &iceberg::MemoryCatalog,
    namespace: &NamespaceIdent,
    table_ident: &TableIdent,
    table_root: String,
    schema: &Schema,
    partition_spec: Option<UnboundPartitionSpec>,
) -> FloeResult<iceberg::table::Table> {
    let creation_builder = TableCreation::builder()
        .name(table_ident.name().to_string())
        .location(table_root)
        .schema(schema.clone());
    let creation = match partition_spec {
        Some(partition_spec) => creation_builder.partition_spec(partition_spec).build(),
        None => creation_builder.build(),
    };
    catalog
        .create_table(namespace, creation)
        .await
        .map_err(map_iceberg_err("iceberg create table failed"))
}

pub(super) fn sanitize_table_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "table".to_string()
    } else {
        out
    }
}
