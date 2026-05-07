use std::collections::HashMap;
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::errors::RunError;
use crate::io::storage::{object_store, Target};
use crate::io::write::iceberg::metadata::{
    latest_gcs_metadata_location, latest_local_metadata_location, latest_s3_metadata_location,
};
use crate::io::write::iceberg::{
    load_glue_table_state, sanitize_table_name, storage_factory_for_location, IcebergCatalogConfig,
    RestIcebergCatalogConfig, ICEBERG_CATALOG_NAME, ICEBERG_NAMESPACE,
};
use crate::{check, config, io, FloeResult};

use super::{seed_from_batches, FormatSeeder};

pub(super) struct IcebergSeeder<'a> {
    pub(super) target: &'a Target,
    pub(super) cloud: &'a mut io::storage::CloudClient,
    pub(super) resolver: &'a config::StorageResolver,
    pub(super) catalogs: &'a config::CatalogResolver,
    pub(super) entity: &'a config::EntityConfig,
}

impl FormatSeeder for IcebergSeeder<'_> {
    fn seed(
        &mut self,
        unique_tracker: &mut check::UniqueTracker,
        scan_cols: &[String],
        rename_back: &HashMap<String, String>,
    ) -> FloeResult<()> {
        // When a Glue catalog is configured, the writer uses glue_target.table_location as the
        // real table root (not target). Seed from the same location so duplicate detection is
        // consistent with what was actually written.
        if let Some(resolved) = self.catalogs.resolve_iceberg_target(
            self.resolver,
            self.entity,
            &self.entity.sink.accepted,
        )? {
            let catalog_cfg = IcebergCatalogConfig::from_resolved(&resolved)?;
            let catalog_target = Target::from_resolved(&resolved.table_location)?;
            let store =
                object_store::iceberg_store_config(&catalog_target, self.resolver, self.entity)?;
            return seed_from_catalog(
                unique_tracker,
                &catalog_cfg,
                store.file_io_props,
                store.warehouse_location,
                self.entity,
                scan_cols,
                rename_back,
            );
        }

        let store = object_store::iceberg_store_config(self.target, self.resolver, self.entity)?;

        let metadata_location: Option<String> = match self.target {
            Target::Local { base_path, .. } => {
                latest_local_metadata_location(Path::new(base_path))?
            }
            Target::S3 {
                storage, base_key, ..
            } => {
                let client = self.cloud.client_for(self.resolver, storage, self.entity)?;
                latest_s3_metadata_location(client, base_key)?
            }
            Target::Gcs {
                storage, base_key, ..
            } => {
                let client = self.cloud.client_for(self.resolver, storage, self.entity)?;
                latest_gcs_metadata_location(client, base_key)?
            }
            Target::Adls { .. } => return Ok(()),
        };
        let Some(metadata_location) = metadata_location else {
            return Ok(());
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                Box::new(RunError(format!("iceberg seed runtime init failed: {err}")))
            })?;

        let batches = runtime
            .block_on(collect_iceberg_batches(
                metadata_location,
                store.file_io_props,
                store.warehouse_location,
                &self.entity.name,
                scan_cols,
            ))
            .map_err(|err| Box::new(RunError(format!("iceberg seed failed: {err}"))))?;

        seed_from_batches(unique_tracker, batches, rename_back)
    }
}

/// Dispatches to the catalog-type-specific seed implementation.
/// Add a new arm here when supporting a new catalog type.
fn seed_from_catalog(
    unique_tracker: &mut check::UniqueTracker,
    catalog_cfg: &IcebergCatalogConfig,
    file_io_props: HashMap<String, String>,
    warehouse_location: String,
    entity: &config::EntityConfig,
    scan_cols: &[String],
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    match catalog_cfg {
        IcebergCatalogConfig::Glue(glue_cfg) => seed_from_glue(
            unique_tracker,
            glue_cfg,
            file_io_props,
            warehouse_location,
            entity,
            scan_cols,
            rename_back,
        ),
        IcebergCatalogConfig::Rest(rest_cfg) => seed_from_rest(
            unique_tracker,
            rest_cfg,
            file_io_props,
            warehouse_location,
            scan_cols,
            rename_back,
        ),
    }
}

fn seed_from_glue(
    unique_tracker: &mut check::UniqueTracker,
    glue_cfg: &crate::io::write::iceberg::GlueIcebergCatalogConfig,
    file_io_props: HashMap<String, String>,
    warehouse_location: String,
    entity: &config::EntityConfig,
    scan_cols: &[String],
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            Box::new(RunError(format!(
                "glue iceberg seed runtime init failed: {err}"
            )))
        })?;

    let glue_state = runtime
        .block_on(load_glue_table_state(glue_cfg))
        .map_err(|err| {
            Box::new(RunError(format!(
                "glue get_table for iceberg seed failed: {err}"
            )))
        })?;

    let Some(metadata_location) = glue_state.metadata_location else {
        return Ok(());
    };

    let batches = runtime
        .block_on(collect_iceberg_batches(
            metadata_location,
            file_io_props,
            warehouse_location,
            &entity.name,
            scan_cols,
        ))
        .map_err(|err| Box::new(RunError(format!("glue iceberg seed failed: {err}"))))?;

    seed_from_batches(unique_tracker, batches, rename_back)
}

fn seed_from_rest(
    unique_tracker: &mut check::UniqueTracker,
    rest_cfg: &RestIcebergCatalogConfig,
    file_io_props: HashMap<String, String>,
    warehouse_location: String,
    scan_cols: &[String],
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    use futures::TryStreamExt;
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
    use iceberg_catalog_rest::RestCatalogBuilder;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            Box::new(RunError(format!(
                "rest iceberg seed runtime init failed: {err}"
            )))
        })?;

    let batches = runtime
        .block_on(async {
            // Start with any storage props (e.g. S3 region) for cloud FileIO.
            let mut props = file_io_props;
            props.insert("uri".to_string(), rest_cfg.uri.clone());
            if let Some(cred) = &rest_cfg.credential {
                // "token:<value>"                  → static PAT (Unity Catalog)
                // "client_credentials:<id>:<secret>" → OAuth2 client-credentials pair
                // anything else                    → raw OAuth2 credential string
                if let Some(token) = cred.strip_prefix("token:") {
                    props.insert("token".to_string(), token.to_string());
                } else if let Some(cred_pair) = cred.strip_prefix("client_credentials:") {
                    props.insert("credential".to_string(), cred_pair.to_string());
                } else {
                    props.insert("credential".to_string(), cred.clone());
                }
            }
            if let Some(wh) = &rest_cfg.warehouse {
                props.insert("warehouse".to_string(), wh.clone());
            }
            if let Some(oauth_uri) = &rest_cfg.oauth2_server_uri {
                props.insert("oauth2-server-uri".to_string(), oauth_uri.clone());
            }
            if let Some(scope) = &rest_cfg.scope {
                props.insert("scope".to_string(), scope.clone());
            }

            // iceberg-catalog-rest 0.9.x requires a StorageFactory for table FileIO.
            // Match the factory to the resolved warehouse/table root so duplicate
            // seeding works for local, S3, and GCS-backed REST tables.
            let catalog = RestCatalogBuilder::default()
                .with_storage_factory(storage_factory_for_location(&warehouse_location))
                .load(&rest_cfg.catalog_name, props)
                .await
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;

            let namespace = NamespaceIdent::new(rest_cfg.namespace.clone());
            let table_name = sanitize_table_name(&rest_cfg.table);
            let table_ident = TableIdent::new(namespace, table_name);

            if !catalog.table_exists(&table_ident).await? {
                return Ok(vec![]);
            }
            let table = catalog.load_table(&table_ident).await?;
            let scan = table.scan().select(scan_cols.iter().cloned()).build()?;
            let batch_stream = scan.to_arrow().await?;
            batch_stream
                .try_filter(|b| std::future::ready(b.num_rows() > 0))
                .try_collect::<Vec<_>>()
                .await
        })
        .map_err(|err: iceberg::Error| {
            Box::new(RunError(format!("rest iceberg seed failed: {err}")))
                as Box<dyn std::error::Error + Send + Sync>
        })?;

    seed_from_batches(unique_tracker, batches, rename_back)
}

// Uses table.scan().to_arrow() so the Iceberg reader applies positional and equality deletes
// before returning rows — only live rows are seeded.
async fn collect_iceberg_batches(
    metadata_location: String,
    catalog_props: HashMap<String, String>,
    warehouse_location: String,
    entity_name: &str,
    scan_columns: &[String],
) -> Result<Vec<RecordBatch>, iceberg::Error> {
    use futures::TryStreamExt;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

    let mut props = catalog_props;
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_location);

    let catalog = MemoryCatalogBuilder::default()
        .load(ICEBERG_CATALOG_NAME, props)
        .await?;

    let namespace = NamespaceIdent::new(ICEBERG_NAMESPACE.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }

    let table_name = sanitize_table_name(entity_name);
    let table_ident = TableIdent::new(namespace, table_name);

    let table = catalog
        .register_table(&table_ident, metadata_location)
        .await?;

    let scan = table.scan().select(scan_columns.iter().cloned()).build()?;
    let batch_stream = scan.to_arrow().await?;
    batch_stream
        .try_filter(|b| std::future::ready(b.num_rows() > 0))
        .try_collect()
        .await
}
