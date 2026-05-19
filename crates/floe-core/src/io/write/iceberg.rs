use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{Schema, UnboundPartitionSpec};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_storage_opendal::OpenDalStorageFactory;
use polars::prelude::DataFrame;

use crate::errors::RunError;
use crate::io::format::{
    AcceptedWriteMetrics, AcceptedWriteOutput, AcceptedWritePerfBreakdown, AcceptedWriteRequest,
    CatalogRegistration,
};
use crate::io::storage::{object_store, Target};
use crate::io::unique_seed::seed_from_batches;
use crate::io::write::sink_format::{SeedContext, SinkFormat};
use crate::{check, config, io, FloeResult};

use super::metrics;

mod context;
mod data_files;
mod glue;
pub(crate) mod metadata;
pub(crate) mod rest;
mod schema;

pub(crate) use self::context::sanitize_table_name;
use self::context::{build_iceberg_write_context, create_table, ensure_namespace};
use self::data_files::{iceberg_small_file_threshold_bytes, write_data_files};
pub(crate) use self::glue::load_glue_table_state;
use self::glue::upsert_glue_table;
use self::metadata::{
    latest_gcs_metadata_location, latest_local_metadata_location, latest_s3_metadata_location,
    parse_metadata_version_from_location,
};
pub(crate) use self::rest::{build_rest_catalog, write_via_rest_catalog, RestIcebergCatalogConfig};
use self::schema::{ensure_partition_spec_matches, ensure_schema_matches, prepare_iceberg_write};

pub(crate) struct IcebergSinkFormat;

pub(crate) static ICEBERG_SINK_FORMAT: IcebergSinkFormat = IcebergSinkFormat;

pub(crate) const ICEBERG_NAMESPACE: &str = "floe";
pub(crate) const ICEBERG_CATALOG_NAME: &str = "floe_iceberg";

#[derive(Debug)]
pub(crate) struct PreparedIcebergWrite {
    pub(crate) iceberg_schema: Schema,
    pub(crate) partition_spec: Option<UnboundPartitionSpec>,
    pub(crate) batch: RecordBatch,
}

#[derive(Debug)]
pub(crate) struct IcebergWriteResult {
    files_written: u64,
    snapshot_id: Option<i64>,
    metadata_version: Option<i64>,
    file_paths: Vec<String>,
    metrics: AcceptedWriteMetrics,
    table_root_uri: String,
    catalog: Option<CatalogRegistration>,
    perf: AcceptedWritePerfBreakdown,
}

struct IcebergRemoteContext<'a> {
    cloud: &'a mut io::storage::CloudClient,
    resolver: &'a config::StorageResolver,
    catalogs: &'a config::CatalogResolver,
}

struct IcebergWriteContext {
    table_root_uri: String,
    catalog_name: &'static str,
    catalog_props: HashMap<String, String>,
    metadata_location: Option<String>,
    catalog: Option<IcebergCatalogConfig>,
}

/// Per-catalog-type configuration used at write and seed time.
/// Add a new variant here when supporting a new catalog type.
#[derive(Debug, Clone)]
pub(crate) enum IcebergCatalogConfig {
    Glue(GlueIcebergCatalogConfig),
    Rest(RestIcebergCatalogConfig),
}

impl IcebergCatalogConfig {
    /// Constructs the catalog config from a resolved catalog target.
    /// This is the single dispatch point for catalog_type → variant mapping.
    /// Add a new arm here when supporting a new catalog type.
    pub(crate) fn from_resolved(
        resolved: &config::ResolvedIcebergCatalogTarget,
    ) -> FloeResult<Self> {
        match &resolved.type_config {
            config::CatalogTypeConfig::Glue {
                region,
                database,
                create_database_if_missing,
                allow_takeover,
            } => Ok(Self::Glue(GlueIcebergCatalogConfig {
                catalog_name: resolved.catalog_name.clone(),
                region: region.clone(),
                database: database.clone(),
                namespace: resolved.namespace.clone(),
                table: resolved.table.clone(),
                create_database_if_missing: *create_database_if_missing,
                allow_takeover: *allow_takeover,
            })),
            config::CatalogTypeConfig::Rest { .. } => {
                Ok(Self::Rest(RestIcebergCatalogConfig::from_type_config(
                    resolved.catalog_name.clone(),
                    &resolved.type_config,
                    resolved.namespace.clone(),
                    resolved.table.clone(),
                )?))
            }
            // Unity catalogs are Delta-only; validate.rs blocks this path.
            config::CatalogTypeConfig::Unity { .. } => Err(Box::new(RunError(format!(
                "IcebergCatalogConfig::from_resolved called on unity catalog '{}'",
                resolved.catalog_name
            )))),
        }
    }

    pub(crate) fn catalog_name(&self) -> &str {
        match self {
            Self::Glue(cfg) => &cfg.catalog_name,
            Self::Rest(cfg) => &cfg.catalog_name,
        }
    }

    pub(crate) fn database(&self) -> Option<&str> {
        match self {
            Self::Glue(cfg) => Some(&cfg.database),
            Self::Rest(_) => None,
        }
    }

    pub(crate) fn namespace(&self) -> &str {
        match self {
            Self::Glue(cfg) => &cfg.namespace,
            Self::Rest(cfg) => &cfg.namespace,
        }
    }

    pub(crate) fn table(&self) -> &str {
        match self {
            Self::Glue(cfg) => &cfg.table,
            Self::Rest(cfg) => &cfg.table,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GlueIcebergCatalogConfig {
    pub(crate) catalog_name: String,
    pub(crate) region: String,
    pub(crate) database: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) create_database_if_missing: bool,
    pub(crate) allow_takeover: bool,
}

impl SinkFormat for IcebergSinkFormat {
    fn format_name(&self) -> &'static str {
        "iceberg"
    }

    fn supported_modes(&self) -> &'static [config::WriteMode] {
        &[config::WriteMode::Overwrite, config::WriteMode::Append]
    }

    fn supported_storages(&self) -> &'static [&'static str] {
        &["local", "s3", "gcs"]
    }

    fn write(&self, req: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput> {
        let AcceptedWriteRequest {
            target,
            df,
            mode,
            cloud,
            resolver,
            catalogs,
            entity,
            ..
        } = req;
        write_iceberg_table_with_remote_context(
            df,
            target,
            entity,
            mode,
            Some(IcebergRemoteContext {
                cloud,
                resolver,
                catalogs,
            }),
        )
    }

    fn seed_unique_tracker(
        &self,
        tracker: &mut check::UniqueTracker,
        ctx: &mut SeedContext<'_>,
    ) -> FloeResult<()> {
        // When a catalog is configured, seed from the catalog-reported location (Glue/REST).
        if let Some(resolved) = ctx.catalogs.resolve_iceberg_target(
            ctx.resolver,
            ctx.entity,
            &ctx.entity.sink.accepted,
        )? {
            let catalog_cfg = IcebergCatalogConfig::from_resolved(&resolved)?;
            let catalog_target = Target::from_resolved(&resolved.table_location)?;
            let store =
                object_store::iceberg_store_config(&catalog_target, ctx.resolver, ctx.entity)?;
            return seed_iceberg_from_catalog(
                tracker,
                &catalog_cfg,
                store.file_io_props,
                store.warehouse_location,
                ctx.entity,
                ctx.scan_cols,
                ctx.rename_back,
            );
        }

        let store = object_store::iceberg_store_config(ctx.target, ctx.resolver, ctx.entity)?;

        let metadata_location: Option<String> = match ctx.target {
            Target::Local { base_path, .. } => {
                latest_local_metadata_location(Path::new(base_path))?
            }
            Target::S3 {
                storage, base_key, ..
            } => {
                let client = ctx.cloud.client_for(ctx.resolver, storage, ctx.entity)?;
                latest_s3_metadata_location(client, base_key)?
            }
            Target::Gcs {
                storage, base_key, ..
            } => {
                let client = ctx.cloud.client_for(ctx.resolver, storage, ctx.entity)?;
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
                &ctx.entity.name,
                ctx.scan_cols,
            ))
            .map_err(|err| Box::new(RunError(format!("iceberg seed failed: {err}"))))?;
        seed_from_batches(tracker, batches, ctx.rename_back)
    }
}

pub fn write_iceberg_table(
    df: &mut DataFrame,
    target: &Target,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> FloeResult<AcceptedWriteOutput> {
    write_iceberg_table_with_remote_context(df, target, entity, mode, None)
}

fn write_iceberg_table_with_remote_context(
    df: &mut DataFrame,
    target: &Target,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    mut remote: Option<IcebergRemoteContext<'_>>,
) -> FloeResult<AcceptedWriteOutput> {
    let write_ctx = build_iceberg_write_context(target, entity, mode, remote.as_mut())?;
    let conversion_start = Instant::now();
    let prepared = prepare_iceberg_write(df, entity)?;
    let conversion_ms = conversion_start.elapsed().as_millis() as u64;
    let small_file_threshold_bytes = iceberg_small_file_threshold_bytes(entity);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("iceberg runtime init failed: {err}"))))?;

    let mut result = runtime.block_on(write_iceberg_table_async(
        write_ctx,
        prepared,
        entity,
        mode,
        small_file_threshold_bytes,
    ))?;
    result.perf.conversion_ms = Some(conversion_ms);
    Ok(AcceptedWriteOutput {
        files_written: Some(result.files_written),
        parts_written: result.files_written,
        part_files: result.file_paths,
        table_version: result.metadata_version,
        snapshot_id: result.snapshot_id,
        table_root_uri: result
            .catalog
            .as_ref()
            .map(|_| result.table_root_uri.clone()),
        catalog: result.catalog,
        metrics: result.metrics,
        merge: None,
        schema_evolution: crate::io::format::AcceptedSchemaEvolution {
            enabled: false,
            mode: entity
                .schema
                .resolved_schema_evolution()
                .mode
                .as_str()
                .to_string(),
            applied: false,
            added_columns: Vec::new(),
            incompatible_changes_detected: false,
        },
        perf: Some(result.perf),
    })
}

async fn write_iceberg_table_async(
    write_ctx: IcebergWriteContext,
    prepared: PreparedIcebergWrite,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    small_file_threshold_bytes: u64,
) -> FloeResult<IcebergWriteResult> {
    let IcebergWriteContext {
        table_root_uri,
        catalog_name,
        mut catalog_props,
        mut metadata_location,
        catalog: catalog_cfg,
    } = write_ctx;

    // REST catalog takes a completely different code path: it connects to a remote server.
    if let Some(IcebergCatalogConfig::Rest(rest_cfg)) = catalog_cfg.as_ref() {
        return write_via_rest_catalog(
            rest_cfg,
            table_root_uri,
            catalog_props,
            prepared,
            entity,
            mode,
            small_file_threshold_bytes,
        )
        .await;
    }

    let mut glue_state = None;
    if let Some(IcebergCatalogConfig::Glue(glue_cfg)) = catalog_cfg.as_ref() {
        let state = load_glue_table_state(glue_cfg).await?;
        metadata_location = state.metadata_location.clone();
        glue_state = Some(state);
    }
    catalog_props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), table_root_uri.clone());

    let is_local = !table_root_uri.starts_with("s3://")
        && !table_root_uri.starts_with("s3a://")
        && !table_root_uri.starts_with("gs://")
        && !table_root_uri.starts_with("az://")
        && !table_root_uri.starts_with("abfss://");
    let mut catalog_builder = MemoryCatalogBuilder::default();
    if is_local {
        catalog_builder =
            catalog_builder.with_storage_factory(std::sync::Arc::new(LocalFsStorageFactory));
    } else if table_root_uri.starts_with("s3://") || table_root_uri.starts_with("s3a://") {
        let scheme = table_root_uri
            .split("://")
            .next()
            .unwrap_or("s3")
            .to_string();
        catalog_builder =
            catalog_builder.with_storage_factory(std::sync::Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: scheme,
                customized_credential_load: None,
            }));
    } else if table_root_uri.starts_with("gs://") {
        catalog_builder =
            catalog_builder.with_storage_factory(std::sync::Arc::new(OpenDalStorageFactory::Gcs));
    }
    let catalog = catalog_builder
        .load(catalog_name, catalog_props)
        .await
        .map_err(map_iceberg_err("iceberg catalog init failed"))?;
    let namespace_name = catalog_cfg
        .as_ref()
        .map(|cfg| cfg.namespace().to_string())
        .unwrap_or_else(|| ICEBERG_NAMESPACE.to_string());
    let namespace = NamespaceIdent::new(namespace_name);
    ensure_namespace(&catalog, &namespace).await?;
    let table_name = catalog_cfg
        .as_ref()
        .map(|cfg| cfg.table().to_string())
        .unwrap_or_else(|| sanitize_table_name(&entity.name));
    let table_ident = TableIdent::new(namespace.clone(), table_name);

    let existing_table = if let Some(location) = metadata_location.as_ref() {
        Some(
            catalog
                .register_table(&table_ident, location.clone())
                .await
                .map_err(map_iceberg_err("iceberg register existing table failed"))?,
        )
    } else {
        None
    };

    if let Some(existing) = existing_table.as_ref() {
        ensure_schema_matches(
            existing.metadata().current_schema(),
            &prepared.iceberg_schema,
            entity,
        )?;
        ensure_partition_spec_matches(
            existing.metadata().default_partition_spec(),
            prepared.partition_spec.as_ref(),
            &prepared.iceberg_schema,
            entity,
        )?;
    }

    let table = match mode {
        config::WriteMode::Append => match existing_table {
            Some(table) => table,
            None => {
                create_table(
                    &catalog,
                    &namespace,
                    &table_ident,
                    table_root_uri.clone(),
                    &prepared.iceberg_schema,
                    prepared.partition_spec.clone(),
                )
                .await?
            }
        },
        config::WriteMode::Overwrite => {
            if existing_table.is_some() {
                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(map_iceberg_err("iceberg drop table mapping failed"))?;
            }
            create_table(
                &catalog,
                &namespace,
                &table_ident,
                table_root_uri.clone(),
                &prepared.iceberg_schema,
                prepared.partition_spec.clone(),
            )
            .await?
        }
        config::WriteMode::MergeScd1 | config::WriteMode::MergeScd2 => {
            return Err(Box::new(RunError(format!(
                "entity.name={} sink.write_mode={} is only supported for delta accepted sinks",
                entity.name,
                mode.as_str()
            ))));
        }
    };

    let mut file_paths = Vec::new();
    let mut file_sizes = Vec::new();
    let mut files_written = 0_u64;
    let mut table_after_write = table;
    let mut perf = AcceptedWritePerfBreakdown::default();

    if prepared.batch.num_rows() > 0 {
        let data_write_start = Instant::now();
        let data_files = write_data_files(&table_after_write, prepared.batch).await?;
        perf.data_write_ms = Some(data_write_start.elapsed().as_millis() as u64);
        files_written = data_files.len() as u64;
        // Iceberg returns DataFile entries for data files only, so these sizes exclude
        // metadata/manifests and match the accepted-output metrics semantics.
        file_sizes = data_files
            .iter()
            .map(iceberg::spec::DataFile::file_size_in_bytes)
            .collect();
        file_paths = data_files
            .iter()
            .map(|file| {
                let file_path = file.file_path().to_string();
                Path::new(file_path.as_str())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or(file_path)
            })
            .take(50)
            .collect();

        let tx = Transaction::new(&table_after_write);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .map_err(map_iceberg_err("iceberg append transaction apply failed"))?;
        let commit_start = Instant::now();
        table_after_write = tx
            .commit(&catalog)
            .await
            .map_err(map_iceberg_err("iceberg commit failed"))?;
        perf.commit_ms = Some(commit_start.elapsed().as_millis() as u64);
    }

    let snapshot_id = table_after_write
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let metadata_version = table_after_write
        .metadata_location()
        .and_then(parse_metadata_version_from_location);
    let final_metadata_location = table_after_write
        .metadata_location()
        .map(|value| value.to_string())
        .ok_or_else(|| {
            Box::new(RunError(
                "iceberg table metadata location missing after commit".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

    if let Some(IcebergCatalogConfig::Glue(glue_cfg)) = catalog_cfg.as_ref() {
        upsert_glue_table(
            glue_cfg,
            &table_root_uri,
            &final_metadata_location,
            glue_state
                .as_ref()
                .and_then(|state| state.version_id.as_deref()),
        )
        .await?;
    }
    let metrics_start = Instant::now();
    let metrics = metrics::summarize_written_file_sizes(
        &file_sizes,
        files_written,
        small_file_threshold_bytes,
    );
    perf.metrics_read_ms = Some(metrics_start.elapsed().as_millis() as u64);

    let catalog = catalog_cfg
        .as_ref()
        .map(|cfg| CatalogRegistration::IcebergGlue {
            catalog_name: cfg.catalog_name().to_string(),
            database: cfg.database().map(ToOwned::to_owned),
            namespace: cfg.namespace().to_string(),
            table: cfg.table().to_string(),
        });
    Ok(IcebergWriteResult {
        files_written,
        snapshot_id,
        metadata_version,
        file_paths,
        metrics,
        table_root_uri,
        catalog,
        perf,
    })
}

pub(crate) fn map_iceberg_err(
    context: &'static str,
) -> impl FnOnce(iceberg::Error) -> Box<dyn std::error::Error + Send + Sync> {
    move |err| Box::new(RunError(format!("{context}: {err}")))
}

// ── Seeding helpers ───────────────────────────────────────────────────────────

fn seed_iceberg_from_catalog(
    tracker: &mut check::UniqueTracker,
    catalog_cfg: &IcebergCatalogConfig,
    file_io_props: HashMap<String, String>,
    warehouse_location: String,
    entity: &config::EntityConfig,
    scan_cols: &[String],
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    match catalog_cfg {
        IcebergCatalogConfig::Glue(glue_cfg) => {
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
            seed_from_batches(tracker, batches, rename_back)
        }
        IcebergCatalogConfig::Rest(rest_cfg) => {
            seed_iceberg_from_rest(tracker, rest_cfg, file_io_props, scan_cols, rename_back)
        }
    }
}

fn seed_iceberg_from_rest(
    tracker: &mut check::UniqueTracker,
    rest_cfg: &RestIcebergCatalogConfig,
    file_io_props: HashMap<String, String>,
    scan_cols: &[String],
    rename_back: &HashMap<String, String>,
) -> FloeResult<()> {
    use futures::TryStreamExt;
    use iceberg::{Catalog, NamespaceIdent, TableIdent};
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
            let catalog = build_rest_catalog(rest_cfg, file_io_props).await?;
            let namespace = NamespaceIdent::new(rest_cfg.namespace.clone());

            if !catalog
                .namespace_exists(&namespace)
                .await
                .map_err(map_iceberg_err("rest iceberg seed namespace_exists failed"))?
            {
                return Ok(Vec::new());
            }

            let table_ident = TableIdent::new(namespace, rest_cfg.table.clone());
            if !catalog
                .table_exists(&table_ident)
                .await
                .map_err(map_iceberg_err("rest iceberg seed table_exists failed"))?
            {
                return Ok(Vec::new());
            }

            let table = catalog
                .load_table(&table_ident)
                .await
                .map_err(map_iceberg_err("rest iceberg seed load_table failed"))?;

            let scan = table
                .scan()
                .select(scan_cols.iter().cloned())
                .build()
                .map_err(map_iceberg_err("rest iceberg seed scan build failed"))?;

            scan.to_arrow()
                .await
                .map_err(map_iceberg_err("rest iceberg seed to_arrow failed"))?
                .try_filter(|b| std::future::ready(b.num_rows() > 0))
                .try_collect::<Vec<_>>()
                .await
                .map_err(map_iceberg_err("rest iceberg seed collect failed"))
        })
        .map_err(|err: Box<dyn std::error::Error + Send + Sync>| {
            Box::new(RunError(format!("rest iceberg seed failed: {err}")))
        })?;

    seed_from_batches(tracker, batches, rename_back)
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
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

    let is_local = !warehouse_location.starts_with("s3://")
        && !warehouse_location.starts_with("s3a://")
        && !warehouse_location.starts_with("gs://")
        && !warehouse_location.starts_with("az://")
        && !warehouse_location.starts_with("abfss://");

    let mut props = catalog_props;
    props.insert(
        MEMORY_CATALOG_WAREHOUSE.to_string(),
        warehouse_location.clone(),
    );

    let mut catalog_builder = MemoryCatalogBuilder::default();
    if is_local {
        catalog_builder =
            catalog_builder.with_storage_factory(std::sync::Arc::new(LocalFsStorageFactory));
    } else if warehouse_location.starts_with("s3://") || warehouse_location.starts_with("s3a://") {
        let scheme = warehouse_location
            .split("://")
            .next()
            .unwrap_or("s3")
            .to_string();
        catalog_builder =
            catalog_builder.with_storage_factory(std::sync::Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: scheme,
                customized_credential_load: None,
            }));
    } else if warehouse_location.starts_with("gs://") {
        catalog_builder =
            catalog_builder.with_storage_factory(std::sync::Arc::new(OpenDalStorageFactory::Gcs));
    }
    let catalog = catalog_builder.load(ICEBERG_CATALOG_NAME, props).await?;

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
