use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{Schema, UnboundPartitionSpec};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use polars::prelude::DataFrame;

use crate::errors::RunError;
use crate::io::format::{
    AcceptedSinkAdapter, AcceptedWriteMetrics, AcceptedWriteOutput, AcceptedWritePerfBreakdown,
};
use crate::io::storage::Target;
use crate::{config, io, FloeResult};

use super::metrics;

mod context;
mod data_files;
mod glue;
mod metadata;
mod schema;

use self::context::{
    build_iceberg_write_context, create_table, ensure_namespace, sanitize_table_name,
};
use self::data_files::{iceberg_small_file_threshold_bytes, write_data_files};
use self::glue::{load_glue_table_state, upsert_glue_table};
use self::metadata::parse_metadata_version_from_location;
use self::schema::{ensure_partition_spec_matches, ensure_schema_matches, prepare_iceberg_write};

struct IcebergAcceptedAdapter;

static ICEBERG_ACCEPTED_ADAPTER: IcebergAcceptedAdapter = IcebergAcceptedAdapter;

const ICEBERG_NAMESPACE: &str = "floe";

pub(crate) fn iceberg_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &ICEBERG_ACCEPTED_ADAPTER
}

#[derive(Debug)]
struct PreparedIcebergWrite {
    iceberg_schema: Schema,
    partition_spec: Option<UnboundPartitionSpec>,
    batch: RecordBatch,
}

#[derive(Debug)]
struct IcebergWriteResult {
    files_written: u64,
    snapshot_id: Option<i64>,
    metadata_version: Option<i64>,
    file_paths: Vec<String>,
    metrics: AcceptedWriteMetrics,
    table_root_uri: String,
    iceberg_catalog_name: Option<String>,
    iceberg_database: Option<String>,
    iceberg_namespace: Option<String>,
    iceberg_table: Option<String>,
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
    glue_catalog: Option<GlueIcebergCatalogConfig>,
}

#[derive(Debug, Clone)]
struct GlueIcebergCatalogConfig {
    catalog_name: String,
    region: String,
    database: String,
    namespace: String,
    table: String,
}

impl AcceptedSinkAdapter for IcebergAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &Target,
        df: &mut DataFrame,
        mode: config::WriteMode,
        _output_stem: &str,
        _temp_dir: Option<&Path>,
        cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        catalogs: &config::CatalogResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
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
        files_written: result.files_written,
        parts_written: result.files_written,
        part_files: result.file_paths,
        table_version: result.metadata_version,
        snapshot_id: result.snapshot_id,
        table_root_uri: result
            .iceberg_catalog_name
            .as_ref()
            .map(|_| result.table_root_uri.clone()),
        iceberg_catalog_name: result.iceberg_catalog_name,
        iceberg_database: result.iceberg_database,
        iceberg_namespace: result.iceberg_namespace,
        iceberg_table: result.iceberg_table,
        metrics: result.metrics,
        merge: None,
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
        glue_catalog,
    } = write_ctx;
    let mut glue_table_state = None;
    if let Some(glue_cfg) = glue_catalog.as_ref() {
        let state = load_glue_table_state(glue_cfg).await?;
        metadata_location = state.metadata_location.clone();
        glue_table_state = Some(state);
    }
    catalog_props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), table_root_uri.clone());

    let catalog = MemoryCatalogBuilder::default()
        .load(catalog_name, catalog_props)
        .await
        .map_err(map_iceberg_err("iceberg catalog init failed"))?;
    let namespace_name = glue_catalog
        .as_ref()
        .map(|cfg| cfg.namespace.clone())
        .unwrap_or_else(|| ICEBERG_NAMESPACE.to_string());
    let namespace = NamespaceIdent::new(namespace_name);
    ensure_namespace(&catalog, &namespace).await?;
    let table_name = glue_catalog
        .as_ref()
        .map(|cfg| cfg.table.clone())
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

    if let Some(glue_cfg) = glue_catalog.as_ref() {
        upsert_glue_table(
            glue_cfg,
            &table_root_uri,
            &final_metadata_location,
            glue_table_state
                .as_ref()
                .and_then(|state| state.version_id.as_deref()),
        )
        .await?;
    }
    let metrics_start = Instant::now();
    let metrics = metrics::summarize_written_file_sizes(&file_sizes, small_file_threshold_bytes);
    perf.metrics_read_ms = Some(metrics_start.elapsed().as_millis() as u64);

    Ok(IcebergWriteResult {
        files_written,
        snapshot_id,
        metadata_version,
        file_paths,
        metrics,
        table_root_uri,
        iceberg_catalog_name: glue_catalog.as_ref().map(|cfg| cfg.catalog_name.clone()),
        iceberg_database: glue_catalog.as_ref().map(|cfg| cfg.database.clone()),
        iceberg_namespace: glue_catalog.as_ref().map(|cfg| cfg.namespace.clone()),
        iceberg_table: glue_catalog.as_ref().map(|cfg| cfg.table.clone()),
        perf,
    })
}

fn map_iceberg_err(
    context: &'static str,
) -> impl FnOnce(iceberg::Error) -> Box<dyn std::error::Error + Send + Sync> {
    move |err| Box::new(RunError(format!("{context}: {err}")))
}
