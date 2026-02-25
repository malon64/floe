use deltalake::table::builder::DeltaTableBuilder;
use polars::prelude::DataFrame;
use std::path::Path;

use crate::errors::RunError;
use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteMetrics, AcceptedWriteOutput};
use crate::io::storage::{object_store, Target};
use crate::{config, io, FloeResult};

mod commit_metrics;
mod options;
mod record_batch;

use self::commit_metrics::delta_commit_metrics_for_target;
pub use self::commit_metrics::{
    delta_commit_metrics_from_log_bytes, delta_commit_metrics_from_log_bytes_best_effort,
    parse_delta_commit_add_stats_bytes, DeltaCommitAddStats,
};
pub use self::options::{delta_write_runtime_options, DeltaWriteRuntimeOptions};
use self::record_batch::{dataframe_to_record_batch, save_mode_for_write_mode};

struct DeltaAcceptedAdapter;

static DELTA_ACCEPTED_ADAPTER: DeltaAcceptedAdapter = DeltaAcceptedAdapter;

#[derive(Debug)]
struct DeltaWriteResult {
    version: i64,
    files_written: u64,
    part_files: Vec<String>,
    metrics: AcceptedWriteMetrics,
}

pub(crate) fn delta_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &DELTA_ACCEPTED_ADAPTER
}

pub fn write_delta_table(
    df: &mut DataFrame,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> FloeResult<i64> {
    Ok(write_delta_table_with_metrics(df, target, resolver, entity, mode)?.version)
}

fn write_delta_table_with_metrics(
    df: &mut DataFrame,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> FloeResult<DeltaWriteResult> {
    if let Target::Local { base_path, .. } = target {
        std::fs::create_dir_all(Path::new(base_path))?;
    }
    let batch = dataframe_to_record_batch(df, entity)?;
    let runtime_options = delta_write_runtime_options(entity)?;
    let partition_by = runtime_options.partition_by.clone();
    let target_file_size_bytes = runtime_options.target_file_size_bytes;
    let small_file_threshold_bytes = runtime_options.small_file_threshold_bytes;
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let table_url = store.table_url;
    let storage_options = store.storage_options;
    let builder = DeltaTableBuilder::from_url(table_url.clone())
        .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
        .with_storage_options(storage_options.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("delta runtime init failed: {err}"))))?;
    let version = runtime
        .block_on(async move {
            let table = match builder.load().await {
                Ok(table) => table,
                Err(err) => match err {
                    deltalake::DeltaTableError::NotATable(_) => {
                        let builder = DeltaTableBuilder::from_url(table_url)?
                            .with_storage_options(storage_options);
                        builder.build()?
                    }
                    other => return Err(other),
                },
            };
            let mut write = table
                .write(vec![batch])
                .with_save_mode(save_mode_for_write_mode(mode));
            if let Some(partition_by) = partition_by.clone() {
                write = write.with_partition_columns(partition_by);
            }
            if let Some(target_file_size) = target_file_size_bytes {
                write = write.with_target_file_size(target_file_size);
            }
            let table = write.await?;
            let version = table.version().ok_or_else(|| {
                deltalake::DeltaTableError::Generic(
                    "delta table version missing after write".to_string(),
                )
            })?;
            Ok::<i64, deltalake::DeltaTableError>(version)
        })
        .map_err(|err| Box::new(RunError(format!("delta write failed: {err}"))))?;

    let (files_written, part_files, metrics) = delta_commit_metrics_for_target(
        &runtime,
        target,
        resolver,
        entity,
        version,
        small_file_threshold_bytes,
    )?;

    Ok(DeltaWriteResult {
        version,
        files_written,
        part_files,
        metrics,
    })
}

impl AcceptedSinkAdapter for DeltaAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &Target,
        df: &mut DataFrame,
        mode: config::WriteMode,
        _output_stem: &str,
        _temp_dir: Option<&Path>,
        _cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        _catalogs: &config::CatalogResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        let result = write_delta_table_with_metrics(df, target, resolver, entity, mode)?;
        Ok(AcceptedWriteOutput {
            files_written: result.files_written,
            parts_written: 1,
            part_files: result.part_files,
            table_version: Some(result.version),
            snapshot_id: None,
            table_root_uri: None,
            iceberg_catalog_name: None,
            iceberg_database: None,
            iceberg_namespace: None,
            iceberg_table: None,
            metrics: result.metrics,
        })
    }
}
