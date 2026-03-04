use polars::prelude::DataFrame;
use std::path::Path;

use crate::errors::RunError;
use crate::io::format::{
    AcceptedMergeMetrics, AcceptedSinkAdapter, AcceptedWriteMetrics, AcceptedWriteOutput,
};
use crate::io::storage::Target;
use crate::io::write::strategy::merge::{scd1, scd2, shared};
use crate::{config, io, FloeResult};

mod commit_metrics;
mod options;
pub(crate) mod record_batch;

use self::commit_metrics::delta_commit_metrics_for_target;
pub use self::commit_metrics::{
    delta_commit_metrics_from_log_bytes, delta_commit_metrics_from_log_bytes_best_effort,
    parse_delta_commit_add_stats_bytes, DeltaCommitAddStats,
};
pub use self::options::{delta_write_runtime_options, DeltaWriteRuntimeOptions};

struct DeltaAcceptedAdapter;

static DELTA_ACCEPTED_ADAPTER: DeltaAcceptedAdapter = DeltaAcceptedAdapter;

#[derive(Debug)]
struct DeltaWriteResult {
    version: i64,
    files_written: u64,
    part_files: Vec<String>,
    metrics: AcceptedWriteMetrics,
    merge: Option<AcceptedMergeMetrics>,
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
    let runtime_options = delta_write_runtime_options(entity)?;
    let partition_by = runtime_options.partition_by.clone();
    let target_file_size_bytes = runtime_options.target_file_size_bytes;
    let small_file_threshold_bytes = runtime_options.small_file_threshold_bytes;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("delta runtime init failed: {err}"))))?;
    let (version, merge) = match mode {
        config::WriteMode::Overwrite | config::WriteMode::Append => {
            let version = shared::write_standard_delta_version(
                &runtime,
                df,
                target,
                resolver,
                entity,
                mode,
                partition_by,
                target_file_size_bytes,
            )?;
            (version, None)
        }
        config::WriteMode::MergeScd1 => {
            let (version, merge) = scd1::execute_merge_scd1_with_runtime(
                &runtime,
                df,
                target,
                resolver,
                entity,
                partition_by,
                target_file_size_bytes,
            )?;
            (version, Some(merge))
        }
        config::WriteMode::MergeScd2 => {
            let (version, merge) = scd2::execute_merge_scd2_with_runtime(
                &runtime,
                df,
                target,
                resolver,
                entity,
                partition_by,
                target_file_size_bytes,
            )?;
            (version, Some(merge))
        }
    };

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
        merge,
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
            merge: result.merge,
        })
    }
}
