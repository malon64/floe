use std::path::Path;
use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, StringArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use deltalake::arrow::datatypes::{Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::logstore::read_commit_entry;
use deltalake::protocol::SaveMode;
use deltalake::table::builder::DeltaTableBuilder;
use polars::prelude::{DataFrame, DataType, TimeUnit};
use serde_json::Value;

use crate::checks::normalize;
use crate::errors::RunError;
use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteMetrics, AcceptedWriteOutput};
use crate::io::storage::{object_store, Target};
use crate::{config, io, FloeResult};

use super::metrics;

struct DeltaAcceptedAdapter;

static DELTA_ACCEPTED_ADAPTER: DeltaAcceptedAdapter = DeltaAcceptedAdapter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaWriteRuntimeOptions {
    pub partition_by: Option<Vec<String>>,
    pub target_file_size_bytes: Option<usize>,
    pub small_file_threshold_bytes: u64,
}

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

pub fn delta_write_runtime_options(
    entity: &config::EntityConfig,
) -> FloeResult<DeltaWriteRuntimeOptions> {
    let target_file_size_bytes_u64 = entity
        .sink
        .accepted
        .options
        .as_ref()
        .and_then(|options| options.max_size_per_file);
    let target_file_size_bytes = match target_file_size_bytes_u64 {
        Some(value) => Some(usize::try_from(value).map_err(|_| {
            Box::new(RunError(format!(
                "delta sink max_size_per_file is too large for this platform: {value}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?),
        None => None,
    };
    Ok(DeltaWriteRuntimeOptions {
        partition_by: entity.sink.accepted.partition_by.clone(),
        target_file_size_bytes,
        small_file_threshold_bytes: metrics::default_small_file_threshold_bytes(
            target_file_size_bytes_u64,
        ),
    })
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

fn delta_commit_metrics_for_target(
    runtime: &tokio::runtime::Runtime,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    version: i64,
    small_file_threshold_bytes: u64,
) -> FloeResult<(u64, Vec<String>, AcceptedWriteMetrics)> {
    match target {
        Target::Local { base_path, .. } => {
            let stats = delta_commit_add_stats(Path::new(base_path), version)?;
            Ok(delta_commit_stats_to_output(
                stats,
                small_file_threshold_bytes,
            ))
        }
        // Best-effort metrics for remote targets: never fail a successful write because the
        // commit log could not be read or parsed after commit.
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            match delta_commit_add_stats_via_object_store(
                runtime, target, resolver, entity, version,
            ) {
                Ok(stats) => Ok(delta_commit_stats_to_output(
                    stats,
                    small_file_threshold_bytes,
                )),
                Err(_) => Ok(delta_commit_metrics_fallback_unknown()),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeltaCommitAddStats {
    files_written: u64,
    part_files: Vec<String>,
    file_sizes: Vec<u64>,
}

fn delta_commit_add_stats(table_root: &Path, version: i64) -> FloeResult<DeltaCommitAddStats> {
    let log_path = table_root
        .join("_delta_log")
        .join(format!("{version:020}.json"));
    let bytes = std::fs::read(&log_path).map_err(|err| {
        Box::new(RunError(format!(
            "delta metrics failed to open commit log {}: {err}",
            log_path.display()
        )))
    })?;
    parse_delta_commit_add_stats_bytes_with_context(&bytes, &log_path.display().to_string())
}

fn delta_commit_add_stats_via_object_store(
    runtime: &tokio::runtime::Runtime,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    version: i64,
) -> FloeResult<DeltaCommitAddStats> {
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let builder = DeltaTableBuilder::from_url(store.table_url.clone())
        .map_err(|err| Box::new(RunError(format!("delta metrics builder failed: {err}"))))?
        .with_storage_options(store.storage_options);
    let log_store = builder.build_storage().map_err(|err| {
        Box::new(RunError(format!(
            "delta metrics log store init failed: {err}"
        )))
    })?;
    let bytes = runtime
        .block_on(async { read_commit_entry(log_store.object_store(None).as_ref(), version).await })
        .map_err(|err| Box::new(RunError(format!("delta metrics commit read failed: {err}"))))?
        .ok_or_else(|| {
            Box::new(RunError(format!(
                "delta metrics commit log missing for version {version}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    parse_delta_commit_add_stats_bytes_with_context(
        bytes.as_ref(),
        &format!("remote delta commit version {version}"),
    )
}

#[doc(hidden)]
pub fn parse_delta_commit_add_stats_bytes(bytes: &[u8]) -> FloeResult<DeltaCommitAddStats> {
    parse_delta_commit_add_stats_bytes_with_context(bytes, "delta commit log bytes")
}

fn parse_delta_commit_add_stats_bytes_with_context(
    bytes: &[u8],
    context: &str,
) -> FloeResult<DeltaCommitAddStats> {
    let content = std::str::from_utf8(bytes).map_err(|err| {
        Box::new(RunError(format!(
            "delta metrics failed to decode {context} as utf-8: {err}"
        )))
    })?;
    let mut stats = DeltaCommitAddStats::default();
    for line in content.lines() {
        let record: Value = serde_json::from_str(line).map_err(|err| {
            Box::new(RunError(format!(
                "delta metrics failed to parse {context}: {err}"
            )))
        })?;
        let Some(add) = record.get("add") else {
            continue;
        };
        stats.files_written += 1;
        if stats.part_files.len() < 50 {
            if let Some(path) = add.get("path").and_then(|value| value.as_str()) {
                let display_name = Path::new(path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| path.to_string());
                stats.part_files.push(display_name);
            }
        }
        if let Some(size) = add.get("size").and_then(|value| value.as_u64()) {
            stats.file_sizes.push(size);
        }
    }
    Ok(stats)
}

#[doc(hidden)]
pub fn delta_commit_metrics_from_log_bytes(
    bytes: &[u8],
    small_file_threshold_bytes: u64,
) -> FloeResult<(u64, Vec<String>, AcceptedWriteMetrics)> {
    let stats = parse_delta_commit_add_stats_bytes(bytes)?;
    Ok(delta_commit_stats_to_output(
        stats,
        small_file_threshold_bytes,
    ))
}

#[doc(hidden)]
pub fn delta_commit_metrics_from_log_bytes_best_effort(
    bytes: &[u8],
    small_file_threshold_bytes: u64,
) -> (u64, Vec<String>, AcceptedWriteMetrics) {
    match delta_commit_metrics_from_log_bytes(bytes, small_file_threshold_bytes) {
        Ok(output) => output,
        Err(_) => delta_commit_metrics_fallback_unknown(),
    }
}

fn delta_commit_stats_to_output(
    stats: DeltaCommitAddStats,
    small_file_threshold_bytes: u64,
) -> (u64, Vec<String>, AcceptedWriteMetrics) {
    let metrics = if stats.file_sizes.len() == stats.files_written as usize {
        metrics::summarize_written_file_sizes(&stats.file_sizes, small_file_threshold_bytes)
    } else {
        null_accepted_write_metrics()
    };
    (stats.files_written, stats.part_files, metrics)
}

fn delta_commit_metrics_fallback_unknown() -> (u64, Vec<String>, AcceptedWriteMetrics) {
    (0, Vec::new(), null_accepted_write_metrics())
}

fn null_accepted_write_metrics() -> AcceptedWriteMetrics {
    AcceptedWriteMetrics {
        total_bytes_written: None,
        avg_file_size_mb: None,
        small_files_count: None,
    }
}

fn dataframe_to_record_batch(
    df: &DataFrame,
    entity: &config::EntityConfig,
) -> FloeResult<RecordBatch> {
    if entity.schema.columns.is_empty() {
        let mut fields = Vec::with_capacity(df.width());
        let mut arrays = Vec::with_capacity(df.width());
        for column in df.get_columns() {
            let series = column.as_materialized_series();
            let name = series.name().to_string();
            let array = series_to_arrow_array(series)?;
            let nullable = array.null_count() > 0;
            fields.push(Field::new(name, array.data_type().clone(), nullable));
            arrays.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        return RecordBatch::try_new(schema, arrays).map_err(|err| {
            Box::new(RunError(format!("delta record batch build failed: {err}")))
                as Box<dyn std::error::Error + Send + Sync>
        });
    }

    let schema_columns = normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize::resolve_normalize_strategy(entity)?.as_deref(),
    );
    let mut fields = Vec::with_capacity(schema_columns.len());
    let mut arrays = Vec::with_capacity(schema_columns.len());
    for column in &schema_columns {
        let series = df
            .column(column.name.as_str())
            .map_err(|err| Box::new(RunError(format!("delta column lookup failed: {err}"))))?;
        let series = series.as_materialized_series();
        let array = series_to_arrow_array(series)?;
        let nullable = column.nullable.unwrap_or(true);
        if !nullable && array.null_count() > 0 {
            return Err(Box::new(RunError(format!(
                "delta write rejected nulls for non-nullable column {}",
                column.name
            ))));
        }
        fields.push(Field::new(
            column.name.clone(),
            array.data_type().clone(),
            nullable,
        ));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(|err| {
        Box::new(RunError(format!("delta record batch build failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

fn save_mode_for_write_mode(mode: config::WriteMode) -> SaveMode {
    match mode {
        config::WriteMode::Overwrite => SaveMode::Overwrite,
        config::WriteMode::Append => SaveMode::Append,
    }
}

fn series_to_arrow_array(series: &polars::prelude::Series) -> FloeResult<ArrayRef> {
    let array: ArrayRef = match series.dtype() {
        DataType::String => {
            let values = series.str()?;
            Arc::new(StringArray::from_iter(values))
        }
        DataType::Boolean => {
            let values = series.bool()?;
            Arc::new(BooleanArray::from_iter(values))
        }
        DataType::Int8 => {
            let values = series.i8()?;
            Arc::new(Int8Array::from_iter(values))
        }
        DataType::Int16 => {
            let values = series.i16()?;
            Arc::new(Int16Array::from_iter(values))
        }
        DataType::Int32 => {
            let values = series.i32()?;
            Arc::new(Int32Array::from_iter(values))
        }
        DataType::Int64 => {
            let values = series.i64()?;
            Arc::new(Int64Array::from_iter(values))
        }
        DataType::UInt8 => {
            let values = series.u8()?;
            Arc::new(UInt8Array::from_iter(values))
        }
        DataType::UInt16 => {
            let values = series.u16()?;
            Arc::new(UInt16Array::from_iter(values))
        }
        DataType::UInt32 => {
            let values = series.u32()?;
            Arc::new(UInt32Array::from_iter(values))
        }
        DataType::UInt64 => {
            let values = series.u64()?;
            Arc::new(UInt64Array::from_iter(values))
        }
        DataType::Float32 => {
            let values = series.f32()?;
            Arc::new(Float32Array::from_iter(values))
        }
        DataType::Float64 => {
            let values = series.f64()?;
            Arc::new(Float64Array::from_iter(values))
        }
        DataType::Date => {
            let values = series.date()?;
            Arc::new(Date32Array::from_iter(values.phys.iter()))
        }
        DataType::Datetime(unit, _) => {
            let values = series.datetime()?;
            let micros = values.phys.iter().map(|opt| match unit {
                TimeUnit::Milliseconds => opt.map(|value| value.saturating_mul(1000)),
                TimeUnit::Microseconds => opt,
                TimeUnit::Nanoseconds => opt.map(|value| value / 1000),
            });
            Arc::new(TimestampMicrosecondArray::from_iter(micros))
        }
        DataType::Time => {
            let values = series.time()?;
            Arc::new(Time64NanosecondArray::from_iter(values.phys.iter()))
        }
        DataType::Null => Arc::new(NullArray::new(series.len())),
        dtype => {
            return Err(Box::new(RunError(format!(
                "delta sink does not support dtype {dtype:?} for {}",
                series.name()
            ))))
        }
    };
    Ok(array)
}
