use deltalake::table::builder::DeltaTableBuilder;
use polars::prelude::{AnyValue, BooleanChunked, DataFrame, NewChunkedArray, PlSmallStr, Series};
use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;
use url::Url;

use crate::errors::{RunError, StorageError};
use crate::io::format::{
    AcceptedMergeMetrics, AcceptedSinkAdapter, AcceptedWriteMetrics, AcceptedWriteOutput,
};
use crate::io::read::parquet::read_parquet_lazy;
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
    merge: Option<AcceptedMergeMetrics>,
}

#[derive(Debug)]
struct DeltaMergePlanResult {
    merged_df: DataFrame,
    inserted_count: u64,
    updated_count: u64,
    target_rows_before: u64,
}

#[derive(Debug)]
struct ExistingDeltaRows {
    df: DataFrame,
    schema_columns: Vec<String>,
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
        merge: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn merge_scd1_delta_table_with_metrics(
    source_df: &mut DataFrame,
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<DeltaWriteResult> {
    if let Target::Local { base_path, .. } = target {
        std::fs::create_dir_all(Path::new(base_path))?;
    }

    let merge_start = Instant::now();
    let merge_key = resolve_merge_key(entity)?;
    ensure_source_unique_on_merge_key(source_df, &merge_key, &entity.name)?;

    let merge_plan = build_merge_plan(
        source_df, &merge_key, target, temp_dir, cloud, resolver, entity,
    )?;

    let mut merged_df = merge_plan.merged_df;
    let mut result = write_delta_table_with_metrics(
        &mut merged_df,
        target,
        resolver,
        entity,
        config::WriteMode::Overwrite,
    )?;
    result.merge = Some(AcceptedMergeMetrics {
        merge_key,
        inserted_count: merge_plan.inserted_count,
        updated_count: merge_plan.updated_count,
        target_rows_before: merge_plan.target_rows_before,
        target_rows_after: merged_df.height() as u64,
        merge_elapsed_ms: merge_start.elapsed().as_millis() as u64,
    });
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn build_merge_plan(
    source_df: &DataFrame,
    merge_key: &[String],
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<DeltaMergePlanResult> {
    let existing_target = load_existing_delta_rows(target, temp_dir, cloud, resolver, entity)?;
    if let Some(existing_target) = existing_target {
        validate_merge_schema_compatibility(
            &existing_target.df,
            source_df,
            existing_target.schema_columns.as_slice(),
            &entity.name,
        )?;
        merge_target_and_source(existing_target.df, source_df, merge_key)
    } else {
        Ok(DeltaMergePlanResult {
            merged_df: source_df.clone(),
            inserted_count: source_df.height() as u64,
            updated_count: 0,
            target_rows_before: 0,
        })
    }
}

fn merge_target_and_source(
    target_df: DataFrame,
    source_df: &DataFrame,
    merge_key: &[String],
) -> FloeResult<DeltaMergePlanResult> {
    let target_rows_before = target_df.height() as u64;
    if target_rows_before == 0 {
        return Ok(DeltaMergePlanResult {
            merged_df: source_df.clone(),
            inserted_count: source_df.height() as u64,
            updated_count: 0,
            target_rows_before,
        });
    }

    let source_key_series = key_series_for_df(source_df, merge_key, "source merge")?;
    let target_key_series = key_series_for_df(&target_df, merge_key, "target merge")?;

    let mut source_non_null_keys = HashSet::new();
    for row_idx in 0..source_df.height() {
        if let Some(key) = key_from_row(&source_key_series, row_idx)? {
            source_non_null_keys.insert(key);
        }
    }

    let mut matched_source_keys = HashSet::new();
    let mut keep_mask = vec![true; target_df.height()];
    for (row_idx, keep_slot) in keep_mask.iter_mut().enumerate() {
        let Some(target_key) = key_from_row(&target_key_series, row_idx)? else {
            continue;
        };
        if source_non_null_keys.contains(&target_key) {
            matched_source_keys.insert(target_key);
            *keep_slot = false;
        }
    }

    let updated_count = matched_source_keys.len() as u64;
    let inserted_count = (source_df.height() as u64).saturating_sub(updated_count);
    let keep = BooleanChunked::from_slice(PlSmallStr::from_static("__floe_keep"), &keep_mask);
    let mut merged_df = target_df.filter(&keep).map_err(|err| {
        Box::new(RunError(format!(
            "delta merge failed to filter unchanged rows: {err}"
        )))
    })?;
    merged_df.vstack_mut(source_df).map_err(|err| {
        Box::new(RunError(format!(
            "delta merge failed to append source rows: {err}"
        )))
    })?;

    Ok(DeltaMergePlanResult {
        merged_df,
        inserted_count,
        updated_count,
        target_rows_before,
    })
}

fn validate_merge_schema_compatibility(
    target_df: &DataFrame,
    source_df: &DataFrame,
    target_schema_columns: &[String],
    entity_name: &str,
) -> FloeResult<()> {
    if !target_schema_columns.is_empty() {
        let source_columns = source_df
            .get_column_names()
            .iter()
            .map(|name| name.as_str())
            .collect::<HashSet<_>>();
        for target_column in target_schema_columns {
            if !source_columns.contains(target_column.as_str()) {
                return Err(Box::new(RunError(format!(
                    "entity.name={} delta merge failed: source schema missing target column {}",
                    entity_name, target_column
                ))));
            }
        }
    }

    if target_df.width() == 0 {
        return Ok(());
    }

    let source_columns = source_df.get_columns();
    let target_columns = target_df.get_columns();

    for source in source_columns {
        let source_name = source.name().as_str();
        let target = target_df.column(source_name).map_err(|_| {
            Box::new(RunError(format!(
                "entity.name={} delta merge failed: target schema missing column {}",
                entity_name, source_name
            )))
        })?;
        let source_dtype = source.as_materialized_series().dtype();
        let target_dtype = target.as_materialized_series().dtype();
        if source_dtype != target_dtype {
            return Err(Box::new(RunError(format!(
                "entity.name={} delta merge failed: incompatible dtype for column {} (source={:?}, target={:?})",
                entity_name, source_name, source_dtype, target_dtype
            ))));
        }
    }

    for target in target_columns {
        let target_name = target.name().as_str();
        if source_df.column(target_name).is_err() {
            return Err(Box::new(RunError(format!(
                "entity.name={} delta merge failed: source schema missing target column {}",
                entity_name, target_name
            ))));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn load_existing_delta_rows(
    target: &Target,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<Option<ExistingDeltaRows>> {
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let builder = DeltaTableBuilder::from_url(store.table_url)
        .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
        .with_storage_options(store.storage_options);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| Box::new(RunError(format!("delta runtime init failed: {err}"))))?;
    let table = runtime.block_on(async move { builder.load().await });
    let table = match table {
        Ok(table) => table,
        Err(err) => match err {
            deltalake::DeltaTableError::NotATable(_) => return Ok(None),
            other => return Err(Box::new(RunError(format!("delta load failed: {other}")))),
        },
    };

    let file_uris = table
        .get_file_uris()
        .map_err(|err| Box::new(RunError(format!("delta list files failed: {err}"))))?
        .collect::<Vec<_>>();
    let schema_columns = table
        .snapshot()
        .map_err(|err| Box::new(RunError(format!("delta schema load failed: {err}"))))?
        .schema()
        .fields()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    if file_uris.is_empty() {
        return Ok(Some(ExistingDeltaRows {
            df: DataFrame::default(),
            schema_columns,
        }));
    }

    let mut target_df = DataFrame::default();
    for file_uri in file_uris {
        let local_path =
            resolve_delta_file_to_local_path(target, &file_uri, temp_dir, cloud, resolver, entity)?;
        let frame = read_parquet_lazy(&local_path, None)?;
        if target_df.height() == 0 && target_df.width() == 0 {
            target_df = frame;
        } else {
            target_df.vstack_mut(&frame).map_err(|err| {
                Box::new(RunError(format!(
                    "delta merge failed to concatenate target rows: {err}"
                )))
            })?;
        }
    }
    Ok(Some(ExistingDeltaRows {
        df: target_df,
        schema_columns,
    }))
}

#[allow(clippy::too_many_arguments)]
fn resolve_delta_file_to_local_path(
    target: &Target,
    file_uri: &str,
    temp_dir: Option<&Path>,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<std::path::PathBuf> {
    match target {
        Target::Local { .. } => {
            let parsed = Url::parse(file_uri)
                .ok()
                .and_then(|url| url.to_file_path().ok())
                .unwrap_or_else(|| Path::new(file_uri).to_path_buf());
            Ok(parsed)
        }
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(StorageError(format!(
                    "entity.name={} missing temp dir for delta merge read",
                    entity.name
                )))
            })?;
            let client = cloud.client_for(resolver, target.storage(), entity)?;
            client.download_to_temp(file_uri, temp_dir)
        }
    }
}

fn resolve_merge_key(entity: &config::EntityConfig) -> FloeResult<Vec<String>> {
    let primary_key = entity.schema.primary_key.as_ref().ok_or_else(|| {
        Box::new(RunError(format!(
            "entity.name={} sink.write_mode=merge_scd1 requires schema.primary_key",
            entity.name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    if primary_key.is_empty() {
        return Err(Box::new(RunError(format!(
            "entity.name={} sink.write_mode=merge_scd1 requires non-empty schema.primary_key",
            entity.name
        ))));
    }
    Ok(primary_key.clone())
}

fn ensure_source_unique_on_merge_key(
    source_df: &DataFrame,
    merge_key: &[String],
    entity_name: &str,
) -> FloeResult<()> {
    let key_series = key_series_for_df(source_df, merge_key, "source merge key")?;
    let mut seen = HashSet::new();
    let mut duplicate_count = 0_u64;
    for row_idx in 0..source_df.height() {
        let Some(key) = key_from_row(&key_series, row_idx)? else {
            continue;
        };
        if !seen.insert(key) {
            duplicate_count += 1;
        }
    }
    if duplicate_count > 0 {
        return Err(Box::new(RunError(format!(
            "entity.name={} merge_scd1 source is not unique on merge_key [{}]: {} duplicate row(s) found (ambiguous merge)",
            entity_name,
            merge_key.join(","),
            duplicate_count
        ))));
    }
    Ok(())
}

fn key_series_for_df(df: &DataFrame, columns: &[String], context: &str) -> FloeResult<Vec<Series>> {
    let mut series = Vec::with_capacity(columns.len());
    for column in columns {
        let col = df.column(column).map_err(|err| {
            Box::new(RunError(format!(
                "delta merge {context} column {} not found: {err}",
                column
            )))
        })?;
        series.push(col.as_materialized_series().rechunk());
    }
    Ok(series)
}

fn key_from_row(columns: &[Series], row_idx: usize) -> FloeResult<Option<String>> {
    let mut encoded = String::new();
    for (column_idx, series) in columns.iter().enumerate() {
        let value = series.get(row_idx).map_err(|err| {
            Box::new(RunError(format!(
                "delta merge key read failed at row {}: {err}",
                row_idx
            )))
        })?;
        let Some(token) = encode_key_component(value) else {
            return Ok(None);
        };
        if column_idx > 0 {
            encoded.push('\u{1f}');
        }
        encoded.push_str(&token);
    }
    Ok(Some(encoded))
}

fn encode_key_component(value: AnyValue) -> Option<String> {
    match value {
        AnyValue::Null => None,
        AnyValue::String(text) => Some(format!("s:{text}")),
        AnyValue::StringOwned(text) => Some(format!("s:{text}")),
        AnyValue::Boolean(flag) => Some(format!("b:{flag}")),
        AnyValue::Int8(number) => Some(format!("i:{number}")),
        AnyValue::Int16(number) => Some(format!("i:{number}")),
        AnyValue::Int32(number) => Some(format!("i:{number}")),
        AnyValue::Int64(number) => Some(format!("i:{number}")),
        AnyValue::UInt8(number) => Some(format!("u:{number}")),
        AnyValue::UInt16(number) => Some(format!("u:{number}")),
        AnyValue::UInt32(number) => Some(format!("u:{number}")),
        AnyValue::UInt64(number) => Some(format!("u:{number}")),
        AnyValue::Float32(number) => Some(format!("f:{}", (number as f64).to_bits())),
        AnyValue::Float64(number) => Some(format!("f:{}", number.to_bits())),
        AnyValue::Date(number) => Some(format!("d:{number}")),
        AnyValue::Datetime(number, unit, _) => Some(format!("dt:{unit:?}:{number}")),
        AnyValue::Time(number) => Some(format!("t:{number}")),
        AnyValue::Duration(number, unit) => Some(format!("dur:{unit:?}:{number}")),
        AnyValue::Binary(binary) => Some(format!("bin:{binary:?}")),
        AnyValue::BinaryOwned(binary) => Some(format!("bin:{binary:?}")),
        AnyValue::Categorical(idx, _) => Some(format!("cat:{idx}")),
        AnyValue::Enum(idx, _) => Some(format!("enum:{idx}")),
        AnyValue::Int128(value) => Some(format!("i128:{value}")),
        AnyValue::UInt128(value) => Some(format!("u128:{value}")),
        _ => Some(format!("{value:?}")),
    }
}

impl AcceptedSinkAdapter for DeltaAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &Target,
        df: &mut DataFrame,
        mode: config::WriteMode,
        _output_stem: &str,
        temp_dir: Option<&Path>,
        cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        _catalogs: &config::CatalogResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        let result = match mode {
            config::WriteMode::Overwrite | config::WriteMode::Append => {
                write_delta_table_with_metrics(df, target, resolver, entity, mode)?
            }
            config::WriteMode::MergeScd1 => {
                merge_scd1_delta_table_with_metrics(df, target, temp_dir, cloud, resolver, entity)?
            }
        };
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
