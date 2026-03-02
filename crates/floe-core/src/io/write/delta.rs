use deltalake::table::builder::DeltaTableBuilder;
use deltalake::{datafusion::prelude::SessionContext, DeltaTable};
use polars::prelude::{AnyValue, DataFrame, Series};
use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use crate::errors::RunError;
use crate::io::format::{
    AcceptedMergeMetrics, AcceptedSinkAdapter, AcceptedWriteMetrics, AcceptedWriteOutput,
};
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
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<DeltaWriteResult> {
    if let Target::Local { base_path, .. } = target {
        std::fs::create_dir_all(Path::new(base_path))?;
    }

    let merge_start = Instant::now();
    let merge_key = resolve_merge_key(entity)?;
    ensure_source_unique_on_merge_key(source_df, &merge_key, &entity.name)?;
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

    let loaded_table = runtime
        .block_on(async move { builder.load().await })
        .map(Some)
        .or_else(|err| match err {
            deltalake::DeltaTableError::NotATable(_) => Ok(None),
            other => Err(Box::new(RunError(format!("delta load failed: {other}")))),
        })?;
    if loaded_table.is_none() {
        let mut result = write_delta_table_with_metrics(
            source_df,
            target,
            resolver,
            entity,
            config::WriteMode::Append,
        )?;
        result.merge = Some(AcceptedMergeMetrics {
            merge_key,
            inserted_count: source_df.height() as u64,
            updated_count: 0,
            target_rows_before: 0,
            target_rows_after: source_df.height() as u64,
            merge_elapsed_ms: merge_start.elapsed().as_millis() as u64,
        });
        return Ok(result);
    }

    let table = loaded_table.expect("checked is_some");
    let target_schema_columns = delta_schema_columns(&table)?;
    validate_merge_schema_compatibility(&target_schema_columns, source_df, &entity.name)?;
    let source = source_as_datafusion_df(source_df, entity)?;
    let source_columns = source_df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let merge_key_set = merge_key.iter().map(String::as_str).collect::<HashSet<_>>();
    let update_columns = source_columns
        .iter()
        .filter(|name| !merge_key_set.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let predicate = merge_predicate_sql(&merge_key);
    let merge_result = runtime.block_on(async move {
        let mut merge = table
            .merge(source, predicate)
            .with_source_alias("source")
            .with_target_alias("target");
        if !update_columns.is_empty() {
            let update_cols = update_columns.clone();
            merge = merge.when_matched_update(|update| {
                update_cols.iter().fold(update, |builder, column| {
                    builder.update(
                        qualified_column("target", column),
                        qualified_column("source", column),
                    )
                })
            })?;
        }
        let insert_cols = source_columns.clone();
        merge = merge.when_not_matched_insert(|insert| {
            insert_cols.iter().fold(insert, |builder, column| {
                builder.set(
                    qualified_column("target", column),
                    qualified_column("source", column),
                )
            })
        })?;
        merge.await
    });
    let (table, merge_metrics) =
        merge_result.map_err(|err| Box::new(RunError(format!("delta merge failed: {err}"))))?;
    let version = table.version().ok_or_else(|| {
        Box::new(RunError(
            "delta table version missing after merge".to_string(),
        ))
    })?;

    let runtime_options = delta_write_runtime_options(entity)?;
    let small_file_threshold_bytes = runtime_options.small_file_threshold_bytes;
    let (files_written, part_files, metrics) = delta_commit_metrics_for_target(
        &runtime,
        target,
        resolver,
        entity,
        version,
        small_file_threshold_bytes,
    )?;

    let target_rows_before = (merge_metrics.num_target_rows_copied
        + merge_metrics.num_target_rows_updated
        + merge_metrics.num_target_rows_deleted) as u64;
    let target_rows_after = merge_metrics.num_output_rows as u64;
    Ok(DeltaWriteResult {
        version,
        files_written,
        part_files,
        metrics,
        merge: Some(AcceptedMergeMetrics {
            merge_key,
            inserted_count: merge_metrics.num_target_rows_inserted as u64,
            updated_count: merge_metrics.num_target_rows_updated as u64,
            target_rows_before,
            target_rows_after,
            merge_elapsed_ms: merge_start.elapsed().as_millis() as u64,
        }),
    })
}

fn delta_schema_columns(table: &DeltaTable) -> FloeResult<Vec<String>> {
    let columns = table
        .snapshot()
        .map_err(|err| Box::new(RunError(format!("delta schema load failed: {err}"))))?
        .schema()
        .fields()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    Ok(columns)
}

fn validate_merge_schema_compatibility(
    target_schema_columns: &[String],
    source_df: &DataFrame,
    entity_name: &str,
) -> FloeResult<()> {
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

    let target_columns = target_schema_columns
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for source_column in source_columns {
        if !target_columns.contains(source_column) {
            return Err(Box::new(RunError(format!(
                "entity.name={} delta merge failed: target schema missing source column {}",
                entity_name, source_column
            ))));
        }
    }
    Ok(())
}

fn source_as_datafusion_df(
    source_df: &DataFrame,
    entity: &config::EntityConfig,
) -> FloeResult<deltalake::datafusion::prelude::DataFrame> {
    let clone = source_df.clone();
    let batch = dataframe_to_record_batch(&clone, entity)?;
    SessionContext::new().read_batch(batch).map_err(|err| {
        Box::new(RunError(format!(
            "entity.name={} delta merge failed to build source dataframe: {err}",
            entity.name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

fn merge_predicate_sql(merge_key: &[String]) -> String {
    merge_key
        .iter()
        .map(|column| {
            format!(
                "{} = {}",
                qualified_column("target", column),
                qualified_column("source", column)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn qualified_column(alias: &str, column: &str) -> String {
    format!("{alias}.`{}`", column.replace('`', "``"))
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
        _temp_dir: Option<&Path>,
        _cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        _catalogs: &config::CatalogResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        let result = match mode {
            config::WriteMode::Overwrite | config::WriteMode::Append => {
                write_delta_table_with_metrics(df, target, resolver, entity, mode)?
            }
            config::WriteMode::MergeScd1 => {
                merge_scd1_delta_table_with_metrics(df, target, resolver, entity)?
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
