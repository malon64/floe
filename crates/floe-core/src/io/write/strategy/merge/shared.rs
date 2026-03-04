use std::time::Instant;

use arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use deltalake::table::builder::DeltaTableBuilder;
use deltalake::{datafusion::prelude::SessionContext, DeltaTable};
use polars::prelude::DataFrame;
use std::collections::HashSet;

use crate::errors::RunError;
use crate::io::format::AcceptedMergeMetrics;
use crate::io::storage::{object_store, Target};
use crate::{config, FloeResult};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DeltaStandardWritePerf {
    pub(crate) conversion_ms: u64,
    pub(crate) commit_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeltaVersionWriteOutcome {
    pub(crate) version: i64,
    pub(crate) perf: DeltaStandardWritePerf,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DeltaMergePerfBreakdown {
    pub(crate) conversion_ms: u64,
    pub(crate) source_df_build_ms: u64,
    pub(crate) merge_exec_ms: u64,
    pub(crate) commit_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Scd2SystemColumns {
    pub(crate) is_current: String,
    pub(crate) valid_from: String,
    pub(crate) valid_to: String,
}

pub(crate) fn write_standard_delta_version_with_perf(
    runtime: &tokio::runtime::Runtime,
    df: &mut DataFrame,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    partition_by: Option<Vec<String>>,
    target_file_size_bytes: Option<usize>,
) -> FloeResult<DeltaVersionWriteOutcome> {
    let conversion_start = Instant::now();
    let batch = crate::io::write::delta::record_batch::dataframe_to_record_batch(df, entity)?;
    let conversion_ms = conversion_start.elapsed().as_millis() as u64;
    let commit_start = Instant::now();
    let version = write_delta_batch_version(
        runtime,
        batch,
        target,
        resolver,
        entity,
        save_mode_for_write_mode(mode),
        partition_by,
        target_file_size_bytes,
    )?;
    let commit_ms = commit_start.elapsed().as_millis() as u64;
    Ok(DeltaVersionWriteOutcome {
        version,
        perf: DeltaStandardWritePerf {
            conversion_ms,
            commit_ms,
        },
    })
}

pub(crate) fn write_delta_batch_version(
    runtime: &tokio::runtime::Runtime,
    batch: deltalake::arrow::record_batch::RecordBatch,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    save_mode: SaveMode,
    partition_by: Option<Vec<String>>,
    target_file_size_bytes: Option<usize>,
) -> FloeResult<i64> {
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let table_url = store.table_url;
    let storage_options = store.storage_options;
    let builder = DeltaTableBuilder::from_url(table_url.clone())
        .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
        .with_storage_options(storage_options.clone());
    Ok(runtime
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
            let mut write = table.write(vec![batch]).with_save_mode(save_mode);
            if let Some(partition_by) = partition_by {
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
        .map_err(|err| Box::new(RunError(format!("delta write failed: {err}"))))?)
}

fn save_mode_for_write_mode(mode: config::WriteMode) -> SaveMode {
    crate::io::write::delta::record_batch::save_mode_for_write_mode(mode)
}

pub(crate) fn resolve_merge_key(entity: &config::EntityConfig) -> FloeResult<Vec<String>> {
    let primary_key = entity.schema.primary_key.as_ref().ok_or_else(|| {
        Box::new(RunError(format!(
            "entity.name={} merge write modes require schema.primary_key",
            entity.name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    if primary_key.is_empty() {
        return Err(Box::new(RunError(format!(
            "entity.name={} merge write modes require non-empty schema.primary_key",
            entity.name
        ))));
    }
    Ok(primary_key.clone())
}

pub(crate) fn resolve_merge_ignore_columns(entity: &config::EntityConfig) -> HashSet<String> {
    entity
        .sink
        .accepted
        .merge
        .as_ref()
        .and_then(|merge| merge.ignore_columns.as_ref())
        .map(|columns| {
            columns
                .iter()
                .map(|column| column.trim())
                .filter(|column| !column.is_empty())
                .map(ToOwned::to_owned)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

pub(crate) fn resolve_merge_compare_columns(entity: &config::EntityConfig) -> Option<Vec<String>> {
    entity
        .sink
        .accepted
        .merge
        .as_ref()
        .and_then(|merge| merge.compare_columns.as_ref())
        .map(|columns| {
            let mut seen = HashSet::new();
            columns
                .iter()
                .map(|column| column.trim())
                .filter(|column| !column.is_empty())
                .filter_map(|column| {
                    if seen.insert(column.to_string()) {
                        Some(column.to_string())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
}

pub(crate) fn resolve_scd2_system_columns(entity: &config::EntityConfig) -> Scd2SystemColumns {
    let scd2 = entity
        .sink
        .accepted
        .merge
        .as_ref()
        .and_then(|merge| merge.scd2.as_ref());
    let is_current = scd2
        .and_then(|value| value.current_flag_column.as_deref())
        .unwrap_or(config::DEFAULT_SCD2_CURRENT_FLAG_COLUMN)
        .trim()
        .to_string();
    let valid_from = scd2
        .and_then(|value| value.valid_from_column.as_deref())
        .unwrap_or(config::DEFAULT_SCD2_VALID_FROM_COLUMN)
        .trim()
        .to_string();
    let valid_to = scd2
        .and_then(|value| value.valid_to_column.as_deref())
        .unwrap_or(config::DEFAULT_SCD2_VALID_TO_COLUMN)
        .trim()
        .to_string();
    Scd2SystemColumns {
        is_current,
        valid_from,
        valid_to,
    }
}

pub(crate) fn delta_schema_columns(table: &DeltaTable) -> FloeResult<Vec<String>> {
    let columns = table
        .snapshot()
        .map_err(|err| Box::new(RunError(format!("delta schema load failed: {err}"))))?
        .schema()
        .fields()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    Ok(columns)
}

pub(crate) fn validate_merge_schema_compatibility(
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

pub(crate) fn validate_scd2_schema_compatibility(
    target_schema_columns: &[String],
    source_df: &DataFrame,
    system_columns: &[&str],
    entity_name: &str,
) -> FloeResult<()> {
    let source_columns = source_df
        .get_column_names()
        .iter()
        .map(|name| name.as_str())
        .collect::<HashSet<_>>();
    let system_columns_set = system_columns.iter().copied().collect::<HashSet<_>>();
    let target_columns = target_schema_columns
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for source_column in &source_columns {
        if !target_columns.contains(source_column) {
            return Err(Box::new(RunError(format!(
                "entity.name={} delta merge_scd2 failed: target schema missing source column {}",
                entity_name, source_column
            ))));
        }
    }
    for system_column in system_columns {
        if !target_columns.contains(system_column) {
            return Err(Box::new(RunError(format!(
                "entity.name={} delta merge_scd2 failed: target schema missing system column {}",
                entity_name, system_column
            ))));
        }
    }
    for target_column in target_schema_columns {
        if system_columns_set.contains(target_column.as_str()) {
            continue;
        }
        if !source_columns.contains(target_column.as_str()) {
            return Err(Box::new(RunError(format!(
                "entity.name={} delta merge_scd2 failed: source schema missing target column {}",
                entity_name, target_column
            ))));
        }
    }
    Ok(())
}

pub(crate) fn source_record_batch(
    source_df: &DataFrame,
    entity: &config::EntityConfig,
) -> FloeResult<RecordBatch> {
    crate::io::write::delta::record_batch::dataframe_to_record_batch(source_df, entity)
}

pub(crate) fn source_as_datafusion_df_from_batch(
    batch: RecordBatch,
    entity_name: &str,
) -> FloeResult<deltalake::datafusion::prelude::DataFrame> {
    SessionContext::new().read_batch(batch).map_err(|err| {
        Box::new(RunError(format!(
            "entity.name={} delta merge failed to build source dataframe: {err}",
            entity_name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

pub(crate) fn merge_predicate_sql(merge_key: &[String]) -> String {
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

pub(crate) fn qualified_column(alias: &str, column: &str) -> String {
    format!("{alias}.`{}`", column.replace('`', "``"))
}

pub(crate) fn accepted_merge_metrics_from_delta(
    merge_key: Vec<String>,
    merge_metrics: &deltalake::operations::merge::MergeMetrics,
    merge_elapsed_ms: u64,
) -> AcceptedMergeMetrics {
    let target_rows_before = (merge_metrics.num_target_rows_copied
        + merge_metrics.num_target_rows_updated
        + merge_metrics.num_target_rows_deleted) as u64;
    let target_rows_after = merge_metrics.num_output_rows as u64;
    AcceptedMergeMetrics {
        merge_key,
        inserted_count: merge_metrics.num_target_rows_inserted as u64,
        updated_count: merge_metrics.num_target_rows_updated as u64,
        closed_count: None,
        unchanged_count: None,
        target_rows_before,
        target_rows_after,
        merge_elapsed_ms,
    }
}
