use std::time::Instant;

use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::operations::write::SchemaMode;
use deltalake::protocol::SaveMode;
use deltalake::table::builder::DeltaTableBuilder;
use deltalake::{datafusion::prelude::SessionContext, DeltaTable};
use polars::prelude::DataFrame;
use std::collections::{HashMap, HashSet};

use crate::errors::RunError;
use crate::io::format::AcceptedMergeMetrics;
use crate::io::storage::{object_store, Target};
use crate::{config, FloeResult};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DeltaStandardWritePerf {
    pub(crate) conversion_ms: u64,
    pub(crate) commit_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct DeltaVersionWriteOutcome {
    pub(crate) version: i64,
    pub(crate) perf: DeltaStandardWritePerf,
    pub(crate) schema_evolution: crate::io::format::AcceptedSchemaEvolution,
}

#[derive(Clone)]
pub(crate) struct PlannedDeltaSchemaEvolution {
    pub(crate) summary: crate::io::format::AcceptedSchemaEvolution,
    pub(crate) write_schema_mode: Option<SchemaMode>,
    pub(crate) merge_schema: bool,
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

pub(crate) fn default_schema_evolution_summary(
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> crate::io::format::AcceptedSchemaEvolution {
    let schema_evolution = entity.schema.resolved_schema_evolution();
    crate::io::format::AcceptedSchemaEvolution {
        enabled: entity.sink.accepted.format == "delta"
            && schema_evolution.mode == config::SchemaEvolutionMode::AddColumns
            && matches!(
                mode,
                config::WriteMode::Append
                    | config::WriteMode::Overwrite
                    | config::WriteMode::MergeScd1
                    | config::WriteMode::MergeScd2
            ),
        mode: schema_evolution.mode.as_str().to_string(),
        applied: false,
        added_columns: Vec::new(),
        incompatible_changes_detected: false,
    }
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
    let schema_evolution =
        plan_standard_delta_schema_evolution(runtime, &batch, target, resolver, entity, mode)?;
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
        schema_evolution.write_schema_mode,
    )?;
    let commit_ms = commit_start.elapsed().as_millis() as u64;
    Ok(DeltaVersionWriteOutcome {
        version,
        schema_evolution: schema_evolution.summary,
        perf: DeltaStandardWritePerf {
            conversion_ms,
            commit_ms,
        },
    })
}

fn plan_standard_delta_schema_evolution(
    runtime: &tokio::runtime::Runtime,
    batch: &RecordBatch,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
) -> FloeResult<PlannedDeltaSchemaEvolution> {
    plan_delta_schema_evolution(runtime, batch, target, resolver, entity, mode, &[])
}

pub(crate) fn plan_merge_delta_schema_evolution(
    runtime: &tokio::runtime::Runtime,
    batch: &RecordBatch,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    ignored_target_columns: &[&str],
) -> FloeResult<PlannedDeltaSchemaEvolution> {
    plan_delta_schema_evolution(
        runtime,
        batch,
        target,
        resolver,
        entity,
        mode,
        ignored_target_columns,
    )
}

fn plan_delta_schema_evolution(
    runtime: &tokio::runtime::Runtime,
    batch: &RecordBatch,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    mode: config::WriteMode,
    ignored_target_columns: &[&str],
) -> FloeResult<PlannedDeltaSchemaEvolution> {
    let mut summary = default_schema_evolution_summary(entity, mode);
    if !summary.enabled {
        return Ok(PlannedDeltaSchemaEvolution {
            summary,
            write_schema_mode: None,
            merge_schema: false,
        });
    }

    let maybe_table = load_delta_table(runtime, target, resolver, entity)?;
    let Some(table) = maybe_table else {
        return Ok(PlannedDeltaSchemaEvolution {
            summary,
            write_schema_mode: None,
            merge_schema: false,
        });
    };

    let snapshot = table
        .snapshot()
        .map_err(|err| Box::new(RunError(format!("delta schema load failed: {err}"))))?;
    let target_schema = table.schema();
    let target_fields = target_schema.fields();
    let source_schema = batch.schema();
    let source_fields = source_schema.fields();
    let ignored_target_columns = ignored_target_columns
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let added_columns = additive_columns(target_fields, source_fields);
    let incompatible_changes =
        incompatible_schema_changes(target_fields, source_fields, &ignored_target_columns);

    summary.added_columns = added_columns;
    summary.incompatible_changes_detected = !incompatible_changes.is_empty();

    if !incompatible_changes.is_empty() {
        return Err(Box::new(RunError(format!(
            "entity.name={} delta schema evolution failed: incompatible changes detected: {}",
            entity.name,
            incompatible_changes.join("; ")
        ))));
    }

    if matches!(
        mode,
        config::WriteMode::MergeScd1 | config::WriteMode::MergeScd2
    ) {
        let merge_key = resolve_merge_key(entity)?;
        let added_merge_key_columns = merge_key
            .iter()
            .filter(|column| summary.added_columns.iter().any(|added| added == *column))
            .cloned()
            .collect::<Vec<_>>();
        if !added_merge_key_columns.is_empty() {
            summary.incompatible_changes_detected = true;
            return Err(Box::new(RunError(format!(
                "entity.name={} delta schema evolution failed: merge key columns cannot be added: {}",
                entity.name,
                added_merge_key_columns.join(", ")
            ))));
        }
    }

    if summary.added_columns.is_empty() {
        return Ok(PlannedDeltaSchemaEvolution {
            summary,
            write_schema_mode: None,
            merge_schema: false,
        });
    }

    let partition_columns = snapshot.metadata().partition_columns();
    if !partition_columns.is_empty() {
        return Err(Box::new(RunError(format!(
            "entity.name={} delta schema evolution failed: adding columns is unsupported for partitioned delta tables",
            entity.name
        ))));
    }

    summary.applied = true;
    Ok(PlannedDeltaSchemaEvolution {
        write_schema_mode: matches!(
            mode,
            config::WriteMode::Append | config::WriteMode::Overwrite
        )
        .then_some(SchemaMode::Merge),
        merge_schema: matches!(
            mode,
            config::WriteMode::MergeScd1 | config::WriteMode::MergeScd2
        ),
        summary,
    })
}

fn additive_columns(target_fields: &[FieldRef], source_fields: &[FieldRef]) -> Vec<String> {
    let target_names = target_fields
        .iter()
        .map(|field| field.name().to_string())
        .collect::<HashSet<_>>();
    source_fields
        .iter()
        .filter(|field| !target_names.contains(field.name()))
        .map(|field| field.name().to_string())
        .collect()
}

fn incompatible_schema_changes(
    target_fields: &[FieldRef],
    source_fields: &[FieldRef],
    ignored_target_columns: &HashSet<&str>,
) -> Vec<String> {
    let source_by_name = source_fields
        .iter()
        .map(|field| (field.name(), field))
        .collect::<HashMap<_, _>>();
    let mut incompatible = Vec::new();
    for target_field in target_fields {
        if ignored_target_columns.contains(target_field.name().as_str()) {
            continue;
        }
        let Some(source_field) = source_by_name.get(target_field.name()) else {
            incompatible.push(format!("missing existing column {}", target_field.name()));
            continue;
        };
        if target_field.data_type() != source_field.data_type() {
            incompatible.push(format!(
                "column {} type changed from {:?} to {:?}",
                target_field.name(),
                target_field.data_type(),
                source_field.data_type()
            ));
        }
        if !target_field.is_nullable() && source_field.is_nullable() {
            incompatible.push(format!(
                "column {} nullability changed from non-nullable to nullable",
                target_field.name()
            ));
        }
    }
    incompatible
}

pub(crate) fn load_delta_table(
    runtime: &tokio::runtime::Runtime,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<Option<DeltaTable>> {
    let store = object_store::delta_store_config(target, resolver, entity)?;
    let table_url = store.table_url;
    let storage_options = store.storage_options;
    let builder = DeltaTableBuilder::from_url(table_url.clone())
        .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
        .with_storage_options(storage_options.clone());

    runtime
        .block_on(async move {
            match builder.load().await {
                Ok(table) => Ok(Some(table)),
                Err(deltalake::DeltaTableError::NotATable(_)) => Ok(None),
                Err(err) => Err(err),
            }
        })
        .map_err(|err| Box::new(RunError(format!("delta schema load failed: {err}"))).into())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_delta_batch_version(
    runtime: &tokio::runtime::Runtime,
    batch: deltalake::arrow::record_batch::RecordBatch,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    save_mode: SaveMode,
    partition_by: Option<Vec<String>>,
    target_file_size_bytes: Option<usize>,
    schema_mode: Option<SchemaMode>,
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
            if let Some(schema_mode) = schema_mode {
                write = write.with_schema_mode(schema_mode);
            }
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

pub(crate) fn resolve_merge_ignore_columns(
    entity: &config::EntityConfig,
) -> FloeResult<HashSet<String>> {
    let Some(columns) = entity
        .sink
        .accepted
        .merge
        .as_ref()
        .and_then(|merge| merge.ignore_columns.as_ref())
    else {
        return Ok(HashSet::new());
    };

    let schema_to_output = schema_to_output_column_name_map(entity)?;
    let resolved = columns
        .iter()
        .map(|column| column.trim())
        .filter(|column| !column.is_empty())
        .map(|column| {
            schema_to_output
                .get(column)
                .cloned()
                .unwrap_or_else(|| column.to_string())
        })
        .collect::<HashSet<_>>();
    Ok(resolved)
}

pub(crate) fn resolve_merge_compare_columns(
    entity: &config::EntityConfig,
) -> FloeResult<Option<Vec<String>>> {
    let Some(columns) = entity
        .sink
        .accepted
        .merge
        .as_ref()
        .and_then(|merge| merge.compare_columns.as_ref())
    else {
        return Ok(None);
    };

    let schema_to_output = schema_to_output_column_name_map(entity)?;
    let mut seen = HashSet::new();
    let resolved = columns
        .iter()
        .map(|column| column.trim())
        .filter(|column| !column.is_empty())
        .map(|column| {
            schema_to_output
                .get(column)
                .cloned()
                .unwrap_or_else(|| column.to_string())
        })
        .filter(|column| seen.insert(column.clone()))
        .collect::<Vec<_>>();
    Ok(Some(resolved))
}

fn schema_to_output_column_name_map(
    entity: &config::EntityConfig,
) -> FloeResult<HashMap<String, String>> {
    let normalize_strategy = crate::checks::normalize::resolve_normalize_strategy(entity)?;
    let output_columns = crate::checks::normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize_strategy.as_deref(),
    );
    let mut mapping = HashMap::with_capacity(entity.schema.columns.len());
    for (schema_column, output_column) in entity.schema.columns.iter().zip(output_columns.iter()) {
        mapping.insert(
            schema_column.name.trim().to_string(),
            output_column.name.clone(),
        );
    }
    Ok(mapping)
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
    allow_target_additive_evolution: bool,
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
            if allow_target_additive_evolution {
                continue;
            }
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
    allow_target_additive_evolution: bool,
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
            if allow_target_additive_evolution {
                continue;
            }
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
