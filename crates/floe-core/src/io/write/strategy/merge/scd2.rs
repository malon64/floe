use deltalake::protocol::SaveMode;
use deltalake::table::builder::DeltaTableBuilder;
use polars::prelude::{DataFrame, DataType, NamedFrom, Series, TimeUnit};
use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::checks::normalize;
use crate::errors::RunError;
use crate::io::format::AcceptedMergeMetrics;
use crate::io::storage::{object_store, Target};
use crate::{config, FloeResult};

use super::{shared, MergeBackend, MergeExecutionContext};

const SCD2_IS_CURRENT_COLUMN: &str = "__floe_is_current";
const SCD2_VALID_FROM_COLUMN: &str = "__floe_valid_from";
const SCD2_VALID_TO_COLUMN: &str = "__floe_valid_to";

struct DeltaMergeBackend;

pub(crate) fn execute_merge_scd2_with_runtime(
    runtime: &tokio::runtime::Runtime,
    source_df: &mut DataFrame,
    target: &Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    partition_by: Option<Vec<String>>,
    target_file_size_bytes: Option<usize>,
) -> FloeResult<(i64, AcceptedMergeMetrics, shared::DeltaMergePerfBreakdown)> {
    let ctx = MergeExecutionContext {
        runtime,
        target,
        resolver,
        entity,
        partition_by,
        target_file_size_bytes,
    };
    DeltaMergeBackend.execute_scd2(source_df, &ctx)
}

impl MergeBackend for DeltaMergeBackend {
    fn execute_scd1(
        &self,
        _source_df: &mut DataFrame,
        _ctx: &MergeExecutionContext<'_>,
    ) -> FloeResult<(i64, AcceptedMergeMetrics, shared::DeltaMergePerfBreakdown)> {
        Err(Box::new(RunError(
            "write_mode=merge_scd1 is not implemented for scd2 backend".to_string(),
        )))
    }

    fn execute_scd2(
        &self,
        source_df: &mut DataFrame,
        ctx: &MergeExecutionContext<'_>,
    ) -> FloeResult<(i64, AcceptedMergeMetrics, shared::DeltaMergePerfBreakdown)> {
        let merge_start = Instant::now();
        let mut perf = shared::DeltaMergePerfBreakdown::default();
        let merge_key = shared::resolve_merge_key(ctx.entity)?;
        let merge_key_set = merge_key.iter().map(String::as_str).collect::<HashSet<_>>();
        let compare_columns = source_df
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .filter(|name| !merge_key_set.contains(name.as_str()))
            .collect::<Vec<_>>();
        let merge_key_predicate = shared::merge_predicate_sql(&merge_key);

        let store = object_store::delta_store_config(ctx.target, ctx.resolver, ctx.entity)?;
        let table_url = store.table_url;
        let storage_options = store.storage_options;
        let builder = DeltaTableBuilder::from_url(table_url.clone())
            .map_err(|err| Box::new(RunError(format!("delta builder failed: {err}"))))?
            .with_storage_options(storage_options.clone());
        let loaded_table = ctx
            .runtime
            .block_on(async move { builder.load().await })
            .map(Some)
            .or_else(|err| match err {
                deltalake::DeltaTableError::NotATable(_) => Ok(None),
                other => Err(Box::new(RunError(format!("delta load failed: {other}")))),
            })?;

        if loaded_table.is_none() {
            let mut bootstrap_df = source_df.clone();
            append_scd2_system_columns(&mut bootstrap_df)?;
            let bootstrap_schema_columns = build_scd2_bootstrap_schema_columns(ctx.entity)?;
            let conversion_start = Instant::now();
            let batch =
                crate::io::write::delta::record_batch::dataframe_to_record_batch_with_schema(
                    &bootstrap_df,
                    &bootstrap_schema_columns,
                )?;
            perf.conversion_ms = conversion_start.elapsed().as_millis() as u64;
            let commit_start = Instant::now();
            let version = shared::write_delta_batch_version(
                ctx.runtime,
                batch,
                ctx.target,
                ctx.resolver,
                ctx.entity,
                SaveMode::Append,
                ctx.partition_by.clone(),
                ctx.target_file_size_bytes,
            )?;
            perf.commit_ms = commit_start.elapsed().as_millis() as u64;
            return Ok((
                version,
                AcceptedMergeMetrics {
                    merge_key,
                    inserted_count: source_df.height() as u64,
                    updated_count: 0,
                    closed_count: Some(0),
                    unchanged_count: Some(0),
                    target_rows_before: 0,
                    target_rows_after: source_df.height() as u64,
                    merge_elapsed_ms: merge_start.elapsed().as_millis() as u64,
                },
                perf,
            ));
        }

        let table = loaded_table.expect("checked is_some");
        let target_schema_columns = shared::delta_schema_columns(&table)?;
        shared::validate_scd2_schema_compatibility(
            &target_schema_columns,
            source_df,
            &[
                SCD2_IS_CURRENT_COLUMN,
                SCD2_VALID_FROM_COLUMN,
                SCD2_VALID_TO_COLUMN,
            ],
            &ctx.entity.name,
        )?;

        let conversion_start = Instant::now();
        let source_batch = shared::source_record_batch(source_df, ctx.entity)?;
        perf.conversion_ms = conversion_start.elapsed().as_millis() as u64;
        let source_df_build_start = Instant::now();
        let source_for_close =
            shared::source_as_datafusion_df_from_batch(source_batch.clone(), &ctx.entity.name)?;
        let source_for_insert =
            shared::source_as_datafusion_df_from_batch(source_batch, &ctx.entity.name)?;
        perf.source_df_build_ms = source_df_build_start.elapsed().as_millis() as u64;
        let update_predicate = scd2_changed_predicate(&compare_columns);
        let merge_key_predicate_for_close = merge_key_predicate.clone();
        let merge_exec_start = Instant::now();
        let close_result = ctx.runtime.block_on(async move {
            let mut merge = table
                .merge(source_for_close, merge_key_predicate_for_close)
                .with_source_alias("source")
                .with_target_alias("target");
            merge = merge.when_matched_update(|update| {
                update
                    .predicate(format!(
                        "{} = true AND ({})",
                        shared::qualified_column("target", SCD2_IS_CURRENT_COLUMN),
                        update_predicate
                    ))
                    .update(
                        shared::qualified_column("target", SCD2_IS_CURRENT_COLUMN),
                        "false",
                    )
                    .update(
                        shared::qualified_column("target", SCD2_VALID_TO_COLUMN),
                        "current_timestamp()",
                    )
            })?;
            merge.await
        });
        let (table_after_close, close_metrics) =
            close_result.map_err(|err| Box::new(RunError(format!("delta merge failed: {err}"))))?;

        let active_match_predicate = format!(
            "{} AND {} = true",
            merge_key_predicate,
            shared::qualified_column("target", SCD2_IS_CURRENT_COLUMN)
        );
        let source_columns = source_df
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        let insert_result = ctx.runtime.block_on(async move {
            let mut merge = table_after_close
                .merge(source_for_insert, active_match_predicate)
                .with_source_alias("source")
                .with_target_alias("target");
            merge = merge.when_not_matched_insert(|insert| {
                let insert = source_columns.iter().fold(insert, |builder, column| {
                    builder.set(
                        shared::qualified_column("target", column),
                        shared::qualified_column("source", column),
                    )
                });
                insert
                    .set(
                        shared::qualified_column("target", SCD2_IS_CURRENT_COLUMN),
                        "true",
                    )
                    .set(
                        shared::qualified_column("target", SCD2_VALID_FROM_COLUMN),
                        "current_timestamp()",
                    )
                    .set(
                        shared::qualified_column("target", SCD2_VALID_TO_COLUMN),
                        "NULL",
                    )
            })?;
            merge.await
        });
        let (table, insert_metrics) = insert_result
            .map_err(|err| Box::new(RunError(format!("delta merge_scd2 failed: {err}"))))?;
        perf.merge_exec_ms = merge_exec_start.elapsed().as_millis() as u64;
        let version = table.version().ok_or_else(|| {
            Box::new(RunError(
                "delta table version missing after merge".to_string(),
            ))
        })?;
        let source_rows = source_df.height() as u64;
        let closed_count = close_metrics.num_target_rows_updated as u64;
        let inserted_count = insert_metrics.num_target_rows_inserted as u64;
        let unchanged_count = source_rows.saturating_sub(inserted_count);

        let target_rows_before = (close_metrics.num_target_rows_copied
            + close_metrics.num_target_rows_updated
            + close_metrics.num_target_rows_deleted) as u64;
        let target_rows_after = target_rows_before.saturating_add(inserted_count);
        Ok((
            version,
            AcceptedMergeMetrics {
                merge_key,
                inserted_count,
                updated_count: closed_count,
                closed_count: Some(closed_count),
                unchanged_count: Some(unchanged_count),
                target_rows_before,
                target_rows_after,
                merge_elapsed_ms: merge_start.elapsed().as_millis() as u64,
            },
            perf,
        ))
    }
}

fn append_scd2_system_columns(df: &mut DataFrame) -> FloeResult<()> {
    let row_count = df.height();
    let now_micros = now_timestamp_micros();
    let valid_from = Series::new(
        SCD2_VALID_FROM_COLUMN.into(),
        vec![Some(now_micros); row_count],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .map_err(|err| {
        Box::new(RunError(format!(
            "delta merge_scd2 failed to build {} column: {err}",
            SCD2_VALID_FROM_COLUMN
        )))
    })?;
    let valid_to = Series::new(
        SCD2_VALID_TO_COLUMN.into(),
        vec![Option::<i64>::None; row_count],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .map_err(|err| {
        Box::new(RunError(format!(
            "delta merge_scd2 failed to build {} column: {err}",
            SCD2_VALID_TO_COLUMN
        )))
    })?;
    let is_current = Series::new(SCD2_IS_CURRENT_COLUMN.into(), vec![Some(true); row_count]);
    df.with_column(valid_from).map_err(|err| {
        Box::new(RunError(format!(
            "delta merge_scd2 failed to append {} column: {err}",
            SCD2_VALID_FROM_COLUMN
        )))
    })?;
    df.with_column(valid_to).map_err(|err| {
        Box::new(RunError(format!(
            "delta merge_scd2 failed to append {} column: {err}",
            SCD2_VALID_TO_COLUMN
        )))
    })?;
    df.with_column(is_current).map_err(|err| {
        Box::new(RunError(format!(
            "delta merge_scd2 failed to append {} column: {err}",
            SCD2_IS_CURRENT_COLUMN
        )))
    })?;
    Ok(())
}

fn build_scd2_bootstrap_schema_columns(
    entity: &config::EntityConfig,
) -> FloeResult<Vec<config::ColumnConfig>> {
    let mut columns = normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize::resolve_normalize_strategy(entity)?.as_deref(),
    );
    columns.push(config::ColumnConfig {
        name: SCD2_IS_CURRENT_COLUMN.to_string(),
        source: None,
        column_type: "boolean".to_string(),
        nullable: Some(false),
        unique: None,
        width: None,
        trim: None,
    });
    columns.push(config::ColumnConfig {
        name: SCD2_VALID_FROM_COLUMN.to_string(),
        source: None,
        column_type: "datetime".to_string(),
        nullable: Some(false),
        unique: None,
        width: None,
        trim: None,
    });
    columns.push(config::ColumnConfig {
        name: SCD2_VALID_TO_COLUMN.to_string(),
        source: None,
        column_type: "datetime".to_string(),
        nullable: Some(true),
        unique: None,
        width: None,
        trim: None,
    });
    Ok(columns)
}

fn now_timestamp_micros() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    (duration.as_secs() as i64)
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(duration.subsec_micros()))
}

fn scd2_changed_predicate(compare_columns: &[String]) -> String {
    if compare_columns.is_empty() {
        return "false".to_string();
    }
    compare_columns
        .iter()
        .map(|column| {
            let target_col = shared::qualified_column("target", column);
            let source_col = shared::qualified_column("source", column);
            format!(
                "(({target_col} <> {source_col}) OR ({target_col} IS NULL AND {source_col} IS NOT NULL) OR ({target_col} IS NOT NULL AND {source_col} IS NULL))"
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}
