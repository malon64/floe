use deltalake::table::builder::DeltaTableBuilder;
use polars::prelude::DataFrame;
use std::collections::HashSet;
use std::time::Instant;

use crate::errors::RunError;
use crate::io::format::AcceptedMergeMetrics;
use crate::io::storage::{object_store, Target};
use crate::{config, FloeResult};

use super::{shared, MergeBackend, MergeExecutionContext};

struct DeltaMergeBackend;

pub(crate) fn execute_merge_scd1_with_runtime(
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
    DeltaMergeBackend.execute_scd1(source_df, &ctx)
}

impl MergeBackend for DeltaMergeBackend {
    fn execute_scd1(
        &self,
        source_df: &mut DataFrame,
        ctx: &MergeExecutionContext<'_>,
    ) -> FloeResult<(i64, AcceptedMergeMetrics, shared::DeltaMergePerfBreakdown)> {
        let merge_start = Instant::now();
        let mut perf = shared::DeltaMergePerfBreakdown::default();
        let merge_key = shared::resolve_merge_key(ctx.entity)?;
        // Merge-key uniqueness is enforced upstream by check::UniqueTracker for merge_scd1,
        // so the writer can stay focused on sink-level merge execution.
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
            let outcome = shared::write_standard_delta_version_with_perf(
                ctx.runtime,
                source_df,
                ctx.target,
                ctx.resolver,
                ctx.entity,
                config::WriteMode::Append,
                ctx.partition_by.clone(),
                ctx.target_file_size_bytes,
            )?;
            perf.conversion_ms = outcome.perf.conversion_ms;
            perf.commit_ms = outcome.perf.commit_ms;
            return Ok((
                outcome.version,
                AcceptedMergeMetrics {
                    merge_key,
                    inserted_count: source_df.height() as u64,
                    updated_count: 0,
                    closed_count: None,
                    unchanged_count: None,
                    target_rows_before: 0,
                    target_rows_after: source_df.height() as u64,
                    merge_elapsed_ms: merge_start.elapsed().as_millis() as u64,
                },
                perf,
            ));
        }

        let table = loaded_table.expect("checked is_some");
        let target_schema_columns = shared::delta_schema_columns(&table)?;
        shared::validate_merge_schema_compatibility(
            &target_schema_columns,
            source_df,
            &ctx.entity.name,
        )?;
        let conversion_start = Instant::now();
        let source_batch = shared::source_record_batch(source_df, ctx.entity)?;
        perf.conversion_ms = conversion_start.elapsed().as_millis() as u64;
        let source_df_build_start = Instant::now();
        let source = shared::source_as_datafusion_df_from_batch(source_batch, &ctx.entity.name)?;
        perf.source_df_build_ms = source_df_build_start.elapsed().as_millis() as u64;
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
        let predicate = shared::merge_predicate_sql(&merge_key);
        let merge_exec_start = Instant::now();
        let merge_result = ctx.runtime.block_on(async move {
            let mut merge = table
                .merge(source, predicate)
                .with_source_alias("source")
                .with_target_alias("target");
            if !update_columns.is_empty() {
                let update_cols = update_columns.clone();
                merge = merge.when_matched_update(|update| {
                    update_cols.iter().fold(update, |builder, column| {
                        builder.update(
                            shared::qualified_column("target", column),
                            shared::qualified_column("source", column),
                        )
                    })
                })?;
            }
            let insert_cols = source_columns.clone();
            merge = merge.when_not_matched_insert(|insert| {
                insert_cols.iter().fold(insert, |builder, column| {
                    builder.set(
                        shared::qualified_column("target", column),
                        shared::qualified_column("source", column),
                    )
                })
            })?;
            merge.await
        });
        perf.merge_exec_ms = merge_exec_start.elapsed().as_millis() as u64;
        let (table, merge_metrics) =
            merge_result.map_err(|err| Box::new(RunError(format!("delta merge failed: {err}"))))?;
        let version = table.version().ok_or_else(|| {
            Box::new(RunError(
                "delta table version missing after merge".to_string(),
            ))
        })?;
        let accepted_merge_metrics = shared::accepted_merge_metrics_from_delta(
            merge_key,
            &merge_metrics,
            merge_start.elapsed().as_millis() as u64,
        );
        Ok((version, accepted_merge_metrics, perf))
    }
}
