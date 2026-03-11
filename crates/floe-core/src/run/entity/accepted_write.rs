use std::path::Path;
use std::time::Instant;

use polars::prelude::DataFrame;

use crate::errors::RunError;
use crate::{config, io, report, FloeResult};

use super::super::output::{write_accepted_output, AcceptedOutputContext};
use super::EntityPhaseTimings;
use crate::run::events::{event_time_ms, RunEvent, RunObserver};
use crate::run::RunContext;
use io::storage::Target;

#[derive(Debug, Default)]
pub(super) struct AcceptedWriteReportState {
    pub(super) parts_written: u64,
    pub(super) files_written: u64,
    pub(super) part_files: Vec<String>,
    pub(super) table_version: Option<i64>,
    pub(super) snapshot_id: Option<i64>,
    pub(super) table_root_uri: Option<String>,
    pub(super) iceberg_catalog_name: Option<String>,
    pub(super) iceberg_database: Option<String>,
    pub(super) iceberg_namespace: Option<String>,
    pub(super) iceberg_table: Option<String>,
    pub(super) total_bytes_written: Option<u64>,
    pub(super) avg_file_size_mb: Option<f64>,
    pub(super) small_files_count: Option<u64>,
    pub(super) merge_key: Vec<String>,
    pub(super) inserted_count: Option<u64>,
    pub(super) updated_count: Option<u64>,
    pub(super) closed_count: Option<u64>,
    pub(super) unchanged_count: Option<u64>,
    pub(super) target_rows_before: Option<u64>,
    pub(super) target_rows_after: Option<u64>,
    pub(super) merge_elapsed_ms: Option<u64>,
    pub(super) schema_evolution: io::format::AcceptedSchemaEvolution,
    pub(super) write_perf: Option<io::format::AcceptedWritePerfBreakdown>,
}

impl AcceptedWriteReportState {
    pub(super) fn for_entity(entity: &config::EntityConfig, write_mode: config::WriteMode) -> Self {
        let schema_evolution = entity.schema.resolved_schema_evolution();
        Self {
            schema_evolution: io::format::AcceptedSchemaEvolution {
                enabled: entity.sink.accepted.format == "delta"
                    && schema_evolution.mode == config::SchemaEvolutionMode::AddColumns
                    && matches!(
                        write_mode,
                        config::WriteMode::Append | config::WriteMode::Overwrite
                    ),
                mode: schema_evolution.mode.as_str().to_string(),
                applied: false,
                added_columns: Vec::new(),
                incompatible_changes_detected: false,
            },
            ..Self::default()
        }
    }

    pub(super) fn from_write_output(output: io::format::AcceptedWriteOutput) -> Self {
        Self {
            parts_written: output.parts_written,
            files_written: output.files_written,
            part_files: output.part_files,
            table_version: output.table_version,
            snapshot_id: output.snapshot_id,
            table_root_uri: output.table_root_uri,
            iceberg_catalog_name: output.iceberg_catalog_name,
            iceberg_database: output.iceberg_database,
            iceberg_namespace: output.iceberg_namespace,
            iceberg_table: output.iceberg_table,
            total_bytes_written: output.metrics.total_bytes_written,
            avg_file_size_mb: output.metrics.avg_file_size_mb,
            small_files_count: output.metrics.small_files_count,
            merge_key: output
                .merge
                .as_ref()
                .map(|merge| merge.merge_key.clone())
                .unwrap_or_default(),
            inserted_count: output.merge.as_ref().map(|merge| merge.inserted_count),
            updated_count: output.merge.as_ref().map(|merge| merge.updated_count),
            closed_count: output.merge.as_ref().and_then(|merge| merge.closed_count),
            unchanged_count: output
                .merge
                .as_ref()
                .and_then(|merge| merge.unchanged_count),
            target_rows_before: output.merge.as_ref().map(|merge| merge.target_rows_before),
            target_rows_after: output.merge.as_ref().map(|merge| merge.target_rows_after),
            merge_elapsed_ms: output.merge.as_ref().map(|merge| merge.merge_elapsed_ms),
            schema_evolution: output.schema_evolution,
            write_perf: output.perf,
        }
    }

    pub(super) fn apply_accepted_path_to_file_reports(
        &self,
        file_reports: &mut [report::FileReport],
        accepted_target_uri: &str,
    ) {
        if self.parts_written == 0 {
            return;
        }
        let accepted_path = accepted_target_uri.to_string();
        for file_report in file_reports {
            file_report.output.accepted_path = Some(accepted_path.clone());
        }
    }
}

pub(super) struct AcceptedWritePhaseContext<'a> {
    pub(super) run_context: &'a RunContext,
    pub(super) observer: &'a dyn RunObserver,
    pub(super) runtime: &'a mut dyn crate::runtime::Runtime,
    pub(super) entity: &'a config::EntityConfig,
    pub(super) accepted_target: &'a Target,
    pub(super) temp_dir: Option<&'a Path>,
    pub(super) write_mode: config::WriteMode,
    pub(super) perf_enabled: bool,
    pub(super) phase_timings: &'a mut EntityPhaseTimings,
    pub(super) accepted_accum: Vec<DataFrame>,
}

pub(super) fn run_accepted_write_phase(
    context: AcceptedWritePhaseContext<'_>,
) -> FloeResult<AcceptedWriteReportState> {
    let AcceptedWritePhaseContext {
        run_context,
        observer,
        runtime,
        entity,
        accepted_target,
        temp_dir,
        write_mode,
        perf_enabled,
        phase_timings,
        accepted_accum,
    } = context;

    let mut accepted_write_report = AcceptedWriteReportState::for_entity(entity, write_mode);
    if accepted_accum.is_empty() {
        return Ok(accepted_write_report);
    }

    let concat_start = perf_enabled.then(Instant::now);
    let mut accepted_df = concat_accepted_frames(accepted_accum)?;
    if let Some(start) = concat_start {
        phase_timings.concat_accepted_ms += start.elapsed().as_millis() as u64;
    }

    let output_stem = io::storage::paths::build_part_stem(0);
    let accepted_adapter = runtime.accepted_sink_adapter(entity.sink.accepted.format.as_str())?;
    let write_accepted_start = perf_enabled.then(Instant::now);
    let accepted_output = write_accepted_output(AcceptedOutputContext {
        adapter: accepted_adapter,
        target: accepted_target,
        df: &mut accepted_df,
        output_stem: &output_stem,
        temp_dir,
        cloud: runtime.storage(),
        resolver: &run_context.storage_resolver,
        catalogs: &run_context.catalog_resolver,
        entity,
        mode: write_mode,
    })?;
    if let Some(start) = write_accepted_start {
        let elapsed_ms = start.elapsed().as_millis() as u64;
        phase_timings.write_accepted_ms += elapsed_ms;
        match entity.sink.accepted.format.as_str() {
            "delta" => phase_timings.write_delta_ms += elapsed_ms,
            "iceberg" => phase_timings.write_iceberg_ms += elapsed_ms,
            _ => {}
        }
    }

    accepted_write_report = AcceptedWriteReportState::from_write_output(accepted_output);
    if accepted_write_report.schema_evolution.applied {
        observer.on_event(RunEvent::SchemaEvolutionApplied {
            run_id: run_context.run_id.clone(),
            entity: entity.name.clone(),
            mode: accepted_write_report.schema_evolution.mode.clone(),
            added_columns: accepted_write_report.schema_evolution.added_columns.clone(),
            ts_ms: event_time_ms(),
        });
    }
    Ok(accepted_write_report)
}

fn concat_accepted_frames(mut frames: Vec<DataFrame>) -> FloeResult<DataFrame> {
    if frames.is_empty() {
        return Err(Box::new(RunError("missing accepted dataframe".to_string())));
    }
    // Pairwise concatenation bounds repeated growth of a single frame compared to
    // strictly left-associative stacking while preserving row order.
    while frames.len() > 1 {
        let mut next = Vec::with_capacity(frames.len().div_ceil(2));
        let mut iter = frames.into_iter();
        while let Some(mut left) = iter.next() {
            if let Some(right) = iter.next() {
                left.vstack_mut(&right).map_err(|err| {
                    Box::new(RunError(format!("failed to concat accepted rows: {err}")))
                })?;
            }
            next.push(left);
        }
        frames = next;
    }
    frames
        .pop()
        .ok_or_else(|| Box::new(RunError("missing accepted dataframe".to_string())).into())
}
