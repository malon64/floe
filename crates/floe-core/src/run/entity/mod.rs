use crate::{check, io, report, ConfigError, FloeResult};
use serde_json::json;
use std::collections::HashSet;
use std::time::Instant;

use super::file::required_columns;
use super::{EntityOutcome, RunContext, MAX_RESOLVED_INPUTS};
use crate::checks::normalize::{
    output_column_mapping, resolve_normalize_strategy, resolve_source_columns,
    source_column_mapping,
};
use io::storage::Target;

mod accepted_write;
mod precheck;
mod process;
mod resolve;
mod unique_existing;
mod validate_split;
pub(crate) use resolve::{resolve_entity_targets, ResolvedEntityTargets};

use crate::report::entity::{build_run_report, RunReportContext};
use crate::run::events::RunObserver;
use accepted_write::{run_accepted_write_phase, AcceptedWritePhaseContext};
use precheck::{run_precheck, PrecheckContext};
use process::sink_options_warning;
use validate_split::{run_validate_split_phase, ValidateSplitPhaseContext};

pub(super) struct EntityRunResult {
    pub outcome: EntityOutcome,
    pub abort_run: bool,
}

#[derive(Debug, Default)]
struct EntityPhaseTimings {
    precheck_ms: u64,
    read_parse_ms: u64,
    checks_validation_ms: u64,
    accept_reject_split_ms: u64,
    write_rejected_ms: u64,
    archive_ms: u64,
    concat_accepted_ms: u64,
    write_accepted_ms: u64,
    report_write_ms: u64,
}

impl EntityPhaseTimings {
    fn into_json(self) -> serde_json::Value {
        json!({
            "precheck": self.precheck_ms,
            "read_parse": self.read_parse_ms,
            "checks_validation": self.checks_validation_ms,
            "accept_reject_split": self.accept_reject_split_ms,
            "write_rejected": self.write_rejected_ms,
            "archive_input": self.archive_ms,
            "concat_accepted": self.concat_accepted_ms,
            "write_accepted": self.write_accepted_ms,
            "write_entity_report": self.report_write_ms,
        })
    }
}

pub(super) fn run_entity(
    context: &RunContext,
    runtime: &mut dyn crate::runtime::Runtime,
    observer: &dyn RunObserver,
    plan: super::EntityRunPlan<'_>,
) -> FloeResult<EntityRunResult> {
    let entity = plan.entity;
    let perf_enabled = crate::run::perf::phase_timing_enabled();
    let entity_start = perf_enabled.then(Instant::now);
    let mut phase_timings = EntityPhaseTimings::default();
    let input = &entity.source;
    let write_mode = entity.sink.resolved_write_mode();
    let input_adapter = runtime.input_adapter(input.format.as_str())?;
    let resolved_targets = plan.resolved_targets;
    let formatter_name = context
        .config
        .report
        .as_ref()
        .and_then(|report| report.formatter.as_deref())
        .unwrap_or("json");

    let normalize_strategy = resolve_normalize_strategy(entity)?;
    let normalized_columns =
        resolve_source_columns(&entity.schema.columns, normalize_strategy.as_deref(), false)?;
    let source_column_map =
        source_column_mapping(&entity.schema.columns, normalize_strategy.as_deref())?;
    let row_error_formatter = if source_column_map.is_empty() {
        check::row_error_formatter(formatter_name, None)?
    } else {
        check::row_error_formatter(formatter_name, Some(&source_column_map))?
    };
    let read_columns = io::format::resolve_read_columns(
        entity,
        &normalized_columns,
        normalize_strategy.as_deref(),
    )?;
    let output_column_map =
        output_column_mapping(&entity.schema.columns, normalize_strategy.as_deref())?;
    let mut required_cols = required_columns(&normalized_columns);
    append_primary_key_required_columns(&mut required_cols, entity, normalize_strategy.as_deref())?;
    let unique_constraints =
        resolve_unique_constraints(entity, normalize_strategy.as_deref(), write_mode)?;
    let accepted_target = resolved_targets.accepted.clone();
    let rejected_target = resolved_targets.rejected.clone();
    let temp_dir = plan.temp_dir;
    let io::storage::inputs::ResolvedInputs {
        files: input_files,
        listed: resolved_files,
        mode: resolved_mode,
    } = plan.resolved_inputs;

    let severity = match entity.policy.severity.as_str() {
        "warn" => report::Severity::Warn,
        "reject" => report::Severity::Reject,
        "abort" => report::Severity::Abort,
        severity => {
            return Err(Box::new(ConfigError(format!(
                "unsupported policy severity: {severity}"
            ))))
        }
    };
    let track_cast_errors = !matches!(input.cast_mode.as_deref(), Some("coerce"));

    let reported_files = resolved_files
        .iter()
        .take(MAX_RESOLVED_INPUTS)
        .cloned()
        .collect::<Vec<_>>();

    let mut file_reports = Vec::with_capacity(input_files.len());
    let mut totals = report::ResultsTotals {
        files_total: 0,
        rows_total: 0,
        accepted_total: 0,
        rejected_total: 0,
        warnings_total: 0,
        errors_total: 0,
    };
    let archive_target = entity
        .sink
        .archive
        .as_ref()
        .map(|archive| {
            let storage_name = archive
                .storage
                .as_deref()
                .or(entity.source.storage.as_deref());
            let resolved = context.storage_resolver.resolve_path(
                &entity.name,
                "sink.archive.storage",
                storage_name,
                &archive.path,
            )?;
            Target::from_resolved(&resolved)
        })
        .transpose()?;
    let mut file_timings_ms = Vec::with_capacity(input_files.len());
    let sink_options_warning = sink_options_warning(entity);
    // Phase A: per-file precheck (schema mismatch / early rejection).
    let precheck_start = perf_enabled.then(Instant::now);
    let precheck = run_precheck(
        PrecheckContext {
            context,
            entity,
            input_adapter,
            normalized_columns: &normalized_columns,
            resolved_targets: &resolved_targets,
            archive_target: archive_target.as_ref(),
            temp_dir: temp_dir.as_ref(),
            cloud: runtime.storage(),
            observer,
            file_reports: &mut file_reports,
            file_timings_ms: &mut file_timings_ms,
            totals: &mut totals,
        },
        input_files,
    )?;
    if let Some(start) = precheck_start {
        phase_timings.precheck_ms += start.elapsed().as_millis() as u64;
    }
    let mut abort_run = precheck.abort_run;
    let prechecked_inputs = precheck.prechecked;

    let mut accepted_accum = Vec::new();
    let temp_dir_path = temp_dir.as_ref().map(|dir| dir.path());
    let mut unique_tracker = check::UniqueTracker::with_constraints(unique_constraints);
    unique_existing::seed_unique_tracker_for_append(
        &mut unique_tracker,
        write_mode,
        entity.sink.accepted.format.as_str(),
        &accepted_target,
        temp_dir.as_ref().map(|dir| dir.path()),
        runtime.storage(),
        &context.storage_resolver,
        entity,
    )?;
    // Phase B: row-level validation + entity-level accumulation.
    let phase_b = run_validate_split_phase(ValidateSplitPhaseContext {
        run_context: context,
        runtime,
        observer,
        entity,
        input_adapter,
        prechecked_inputs,
        read_columns: &read_columns,
        normalize_strategy: normalize_strategy.as_deref(),
        normalized_columns: &normalized_columns,
        required_cols: &required_cols,
        source_column_map: &source_column_map,
        output_column_map: &output_column_map,
        row_error_formatter: row_error_formatter.as_ref(),
        severity,
        track_cast_errors,
        write_mode,
        rejected_target: rejected_target.as_ref(),
        archive_target: archive_target.as_ref(),
        temp_dir: temp_dir_path,
        sink_options_warning: sink_options_warning.as_deref(),
        perf_enabled,
        phase_timings: &mut phase_timings,
        file_reports: &mut file_reports,
        file_timings_ms: &mut file_timings_ms,
        totals: &mut totals,
        unique_tracker: &mut unique_tracker,
        accepted_accum: &mut accepted_accum,
        initial_abort_run: abort_run,
    })?;
    abort_run = phase_b.abort_run;
    let accepted_accum_rows = phase_b.accepted_accum_rows;
    let accepted_accum_frames = phase_b.accepted_accum_frames;
    let unique_constraints = unique_tracker.results();

    totals.files_total = file_reports.len() as u64;

    let accepted_target_uri = accepted_target.target_uri().to_string();
    let accepted_write_report = run_accepted_write_phase(AcceptedWritePhaseContext {
        run_context: context,
        runtime,
        entity,
        accepted_target: &accepted_target,
        temp_dir: temp_dir_path,
        write_mode,
        perf_enabled,
        phase_timings: &mut phase_timings,
        accepted_accum,
    })?;
    accepted_write_report
        .apply_accepted_path_to_file_reports(&mut file_reports, &accepted_target_uri);

    let perf_files_total = totals.files_total;
    let perf_rows_total = totals.rows_total;

    let run_report = build_run_report(RunReportContext {
        context,
        entity,
        input,
        resolved_targets: &resolved_targets,
        resolved_mode,
        resolved_files: &resolved_files,
        reported_files,
        totals,
        file_reports,
        severity,
        accepted_write_mode: write_mode,
        accepted_parts_written: accepted_write_report.parts_written,
        accepted_files_written: accepted_write_report.files_written,
        accepted_part_files: accepted_write_report.part_files,
        accepted_table_version: accepted_write_report.table_version,
        accepted_snapshot_id: accepted_write_report.snapshot_id,
        accepted_table_root_uri: accepted_write_report.table_root_uri,
        accepted_iceberg_catalog_name: accepted_write_report.iceberg_catalog_name,
        accepted_iceberg_database: accepted_write_report.iceberg_database,
        accepted_iceberg_namespace: accepted_write_report.iceberg_namespace,
        accepted_iceberg_table: accepted_write_report.iceberg_table,
        accepted_total_bytes_written: accepted_write_report.total_bytes_written,
        accepted_avg_file_size_mb: accepted_write_report.avg_file_size_mb,
        accepted_small_files_count: accepted_write_report.small_files_count,
        accepted_merge_key: accepted_write_report.merge_key,
        accepted_inserted_count: accepted_write_report.inserted_count,
        accepted_updated_count: accepted_write_report.updated_count,
        accepted_closed_count: accepted_write_report.closed_count,
        accepted_unchanged_count: accepted_write_report.unchanged_count,
        accepted_target_rows_before: accepted_write_report.target_rows_before,
        accepted_target_rows_after: accepted_write_report.target_rows_after,
        accepted_merge_elapsed_ms: accepted_write_report.merge_elapsed_ms,
        unique_constraints,
    });

    if let Some(report_target) = &context.report_target {
        let report_write_start = perf_enabled.then(Instant::now);
        crate::report::output::write_entity_report(
            report_target,
            &context.run_id,
            entity,
            &run_report,
            runtime.storage(),
            &context.storage_resolver,
        )?;
        if let Some(start) = report_write_start {
            phase_timings.report_write_ms += start.elapsed().as_millis() as u64;
        }
    }

    if let Some(start) = entity_start {
        crate::run::perf::emit_perf_log(
            observer,
            &context.run_id,
            Some(&entity.name),
            "perf_entity_phase_timings",
            json!({
                "entity": entity.name,
                "elapsed_ms": start.elapsed().as_millis() as u64,
                "files_total": perf_files_total,
                "rows_total": perf_rows_total,
                "accepted_rows_accumulated": accepted_accum_rows,
                "accepted_frames_accumulated": accepted_accum_frames,
                "phases_ms": phase_timings.into_json(),
            }),
        );
    }

    Ok(EntityRunResult {
        outcome: EntityOutcome {
            report: run_report,
            file_timings_ms,
        },
        abort_run,
    })
}

fn resolve_unique_constraints(
    entity: &crate::config::EntityConfig,
    normalize_strategy: Option<&str>,
    write_mode: crate::config::WriteMode,
) -> FloeResult<Vec<check::UniqueConstraint>> {
    let unique_keys = check::resolve_schema_unique_keys(&entity.schema);
    if unique_keys.is_empty() {
        return Ok(Vec::new());
    }
    let merge_primary_key = if matches!(
        write_mode,
        crate::config::WriteMode::MergeScd1 | crate::config::WriteMode::MergeScd2
    ) {
        entity.schema.primary_key.as_ref().map(|primary_key| {
            primary_key
                .iter()
                .map(|column| column.trim().to_string())
                .collect::<Vec<_>>()
        })
    } else {
        None
    };
    let mut constraints = Vec::with_capacity(unique_keys.len());
    for key in unique_keys {
        let mut runtime_columns = Vec::with_capacity(key.len());
        for name in &key {
            let column = entity
                .schema
                .columns
                .iter()
                .find(|column| column.name == *name)
                .ok_or_else(|| {
                    Box::new(ConfigError(format!(
                        "entity.name={} schema unique key references unknown column {}",
                        entity.name, name
                    )))
                })?;
            runtime_columns.push(runtime_column_name(column, normalize_strategy));
        }
        let enforce_reject = merge_primary_key
            .as_ref()
            .map(|primary_key| primary_key == &key)
            .unwrap_or(false);
        constraints.push(check::UniqueConstraint {
            runtime_columns,
            report_columns: key,
            enforce_reject,
        });
    }
    Ok(constraints)
}

fn append_primary_key_required_columns(
    required_cols: &mut Vec<String>,
    entity: &crate::config::EntityConfig,
    normalize_strategy: Option<&str>,
) -> FloeResult<()> {
    let Some(primary_key) = entity.schema.primary_key.as_ref() else {
        return Ok(());
    };
    if primary_key.is_empty() {
        return Ok(());
    }
    let mut seen = required_cols.iter().cloned().collect::<HashSet<_>>();
    for key_column in primary_key {
        let column = entity
            .schema
            .columns
            .iter()
            .find(|column| column.name == *key_column)
            .ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} schema.primary_key references unknown column {}",
                    entity.name, key_column
                )))
            })?;
        let runtime = runtime_column_name(column, normalize_strategy);
        if seen.insert(runtime.clone()) {
            required_cols.push(runtime);
        }
    }
    Ok(())
}

fn runtime_column_name(
    column: &crate::config::ColumnConfig,
    normalize_strategy: Option<&str>,
) -> String {
    let source_name = column.source_or_name();
    if let Some(strategy) = normalize_strategy {
        check::normalize::normalize_name(source_name, strategy)
    } else {
        source_name.to_string()
    }
}
