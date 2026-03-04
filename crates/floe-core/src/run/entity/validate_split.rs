use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use polars::prelude::DataFrame;

use crate::checks::normalize::rename_output_columns;
use crate::errors::RunError;
use crate::report::build::summarize_validation_sparse;
use crate::run::events::{event_time_ms, RunObserver};
use crate::run::RunContext;
use crate::{check, config, io, report, warnings, ConfigError, FloeResult};

use super::super::output::{
    append_rejection_columns, validate_rejected_target, write_error_report_output,
    write_rejected_output, write_rejected_raw_output, RejectedOutputContext,
};
use super::precheck::PrecheckedInput;
use super::process::append_sink_options_warning;
use super::EntityPhaseTimings;
use io::format::{InputAdapter, ReadInput};
use io::storage::Target;

#[derive(Debug, Default)]
pub(super) struct ValidateSplitPhaseOutcome {
    pub(super) abort_run: bool,
    pub(super) accepted_accum_rows: u64,
    pub(super) accepted_accum_frames: u64,
}

pub(super) struct ValidateSplitPhaseContext<'a> {
    pub(super) run_context: &'a RunContext,
    pub(super) runtime: &'a mut dyn crate::runtime::Runtime,
    pub(super) observer: &'a dyn RunObserver,
    pub(super) entity: &'a config::EntityConfig,
    pub(super) input_adapter: &'a dyn InputAdapter,
    pub(super) prechecked_inputs: Vec<PrecheckedInput>,
    pub(super) read_columns: &'a [config::ColumnConfig],
    pub(super) normalize_strategy: Option<&'a str>,
    pub(super) normalized_columns: &'a [config::ColumnConfig],
    pub(super) required_cols: &'a [String],
    pub(super) source_column_map: &'a HashMap<String, String>,
    pub(super) output_column_map: &'a HashMap<String, String>,
    pub(super) row_error_formatter: &'a dyn check::RowErrorFormatter,
    pub(super) severity: report::Severity,
    pub(super) track_cast_errors: bool,
    pub(super) write_mode: config::WriteMode,
    pub(super) rejected_target: Option<&'a Target>,
    pub(super) archive_target: Option<&'a Target>,
    pub(super) temp_dir: Option<&'a Path>,
    pub(super) sink_options_warning: Option<&'a str>,
    pub(super) perf_enabled: bool,
    pub(super) phase_timings: &'a mut EntityPhaseTimings,
    pub(super) file_reports: &'a mut Vec<report::FileReport>,
    pub(super) file_timings_ms: &'a mut Vec<Option<u64>>,
    pub(super) totals: &'a mut report::ResultsTotals,
    pub(super) unique_tracker: &'a mut check::UniqueTracker,
    pub(super) accepted_accum: &'a mut Vec<DataFrame>,
    pub(super) initial_abort_run: bool,
}

pub(super) fn run_validate_split_phase(
    context: ValidateSplitPhaseContext<'_>,
) -> FloeResult<ValidateSplitPhaseOutcome> {
    let ValidateSplitPhaseContext {
        run_context,
        runtime,
        observer,
        entity,
        input_adapter,
        prechecked_inputs,
        read_columns,
        normalize_strategy,
        normalized_columns,
        required_cols,
        source_column_map,
        output_column_map,
        row_error_formatter,
        severity,
        track_cast_errors,
        write_mode,
        rejected_target,
        archive_target,
        temp_dir,
        sink_options_warning,
        perf_enabled,
        phase_timings,
        file_reports,
        file_timings_ms,
        totals,
        unique_tracker,
        accepted_accum,
        initial_abort_run,
    } = context;

    let mut abort_run = initial_abort_run;
    let mut rejected_overwrite_used = false;
    let mut accepted_accum_rows = 0_u64;
    let mut accepted_accum_frames = 0_u64;
    let mut sink_options_warned = false;
    let collect_raw = true;

    for prechecked in prechecked_inputs {
        let PrecheckedInput {
            input_file,
            input_columns,
            mismatch,
            file_timer,
        } = prechecked;
        let read_parse_start = perf_enabled.then(Instant::now);
        let mut inputs = input_adapter.read_inputs_with_prechecked_columns(
            entity,
            std::slice::from_ref(&input_file),
            read_columns,
            normalize_strategy,
            collect_raw,
            Some(&input_columns),
        )?;
        if let Some(start) = read_parse_start {
            phase_timings.read_parse_ms += start.elapsed().as_millis() as u64;
        }
        let input = inputs.pop().ok_or_else(|| {
            Box::new(RunError(format!(
                "entity.name={} missing input data",
                entity.name
            )))
        })?;
        let (input_file, mut raw_df, mut df) = match input {
            ReadInput::Data {
                input_file,
                raw_df,
                typed_df,
            } => (input_file, raw_df, typed_df),
            ReadInput::FileError { input_file, error } => {
                crate::errors::emit(
                    &run_context.run_id,
                    Some(&entity.name),
                    Some(&input_file.source_uri),
                    Some(&error.rule),
                    &format!("entity.name={} {}", entity.name, error.message),
                );
                let status = if entity.policy.severity == "abort" {
                    report::FileStatus::Aborted
                } else {
                    report::FileStatus::Rejected
                };
                let mismatch_action = if status == report::FileStatus::Aborted {
                    report::MismatchAction::Aborted
                } else {
                    report::MismatchAction::RejectedFile
                };

                let rejected_path = rejected_target
                    .map(|target| {
                        write_rejected_raw_output(
                            target,
                            &input_file,
                            temp_dir,
                            runtime.storage(),
                            &run_context.storage_resolver,
                            entity,
                        )
                    })
                    .transpose()?;

                let mismatch_report = mismatch.report;
                let file_report = report::FileReport {
                    input_file: input_file.source_uri.clone(),
                    status,
                    row_count: 0,
                    accepted_count: 0,
                    rejected_count: 0,
                    mismatch: report::FileMismatch {
                        declared_columns_count: mismatch_report.declared_columns_count,
                        input_columns_count: mismatch_report.input_columns_count,
                        missing_columns: mismatch_report.missing_columns,
                        extra_columns: mismatch_report.extra_columns,
                        mismatch_action,
                        error: Some(report::MismatchIssue {
                            rule: error.rule,
                            message: format!("entity.name={} {}", entity.name, error.message),
                        }),
                        warning: mismatch_report.warning,
                    },
                    output: report::FileOutput {
                        accepted_path: None,
                        rejected_path,
                        errors_path: None,
                        archived_path: None,
                    },
                    validation: report::FileValidation {
                        errors: 1 + mismatch.errors,
                        warnings: mismatch.warnings,
                        rules: Vec::new(),
                    },
                };

                totals.errors_total += 1 + mismatch.errors;
                totals.warnings_total += mismatch.warnings;
                file_reports.push(file_report);
                file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));

                if status == report::FileStatus::Aborted {
                    abort_run = true;
                    break;
                }
                continue;
            }
        };

        let validation_start = perf_enabled.then(Instant::now);
        check::apply_mismatch_plan(&mismatch, normalized_columns, raw_df.as_mut(), &mut df)?;
        let mismatch_report = mismatch.report;
        let mismatch_errors = mismatch.errors;
        let mismatch_warnings = mismatch.warnings;

        let row_count = raw_df
            .as_ref()
            .map(|df| df.height())
            .unwrap_or_else(|| df.height()) as u64;
        let source_stem = input_file.source_stem.as_str();

        let raw_df = raw_df.ok_or_else(|| {
            Box::new(RunError(format!(
                "entity.name={} raw dataframe unavailable for rejection checks",
                entity.name
            )))
        })?;
        let raw_indices = check::column_index_map(&raw_df);
        let typed_indices = check::column_index_map(&df);

        let cast_counts = if track_cast_errors {
            check::cast_mismatch_counts(&raw_df, &df, normalized_columns)?
        } else {
            Vec::new()
        };
        let cast_total = cast_counts.iter().map(|(_, count, _)| *count).sum::<u64>();

        let mut error_lists = if entity.policy.severity == "abort" && cast_total > 0 {
            check::cast_mismatch_errors_sparse(
                &raw_df,
                &df,
                normalized_columns,
                &raw_indices,
                &typed_indices,
            )?
        } else {
            let not_null_counts = check::not_null_counts(&df, required_cols)?;
            let not_null_total = not_null_counts.iter().map(|(_, count)| *count).sum::<u64>();
            let quick_total = cast_total + not_null_total;

            if quick_total == 0 {
                check::SparseRowErrors::new(row_count as usize)
            } else {
                let mut errors = check::not_null_errors_sparse(&df, required_cols, &typed_indices)?;
                if track_cast_errors && cast_total > 0 {
                    let cast_errors = check::cast_mismatch_errors_sparse(
                        &raw_df,
                        &df,
                        normalized_columns,
                        &raw_indices,
                        &typed_indices,
                    )?;
                    errors.merge(cast_errors);
                }
                errors
            }
        };

        let mut forced_reject_rows = HashSet::new();
        if !(unique_tracker.is_empty() || (entity.policy.severity == "abort" && cast_total > 0)) {
            let unique_errors = unique_tracker.apply_sparse_with_forced_rejects(
                &df,
                normalized_columns,
                &mut forced_reject_rows,
            )?;
            error_lists.merge(unique_errors);
        }
        let forced_reject_count = forced_reject_rows.len() as u64;

        let accept_rows = error_lists.accept_rows();
        let errors_json = error_lists.build_errors_formatted(row_error_formatter);
        let row_error_count = error_lists.error_row_count();
        let violation_count = error_lists.violation_count();

        drop(raw_df);
        let accept_count = accept_rows.iter().filter(|accepted| **accepted).count() as u64;
        let reject_count = row_count.saturating_sub(accept_count);
        let has_errors = row_error_count > 0;
        let mut accepted_df_opt: Option<DataFrame> = None;
        let mut rejected_path = None;
        let mut errors_path = None;
        let mut archived_path = None;
        let mut rules = if has_errors {
            summarize_validation_sparse(
                &error_lists,
                normalized_columns,
                severity,
                Some(source_column_map),
            )
        } else {
            Vec::new()
        };
        let mut sink_options_warnings = 0;
        if let Some(message) = sink_options_warning {
            sink_options_warnings = 1;
            warnings::emit_once(
                &mut sink_options_warned,
                &run_context.run_id,
                Some(&entity.name),
                None,
                Some("sink_options_ignored"),
                message,
            );
            append_sink_options_warning(&mut rules, message);
        }
        if let Some(start) = validation_start {
            phase_timings.checks_validation_ms += start.elapsed().as_millis() as u64;
        }

        let split_start = perf_enabled.then(Instant::now);
        let mut write_rejected_ms_this_file = 0_u64;

        match entity.policy.severity.as_str() {
            "warn" => {
                let mut accepted_df = if forced_reject_rows.is_empty() {
                    df
                } else {
                    let force_accept_rows = (0..df.height())
                        .map(|row_idx| !forced_reject_rows.contains(&row_idx))
                        .collect::<Vec<_>>();
                    let (force_accept_mask, _) = check::build_row_masks(&force_accept_rows);
                    df.filter(&force_accept_mask).map_err(|err| {
                        Box::new(RunError(format!(
                            "failed to filter merge duplicate source rows: {err}"
                        )))
                    })?
                };
                rename_output_columns(&mut accepted_df, output_column_map)?;
                accepted_df_opt = Some(accepted_df);
                if has_errors {
                    if let Some(rejected_target) = rejected_target {
                        let write_start = perf_enabled.then(Instant::now);
                        let errors_path_value = write_error_report_output(
                            rejected_target,
                            source_stem,
                            &errors_json,
                            temp_dir,
                            runtime.storage(),
                            &run_context.storage_resolver,
                            entity,
                        )?;
                        if let Some(start) = write_start {
                            write_rejected_ms_this_file += start.elapsed().as_millis() as u64;
                        }
                        errors_path = Some(errors_path_value);
                    } else {
                        let message = format!(
                            "entity.name={} sink.rejected missing; error report not written",
                            entity.name
                        );
                        warnings::emit(
                            &run_context.run_id,
                            Some(&entity.name),
                            None,
                            Some("sink_rejected_missing"),
                            &message,
                        );
                    }
                }
            }
            "reject" => {
                if has_errors {
                    validate_rejected_target(entity, "reject")?;

                    let (accept_mask, reject_mask) = check::build_row_masks(&accept_rows);
                    let mut accepted_df = df.filter(&accept_mask).map_err(|err| {
                        Box::new(RunError(format!("failed to filter accepted rows: {err}")))
                    })?;
                    let mut rejected_df = df.filter(&reject_mask).map_err(|err| {
                        Box::new(RunError(format!("failed to filter rejected rows: {err}")))
                    })?;
                    append_rejection_columns(&mut rejected_df, &errors_json, false)?;
                    rename_output_columns(&mut accepted_df, output_column_map)?;
                    rename_output_columns(&mut rejected_df, output_column_map)?;
                    accepted_df_opt = Some(accepted_df);
                    let rejected_config = entity.sink.rejected.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.storage is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_target = rejected_target.ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.storage is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_mode = if write_mode == config::WriteMode::Overwrite {
                        if rejected_overwrite_used {
                            config::WriteMode::Append
                        } else {
                            rejected_overwrite_used = true;
                            config::WriteMode::Overwrite
                        }
                    } else {
                        write_mode
                    };
                    let rejected_adapter =
                        runtime.rejected_sink_adapter(rejected_config.format.as_str())?;
                    let write_start = perf_enabled.then(Instant::now);
                    let rejected_path_value = write_rejected_output(RejectedOutputContext {
                        adapter: rejected_adapter,
                        target: rejected_target,
                        df: &mut rejected_df,
                        source_stem,
                        temp_dir,
                        cloud: runtime.storage(),
                        resolver: &run_context.storage_resolver,
                        entity,
                        mode: rejected_mode,
                    })?;
                    if let Some(start) = write_start {
                        write_rejected_ms_this_file += start.elapsed().as_millis() as u64;
                    }
                    rejected_path = Some(rejected_path_value);
                } else {
                    let mut accepted_df = df;
                    rename_output_columns(&mut accepted_df, output_column_map)?;
                    accepted_df_opt = Some(accepted_df);
                }
            }
            "abort" => {
                if has_errors {
                    validate_rejected_target(entity, "abort")?;
                    let rejected_target = rejected_target.ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.storage is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_write_start = perf_enabled.then(Instant::now);
                    let rejected_path_value = write_rejected_raw_output(
                        rejected_target,
                        &input_file,
                        temp_dir,
                        runtime.storage(),
                        &run_context.storage_resolver,
                        entity,
                    )?;
                    if let Some(start) = rejected_write_start {
                        write_rejected_ms_this_file += start.elapsed().as_millis() as u64;
                    }
                    let error_write_start = perf_enabled.then(Instant::now);
                    let errors_path_value = write_error_report_output(
                        rejected_target,
                        source_stem,
                        &errors_json,
                        temp_dir,
                        runtime.storage(),
                        &run_context.storage_resolver,
                        entity,
                    )?;
                    if let Some(start) = error_write_start {
                        write_rejected_ms_this_file += start.elapsed().as_millis() as u64;
                    }
                    rejected_path = Some(rejected_path_value);
                    errors_path = Some(errors_path_value);
                } else {
                    let mut accepted_df = df;
                    rename_output_columns(&mut accepted_df, output_column_map)?;
                    accepted_df_opt = Some(accepted_df);
                }
            }
            severity => {
                return Err(Box::new(ConfigError(format!(
                    "unsupported policy severity: {severity}"
                ))))
            }
        }
        if let Some(start) = split_start {
            let split_elapsed_ms = start.elapsed().as_millis() as u64;
            phase_timings.write_rejected_ms += write_rejected_ms_this_file;
            phase_timings.accept_reject_split_ms +=
                split_elapsed_ms.saturating_sub(write_rejected_ms_this_file);
        }

        if let Some(accepted_df) = accepted_df_opt {
            accepted_accum_rows += accepted_df.height() as u64;
            accepted_accum_frames += 1;
            accepted_accum.push(accepted_df);
        }

        if archive_target.is_some() {
            let archive_start = perf_enabled.then(Instant::now);
            archived_path = io::storage::ops::archive_input(
                runtime.storage(),
                &run_context.storage_resolver,
                &run_context.run_id,
                entity,
                archive_target,
                &input_file,
            )?;
            if let Some(start) = archive_start {
                phase_timings.archive_ms += start.elapsed().as_millis() as u64;
            }
        }

        let (status, accepted_count, rejected_count, errors, warnings) =
            match entity.policy.severity.as_str() {
                "warn" => (
                    report::FileStatus::Success,
                    row_count.saturating_sub(forced_reject_count),
                    forced_reject_count,
                    0,
                    violation_count,
                ),
                "reject" => {
                    if has_errors {
                        (
                            report::FileStatus::Rejected,
                            accept_count,
                            reject_count,
                            violation_count,
                            0,
                        )
                    } else {
                        (report::FileStatus::Success, row_count, 0, 0, 0)
                    }
                }
                "abort" => {
                    if has_errors {
                        (
                            report::FileStatus::Aborted,
                            0,
                            row_count,
                            violation_count,
                            0,
                        )
                    } else {
                        (report::FileStatus::Success, row_count, 0, 0, 0)
                    }
                }
                _ => unreachable!("severity validated earlier"),
            };
        let errors = errors + mismatch_errors;
        let warnings = warnings + mismatch_warnings + sink_options_warnings;

        let file_report = report::FileReport {
            input_file: input_file.source_uri.clone(),
            status,
            row_count,
            accepted_count,
            rejected_count,
            mismatch: mismatch_report,
            output: report::FileOutput {
                accepted_path: None,
                rejected_path,
                errors_path,
                archived_path,
            },
            validation: report::FileValidation {
                errors,
                warnings,
                rules,
            },
        };

        totals.rows_total += row_count;
        totals.accepted_total += accepted_count;
        totals.rejected_total += rejected_count;
        totals.errors_total += errors;
        totals.warnings_total += warnings;
        file_reports.push(file_report);
        file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));
        observer.on_event(crate::run::events::RunEvent::FileFinished {
            run_id: run_context.run_id.clone(),
            entity: entity.name.clone(),
            input: input_file.source_uri.clone(),
            status: file_status_str(status).to_string(),
            rows: row_count,
            accepted: accepted_count,
            rejected: rejected_count,
            elapsed_ms: file_timer.elapsed().as_millis() as u64,
            ts_ms: event_time_ms(),
        });

        if status == report::FileStatus::Aborted {
            abort_run = true;
            break;
        }
    }

    Ok(ValidateSplitPhaseOutcome {
        abort_run,
        accepted_accum_rows,
        accepted_accum_frames,
    })
}

fn file_status_str(status: report::FileStatus) -> &'static str {
    match status {
        report::FileStatus::Success => "success",
        report::FileStatus::Rejected => "rejected",
        report::FileStatus::Aborted => "aborted",
        report::FileStatus::Failed => "failed",
    }
}
