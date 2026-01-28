use std::time::Instant;

use crate::{check, config, io, report, ConfigError, FloeResult};

use super::file::{collect_errors, read_inputs, required_columns};
use super::normalize::normalize_schema_columns;
use super::normalize::resolve_normalize_strategy;
use super::output::{
    append_rejection_columns, validate_rejected_target, write_accepted_output,
    write_error_report_output, write_rejected_output, write_rejected_raw_output,
};
use super::reporting::summarize_validation;
use super::{EntityOutcome, RunContext, MAX_RESOLVED_INPUTS};

use io::format::{self, ReadInput};
use io::storage::Target;

mod entity_report;
mod process;
mod resolve;

use entity_report::build_run_report;
use process::{append_sink_options_warning, sink_options_warning, try_warn_counts};
use resolve::{resolve_entity_targets, resolve_input_files};

pub(super) struct EntityRunResult {
    pub outcome: EntityOutcome,
    pub abort_run: bool,
}

pub(super) struct WarnOutcome {
    pub file_report: report::FileReport,
    pub status: report::FileStatus,
    pub elapsed_ms: u64,
}

pub(super) fn run_entity(
    context: &RunContext,
    cloud: &mut io::storage::CloudClient,
    entity: &config::EntityConfig,
) -> FloeResult<EntityRunResult> {
    let input = &entity.source;
    let input_adapter = format::input_adapter(input.format.as_str())?;
    let resolved_targets = resolve_entity_targets(&context.storage_resolver, entity)?;
    let source_is_s3 = matches!(resolved_targets.source, Target::S3 { .. });
    let formatter_name = context
        .config
        .report
        .as_ref()
        .and_then(|report| report.formatter.as_deref())
        .unwrap_or("json");
    let row_error_formatter = check::row_error_formatter(formatter_name)?;

    let normalize_strategy = resolve_normalize_strategy(entity)?;
    let normalized_columns = if let Some(strategy) = normalize_strategy.as_deref() {
        normalize_schema_columns(&entity.schema.columns, strategy)?
    } else {
        entity.schema.columns.clone()
    };
    let required_cols = required_columns(&normalized_columns);
    let accepted_target = resolved_targets.accepted.clone();
    let rejected_target = resolved_targets.rejected.clone();
    let needs_temp = source_is_s3
        || matches!(accepted_target, Target::S3 { .. })
        || matches!(rejected_target, Some(Target::S3 { .. }));
    let temp_dir = if needs_temp {
        Some(
            tempfile::TempDir::new()
                .map_err(|err| Box::new(ConfigError(format!("tempdir failed: {err}"))))?,
        )
    } else {
        None
    };

    let (input_files, resolved_mode) = resolve_input_files(
        context,
        cloud,
        entity,
        input_adapter,
        &resolved_targets,
        source_is_s3,
        temp_dir.as_ref(),
    )?;

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
    let collect_raw = entity.policy.severity != "warn" || track_cast_errors;

    let inputs = read_inputs(
        input_adapter,
        entity,
        &input_files,
        &normalized_columns,
        normalize_strategy.as_deref(),
        collect_raw,
    )?;
    let resolved_files = input_files
        .iter()
        .map(|input| input.source_uri.clone())
        .collect::<Vec<_>>();
    let reported_files = resolved_files
        .iter()
        .take(MAX_RESOLVED_INPUTS)
        .cloned()
        .collect::<Vec<_>>();

    let mut file_reports = Vec::with_capacity(inputs.len());
    let mut file_statuses = Vec::with_capacity(inputs.len());
    let mut totals = report::ResultsTotals {
        files_total: 0,
        rows_total: 0,
        accepted_total: 0,
        rejected_total: 0,
        warnings_total: 0,
        errors_total: 0,
    };
    let archive_enabled = entity.sink.archive.is_some();
    let archive_dir = entity
        .sink
        .archive
        .as_ref()
        .map(|archive| config::resolve_local_path(&context.config_dir, &archive.path));

    let mut file_timings_ms = Vec::with_capacity(inputs.len());
    let sink_options_warning = sink_options_warning(entity);
    let mut sink_options_warned = false;
    let mut abort_run = false;
    for input in inputs {
        let file_timer = Instant::now();
        let (input_file, input_columns, mut raw_df, mut df) = match input {
            ReadInput::Data {
                input_file,
                input_columns,
                raw_df,
                typed_df,
            } => (input_file, input_columns, raw_df, typed_df),
            ReadInput::FileError { input_file, error } => {
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
                    .as_ref()
                    .map(|target| {
                        write_rejected_raw_output(
                            target,
                            &input_file,
                            temp_dir.as_ref().map(|dir| dir.path()),
                            cloud,
                            &context.storage_resolver,
                            entity,
                        )
                    })
                    .transpose()?;

                let file_report = report::FileReport {
                    input_file: input_file.source_uri.clone(),
                    status,
                    row_count: 0,
                    accepted_count: 0,
                    rejected_count: 0,
                    mismatch: report::FileMismatch {
                        declared_columns_count: normalized_columns.len() as u64,
                        input_columns_count: 0,
                        missing_columns: Vec::new(),
                        extra_columns: Vec::new(),
                        mismatch_action,
                        error: Some(report::MismatchIssue {
                            rule: error.rule,
                            message: format!("entity.name={} {}", entity.name, error.message),
                        }),
                        warning: None,
                    },
                    output: report::FileOutput {
                        accepted_path: None,
                        rejected_path,
                        errors_path: None,
                        archived_path: None,
                    },
                    validation: report::FileValidation {
                        errors: 1,
                        warnings: 0,
                        rules: Vec::new(),
                        examples: report::ExampleSummary {
                            max_examples_per_rule: 3,
                            items: Vec::new(),
                        },
                    },
                };

                totals.errors_total += 1;
                file_statuses.push(status);
                file_reports.push(file_report);
                file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));

                if status == report::FileStatus::Aborted {
                    abort_run = true;
                    break;
                }
                continue;
            }
        };

        let source_stem = input_file.source_stem.as_str();
        let mismatch = check::apply_schema_mismatch(
            entity,
            &normalized_columns,
            &input_columns,
            raw_df.as_mut(),
            &mut df,
        )?;
        let row_count = raw_df
            .as_ref()
            .map(|df| df.height())
            .unwrap_or_else(|| df.height()) as u64;

        if mismatch.rejected || mismatch.aborted {
            let accepted_path = None;
            let errors_path = None;

            validate_rejected_target(entity, if mismatch.aborted { "abort" } else { "reject" })?;
            let rejected_target = rejected_target.as_ref().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} sink.rejected.storage is required for rejection",
                    entity.name
                )))
            })?;
            let rejected_path = Some(write_rejected_raw_output(
                rejected_target,
                &input_file,
                temp_dir.as_ref().map(|dir| dir.path()),
                cloud,
                &context.storage_resolver,
                entity,
            )?);

            let archived_path = if archive_enabled {
                if let Some(dir) = &archive_dir {
                    let archived_path_buf = io::write::archive_input(&input_file.local_path, dir)?;
                    Some(archived_path_buf.display().to_string())
                } else {
                    None
                }
            } else {
                None
            };

            let status = if mismatch.aborted {
                report::FileStatus::Aborted
            } else {
                report::FileStatus::Rejected
            };
            let accepted_count = 0;
            let rejected_count = row_count;
            let errors = mismatch.errors;
            let warnings = mismatch.warnings;

            let file_report = report::FileReport {
                input_file: input_file.source_uri.clone(),
                status,
                row_count,
                accepted_count,
                rejected_count,
                mismatch: mismatch.report,
                output: report::FileOutput {
                    accepted_path,
                    rejected_path,
                    errors_path,
                    archived_path,
                },
                validation: report::FileValidation {
                    errors,
                    warnings,
                    rules: Vec::new(),
                    examples: report::ExampleSummary {
                        max_examples_per_rule: 3,
                        items: Vec::new(),
                    },
                },
            };

            totals.rows_total += row_count;
            totals.accepted_total += accepted_count;
            totals.rejected_total += rejected_count;
            totals.errors_total += errors;
            totals.warnings_total += warnings;
            file_statuses.push(status);
            file_reports.push(file_report);
            file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));

            if mismatch.aborted {
                abort_run = true;
                break;
            }
            continue;
        }

        let mut warn_ctx = process::WarnContext {
            entity,
            input_file: &input_file,
            row_count,
            raw_df: raw_df.as_ref(),
            df: &mut df,
            required_cols: &required_cols,
            normalized_columns: &normalized_columns,
            track_cast_errors,
            severity,
            accepted_target: &accepted_target,
            sink_options_warning: &sink_options_warning,
            sink_options_warned: &mut sink_options_warned,
            archive_enabled,
            archive_dir: &archive_dir,
            mismatch: &mismatch,
            source_stem,
            temp_dir: temp_dir.as_ref().map(|dir| dir.path()),
            cloud,
            resolver: &context.storage_resolver,
            elapsed_ms: file_timer.elapsed().as_millis() as u64,
        };
        if let Some(outcome) = try_warn_counts(&mut warn_ctx)? {
            totals.rows_total += outcome.file_report.row_count;
            totals.accepted_total += outcome.file_report.accepted_count;
            totals.rejected_total += outcome.file_report.rejected_count;
            totals.errors_total += outcome.file_report.validation.errors;
            totals.warnings_total += outcome.file_report.validation.warnings;
            file_statuses.push(outcome.status);
            file_reports.push(outcome.file_report);
            file_timings_ms.push(Some(outcome.elapsed_ms));
            continue;
        }

        let raw_df = raw_df.ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} raw dataframe unavailable for rejection checks",
                entity.name
            )))
        })?;
        let raw_indices = check::column_index_map(&raw_df);
        let typed_indices = check::column_index_map(&df);

        let cast_counts = if track_cast_errors {
            check::cast_mismatch_counts(&raw_df, &df, &normalized_columns)?
        } else {
            Vec::new()
        };
        let cast_total = cast_counts.iter().map(|(_, count, _)| *count).sum::<u64>();

        let (accept_rows, errors_json, error_lists, row_error_count, violation_count) =
            if entity.policy.severity == "abort" && cast_total > 0 {
                let cast_errors = check::cast_mismatch_errors(
                    &raw_df,
                    &df,
                    &normalized_columns,
                    &raw_indices,
                    &typed_indices,
                )?;
                let accept_rows = check::build_accept_rows(&cast_errors);
                let errors_json = check::build_errors_formatted(
                    &cast_errors,
                    &accept_rows,
                    row_error_formatter.as_ref(),
                );
                let row_error_count = cast_errors
                    .iter()
                    .filter(|errors| !errors.is_empty())
                    .count() as u64;
                let violation_count = cast_errors
                    .iter()
                    .map(|errors| errors.len() as u64)
                    .sum::<u64>();
                (
                    accept_rows,
                    errors_json,
                    cast_errors,
                    row_error_count,
                    violation_count,
                )
            } else {
                let not_null_counts = check::not_null_counts(&df, &required_cols)?;
                let unique_counts = check::unique_counts(&df, &normalized_columns)?;
                let not_null_total = not_null_counts.iter().map(|(_, count)| *count).sum::<u64>();
                let unique_total = unique_counts.iter().map(|(_, count)| *count).sum::<u64>();
                let quick_total = cast_total + not_null_total + unique_total;

                if quick_total == 0 {
                    (vec![true; row_count as usize], Vec::new(), Vec::new(), 0, 0)
                } else {
                    let (rows, json, lists) = collect_errors(
                        &raw_df,
                        &df,
                        &required_cols,
                        &normalized_columns,
                        track_cast_errors && cast_total > 0,
                        &raw_indices,
                        &typed_indices,
                        row_error_formatter.as_ref(),
                    )?;
                    let row_error_count =
                        lists.iter().filter(|errors| !errors.is_empty()).count() as u64;
                    let violation_count =
                        lists.iter().map(|errors| errors.len() as u64).sum::<u64>();
                    (rows, json, lists, row_error_count, violation_count)
                }
            };
        drop(raw_df);
        let accept_count = accept_rows.iter().filter(|accepted| **accepted).count() as u64;
        let reject_count = row_count.saturating_sub(accept_count);
        let has_errors = row_error_count > 0;
        let mut accepted_path = None;
        let mut rejected_path = None;
        let mut errors_path = None;
        let mut archived_path = None;
        let (mut rules, mut examples) = if has_errors {
            summarize_validation(&error_lists, &normalized_columns, severity)
        } else {
            (
                Vec::new(),
                report::ExampleSummary {
                    max_examples_per_rule: 3,
                    items: Vec::new(),
                },
            )
        };
        let mut sink_options_warnings = 0;
        if let Some(message) = sink_options_warning.as_deref() {
            sink_options_warnings = 1;
            if !sink_options_warned {
                eprintln!("warn: {message}");
                sink_options_warned = true;
            }
            append_sink_options_warning(&mut rules, &mut examples, message);
        }

        match entity.policy.severity.as_str() {
            "warn" => {
                let output_path = write_accepted_output(
                    entity.sink.accepted.format.as_str(),
                    &accepted_target,
                    &mut df,
                    source_stem,
                    temp_dir.as_ref().map(|dir| dir.path()),
                    cloud,
                    &context.storage_resolver,
                    entity,
                )?;
                accepted_path = Some(output_path);
            }
            "reject" => {
                if has_errors {
                    validate_rejected_target(entity, "reject")?;

                    let (accept_mask, reject_mask) = check::build_row_masks(&accept_rows);
                    let mut accepted_df = df.filter(&accept_mask).map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to filter accepted rows: {err}"
                        )))
                    })?;
                    let mut rejected_df = df.filter(&reject_mask).map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to filter rejected rows: {err}"
                        )))
                    })?;
                    append_rejection_columns(&mut rejected_df, &errors_json, false)?;

                    let output_path = write_accepted_output(
                        entity.sink.accepted.format.as_str(),
                        &accepted_target,
                        &mut accepted_df,
                        source_stem,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        cloud,
                        &context.storage_resolver,
                        entity,
                    )?;
                    accepted_path = Some(output_path);
                    let rejected_config = entity.sink.rejected.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.storage is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_target = rejected_target.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.storage is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_path_value = write_rejected_output(
                        rejected_config.format.as_str(),
                        rejected_target,
                        &mut rejected_df,
                        source_stem,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        cloud,
                        &context.storage_resolver,
                        entity,
                    )?;
                    rejected_path = Some(rejected_path_value);
                } else {
                    let output_path = write_accepted_output(
                        entity.sink.accepted.format.as_str(),
                        &accepted_target,
                        &mut df,
                        source_stem,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        cloud,
                        &context.storage_resolver,
                        entity,
                    )?;
                    accepted_path = Some(output_path);
                }
            }
            "abort" => {
                if has_errors {
                    validate_rejected_target(entity, "abort")?;
                    let rejected_target = rejected_target.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.storage is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_path_value = write_rejected_raw_output(
                        rejected_target,
                        &input_file,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        cloud,
                        &context.storage_resolver,
                        entity,
                    )?;
                    let errors_path_value = write_error_report_output(
                        rejected_target,
                        source_stem,
                        &errors_json,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        cloud,
                        &context.storage_resolver,
                        entity,
                    )?;
                    rejected_path = Some(rejected_path_value);
                    errors_path = Some(errors_path_value);
                } else {
                    let output_path = write_accepted_output(
                        entity.sink.accepted.format.as_str(),
                        &accepted_target,
                        &mut df,
                        source_stem,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        cloud,
                        &context.storage_resolver,
                        entity,
                    )?;
                    accepted_path = Some(output_path);
                }
            }
            severity => {
                return Err(Box::new(ConfigError(format!(
                    "unsupported policy severity: {severity}"
                ))))
            }
        }

        if archive_enabled {
            if let Some(dir) = &archive_dir {
                let archived_path_buf = io::write::archive_input(&input_file.local_path, dir)?;
                archived_path = Some(archived_path_buf.display().to_string());
            }
        }

        let (status, accepted_count, rejected_count, errors, warnings) =
            match entity.policy.severity.as_str() {
                "warn" => (
                    report::FileStatus::Success,
                    row_count,
                    0,
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
        let errors = errors + mismatch.errors;
        let warnings = warnings + mismatch.warnings + sink_options_warnings;

        let file_report = report::FileReport {
            input_file: input_file.source_uri.clone(),
            status,
            row_count,
            accepted_count,
            rejected_count,
            mismatch: mismatch.report,
            output: report::FileOutput {
                accepted_path,
                rejected_path,
                errors_path,
                archived_path,
            },
            validation: report::FileValidation {
                errors,
                warnings,
                rules,
                examples,
            },
        };

        totals.rows_total += row_count;
        totals.accepted_total += accepted_count;
        totals.rejected_total += rejected_count;
        totals.errors_total += errors;
        totals.warnings_total += warnings;
        file_statuses.push(status);
        file_reports.push(file_report);
        file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));
    }

    totals.files_total = file_reports.len() as u64;

    let (mut run_status, exit_code) = report::compute_run_outcome(&file_statuses);
    if run_status == report::RunStatus::Success && totals.warnings_total > 0 {
        run_status = report::RunStatus::SuccessWithWarnings;
    }

    let run_report = build_run_report(entity_report::RunReportContext {
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
        run_status,
        exit_code,
    });

    if let Some(report_dir) = &context.report_dir {
        report::ReportWriter::write_report(report_dir, &context.run_id, &entity.name, &run_report)?;
    }

    Ok(EntityRunResult {
        outcome: EntityOutcome {
            report: run_report,
            file_timings_ms,
        },
        abort_run,
    })
}
