use std::time::Instant;

use crate::errors::{IoError, RunError};
use crate::{check, config, io, report, warnings, ConfigError, FloeResult};

use super::file::{collect_row_errors, required_columns};
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
use process::{append_sink_options_warning, sink_options_warning};
use resolve::{resolve_entity_targets, resolve_input_files};

pub(super) struct EntityRunResult {
    pub outcome: EntityOutcome,
    pub abort_run: bool,
}

struct PrecheckedInput {
    input_file: io::format::InputFile,
    mismatch: check::MismatchOutcome,
    file_timer: Instant,
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
                .map_err(|err| Box::new(IoError(format!("tempdir failed: {err}"))))?,
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
    let collect_raw = true;

    let resolved_files = input_files
        .iter()
        .map(|input| input.source_uri.clone())
        .collect::<Vec<_>>();
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
    let archive_enabled = entity.sink.archive.is_some();
    let archive_dir = entity
        .sink
        .archive
        .as_ref()
        .map(|archive| config::resolve_local_path(&context.config_dir, &archive.path));

    let mut file_timings_ms = Vec::with_capacity(input_files.len());
    let sink_options_warning = sink_options_warning(entity);
    let mut sink_options_warned = false;
    let mut abort_run = false;
    let mut prechecked_inputs = Vec::with_capacity(input_files.len());
    for input_file in input_files {
        let file_timer = Instant::now();
        let input_columns =
            match input_adapter.read_input_columns(entity, &input_file, &normalized_columns) {
                Ok(columns) => columns,
                Err(error) => {
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
                        },
                    };

                    totals.errors_total += 1;
                    file_reports.push(file_report);
                    file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));

                    if status == report::FileStatus::Aborted {
                        abort_run = true;
                        break;
                    }
                    continue;
                }
            };

        let mismatch = check::plan_schema_mismatch(entity, &normalized_columns, &input_columns)?;
        if let Some(message) = mismatch.report.warning.as_deref() {
            warnings::emit(message);
        }

        if mismatch.rejected || mismatch.aborted {
            let accepted_path = None;
            let errors_path = None;
            let row_count = 0;

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
                    let archived_path_buf =
                        io::write::archive_input(&input_file.source_local_path, dir)?;
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
                },
            };

            totals.rows_total += row_count;
            totals.accepted_total += accepted_count;
            totals.rejected_total += rejected_count;
            totals.errors_total += errors;
            totals.warnings_total += warnings;
            file_reports.push(file_report);
            file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));

            if mismatch.aborted {
                abort_run = true;
                break;
            }
            continue;
        }

        prechecked_inputs.push(PrecheckedInput {
            input_file,
            mismatch,
            file_timer,
        });
    }

    let mut unique_tracker = check::UniqueTracker::new(&normalized_columns);

    for prechecked in prechecked_inputs {
        let PrecheckedInput {
            input_file,
            mismatch,
            file_timer,
        } = prechecked;
        let mut inputs = input_adapter.read_inputs(
            entity,
            std::slice::from_ref(&input_file),
            &normalized_columns,
            normalize_strategy.as_deref(),
            collect_raw,
        )?;
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
                        input_columns_count: mismatch.report.input_columns_count,
                        missing_columns: mismatch.report.missing_columns.clone(),
                        extra_columns: mismatch.report.extra_columns.clone(),
                        mismatch_action,
                        error: Some(report::MismatchIssue {
                            rule: error.rule,
                            message: format!("entity.name={} {}", entity.name, error.message),
                        }),
                        warning: mismatch.report.warning.clone(),
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

        check::apply_mismatch_plan(&mismatch, &normalized_columns, raw_df.as_mut(), &mut df)?;

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
            check::cast_mismatch_counts(&raw_df, &df, &normalized_columns)?
        } else {
            Vec::new()
        };
        let cast_total = cast_counts.iter().map(|(_, count, _)| *count).sum::<u64>();

        let mut error_lists = if entity.policy.severity == "abort" && cast_total > 0 {
            check::cast_mismatch_errors(
                &raw_df,
                &df,
                &normalized_columns,
                &raw_indices,
                &typed_indices,
            )?
        } else {
            let not_null_counts = check::not_null_counts(&df, &required_cols)?;
            let not_null_total = not_null_counts.iter().map(|(_, count)| *count).sum::<u64>();
            let quick_total = cast_total + not_null_total;

            if quick_total == 0 {
                vec![Vec::new(); row_count as usize]
            } else {
                collect_row_errors(
                    &raw_df,
                    &df,
                    &required_cols,
                    &normalized_columns,
                    track_cast_errors && cast_total > 0,
                    &raw_indices,
                    &typed_indices,
                )?
            }
        };

        if !(unique_tracker.is_empty() || (entity.policy.severity == "abort" && cast_total > 0)) {
            let unique_errors = unique_tracker.apply(&df, &normalized_columns)?;
            for (errors, unique) in error_lists.iter_mut().zip(unique_errors) {
                errors.extend(unique);
            }
        }

        let accept_rows = check::build_accept_rows(&error_lists);
        let errors_json =
            check::build_errors_formatted(&error_lists, &accept_rows, row_error_formatter.as_ref());
        let row_error_count = error_lists
            .iter()
            .filter(|errors| !errors.is_empty())
            .count() as u64;
        let violation_count = error_lists
            .iter()
            .map(|errors| errors.len() as u64)
            .sum::<u64>();

        drop(raw_df);
        let accept_count = accept_rows.iter().filter(|accepted| **accepted).count() as u64;
        let reject_count = row_count.saturating_sub(accept_count);
        let has_errors = row_error_count > 0;
        let mut accepted_path = None;
        let mut rejected_path = None;
        let mut errors_path = None;
        let mut archived_path = None;
        let mut rules = if has_errors {
            summarize_validation(&error_lists, &normalized_columns, severity)
        } else {
            Vec::new()
        };
        let mut sink_options_warnings = 0;
        if let Some(message) = sink_options_warning.as_deref() {
            sink_options_warnings = 1;
            warnings::emit_once(&mut sink_options_warned, message);
            append_sink_options_warning(&mut rules, message);
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
                if has_errors {
                    if let Some(rejected_target) = rejected_target.as_ref() {
                        let errors_path_value = write_error_report_output(
                            rejected_target,
                            source_stem,
                            &errors_json,
                            temp_dir.as_ref().map(|dir| dir.path()),
                            cloud,
                            &context.storage_resolver,
                            entity,
                        )?;
                        errors_path = Some(errors_path_value);
                    } else {
                        warnings::emit(&format!(
                            "entity.name={} sink.rejected missing; error report not written",
                            entity.name
                        ));
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
                let archived_path_buf =
                    io::write::archive_input(&input_file.source_local_path, dir)?;
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
            },
        };

        totals.rows_total += row_count;
        totals.accepted_total += accepted_count;
        totals.rejected_total += rejected_count;
        totals.errors_total += errors;
        totals.warnings_total += warnings;
        file_reports.push(file_report);
        file_timings_ms.push(Some(file_timer.elapsed().as_millis() as u64));

        if status == report::FileStatus::Aborted {
            abort_run = true;
            break;
        }
    }

    totals.files_total = file_reports.len() as u64;

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
