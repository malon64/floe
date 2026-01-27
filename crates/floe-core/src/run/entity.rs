use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::{check, config, io, report, ConfigError, FloeResult};

use super::file::{collect_errors, read_inputs, required_columns};
use super::normalize::normalize_schema_columns;
use super::normalize::resolve_normalize_strategy;
use super::output::{
    append_rejection_columns, validate_rejected_target, write_accepted_output,
    write_error_report_output, write_rejected_output, write_rejected_raw_output,
};
use super::reporting::{
    entity_metadata_json, project_metadata_json, source_options_json, summarize_validation,
};
use super::{EntityOutcome, RunContext, MAX_RESOLVED_INPUTS};

use io::format::{self, InputAdapter, InputFile, ReadInput, StorageTarget};

pub(super) struct EntityRunResult {
    pub outcome: EntityOutcome,
    pub abort_run: bool,
}

struct WarnOutcome {
    file_report: report::FileReport,
    status: report::FileStatus,
    elapsed_ms: u64,
}

#[derive(Debug, Clone)]
struct ResolvedEntityPaths {
    source: config::ResolvedPath,
    accepted: config::ResolvedPath,
    rejected: Option<config::ResolvedPath>,
}

pub(super) fn run_entity(
    context: &RunContext,
    s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
    entity: &config::EntityConfig,
) -> FloeResult<EntityRunResult> {
    let config = &context.config;
    let input = &entity.source;
    let input_adapter = format::input_adapter(input.format.as_str())?;
    let resolved_paths = resolve_entity_paths(&context.filesystem_resolver, entity)?;
    let source_definition = filesystem_definition(
        &context.filesystem_resolver,
        &resolved_paths.source.filesystem,
        entity,
    )?;
    let source_is_s3 = source_definition.fs_type == "s3";
    if !source_is_s3 && resolved_paths.source.local_path.is_none() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} source.filesystem={} is not supported for run",
            entity.name, resolved_paths.source.filesystem
        ))));
    }

    let normalize_strategy = resolve_normalize_strategy(entity)?;
    let normalized_columns = if let Some(strategy) = normalize_strategy.as_deref() {
        normalize_schema_columns(&entity.schema.columns, strategy)?
    } else {
        entity.schema.columns.clone()
    };
    let required_cols = required_columns(&normalized_columns);
    let accepted_target = storage_target_from_resolved(&resolved_paths.accepted)?;
    let rejected_target = resolved_paths
        .rejected
        .as_ref()
        .map(storage_target_from_resolved)
        .transpose()?;
    let needs_temp = source_is_s3
        || matches!(accepted_target, StorageTarget::S3 { .. })
        || matches!(rejected_target, Some(StorageTarget::S3 { .. }));
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
        s3_clients,
        entity,
        input_adapter,
        &resolved_paths,
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

                let rejected_target = resolved_paths
                    .rejected
                    .as_ref()
                    .map(storage_target_from_resolved)
                    .transpose()?;
                let rejected_path = rejected_target
                    .as_ref()
                    .map(|target| {
                        write_rejected_raw_output(
                            target,
                            &input_file,
                            temp_dir.as_ref().map(|dir| dir.path()),
                            s3_clients,
                            &context.filesystem_resolver,
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
                    "entity.name={} sink.rejected.filesystem is required for rejection",
                    entity.name
                )))
            })?;
            let rejected_path = Some(write_rejected_raw_output(
                rejected_target,
                &input_file,
                temp_dir.as_ref().map(|dir| dir.path()),
                s3_clients,
                &context.filesystem_resolver,
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

        if let Some(outcome) = try_warn_counts(
            entity,
            &input_file,
            row_count,
            raw_df.as_ref(),
            &mut df,
            &required_cols,
            &normalized_columns,
            track_cast_errors,
            severity,
            &accepted_target,
            &sink_options_warning,
            &mut sink_options_warned,
            archive_enabled,
            &archive_dir,
            &mismatch,
            source_stem,
            temp_dir.as_ref().map(|dir| dir.path()),
            s3_clients,
            &context.filesystem_resolver,
            file_timer.elapsed().as_millis() as u64,
        )? {
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
                let errors_json = check::build_errors_json(&cast_errors, &accept_rows);
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
                    s3_clients,
                    &context.filesystem_resolver,
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
                        s3_clients,
                        &context.filesystem_resolver,
                        entity,
                    )?;
                    accepted_path = Some(output_path);
                    let rejected_config = entity.sink.rejected.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.filesystem is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_target = rejected_target.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.filesystem is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_path_value = write_rejected_output(
                        rejected_config.format.as_str(),
                        rejected_target,
                        &mut rejected_df,
                        source_stem,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        s3_clients,
                        &context.filesystem_resolver,
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
                        s3_clients,
                        &context.filesystem_resolver,
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
                            "entity.name={} sink.rejected.filesystem is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_path_value = write_rejected_raw_output(
                        rejected_target,
                        &input_file,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        s3_clients,
                        &context.filesystem_resolver,
                        entity,
                    )?;
                    let errors_path_value = write_error_report_output(
                        rejected_target,
                        source_stem,
                        &errors_json,
                        temp_dir.as_ref().map(|dir| dir.path()),
                        s3_clients,
                        &context.filesystem_resolver,
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
                        s3_clients,
                        &context.filesystem_resolver,
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

    let report_path = context
        .report_dir
        .as_ref()
        .map(|dir| report::ReportWriter::report_path(dir, &context.run_id, &entity.name));
    let report_base_path = context
        .report_base_path
        .clone()
        .unwrap_or_else(|| "disabled".to_string());
    let report_file_path = report_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "disabled".to_string());
    let finished_at = report::now_rfc3339();
    let duration_ms = context.run_timer.elapsed().as_millis() as u64;
    let run_report = report::RunReport {
        spec_version: config.version.clone(),
        tool: report::ToolInfo {
            name: "floe".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git: None,
        },
        run: report::RunInfo {
            run_id: context.run_id.clone(),
            started_at: context.started_at.clone(),
            finished_at,
            duration_ms,
            status: run_status,
            exit_code,
        },
        config: report::ConfigEcho {
            path: context.config_path.display().to_string(),
            version: config.version.clone(),
            metadata: config.metadata.as_ref().map(project_metadata_json),
        },
        entity: report::EntityEcho {
            name: entity.name.clone(),
            metadata: entity.metadata.as_ref().map(entity_metadata_json),
        },
        source: report::SourceEcho {
            format: input.format.clone(),
            path: resolved_paths.source.uri.clone(),
            options: input.options.as_ref().map(source_options_json),
            cast_mode: input.cast_mode.clone(),
            read_plan: report::SourceReadPlan::RawAndTyped,
            resolved_inputs: report::ResolvedInputs {
                mode: resolved_mode,
                file_count: resolved_files.len() as u64,
                files: reported_files,
            },
        },
        sink: report::SinkEcho {
            accepted: report::SinkTargetEcho {
                format: entity.sink.accepted.format.clone(),
                path: resolved_paths.accepted.uri.clone(),
            },
            rejected: entity
                .sink
                .rejected
                .as_ref()
                .map(|rejected| report::SinkTargetEcho {
                    format: rejected.format.clone(),
                    path: resolved_paths
                        .rejected
                        .as_ref()
                        .map(|target| target.uri.clone())
                        .unwrap_or_else(|| rejected.path.clone()),
                }),
            archive: report::SinkArchiveEcho {
                enabled: entity.sink.archive.is_some(),
                path: entity
                    .sink
                    .archive
                    .as_ref()
                    .map(|archive| archive.path.clone()),
            },
        },
        report: report::ReportEcho {
            path: report_base_path,
            report_file: report_file_path,
        },
        policy: report::PolicyEcho { severity },
        results: totals,
        files: file_reports,
    };

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

#[allow(clippy::too_many_arguments)]
fn try_warn_counts(
    entity: &config::EntityConfig,
    input_file: &InputFile,
    row_count: u64,
    raw_df: Option<&polars::prelude::DataFrame>,
    df: &mut polars::prelude::DataFrame,
    required_cols: &[String],
    normalized_columns: &[config::ColumnConfig],
    track_cast_errors: bool,
    severity: report::Severity,
    accepted_target: &StorageTarget,
    sink_options_warning: &Option<String>,
    sink_options_warned: &mut bool,
    archive_enabled: bool,
    archive_dir: &Option<PathBuf>,
    mismatch: &check::MismatchOutcome,
    source_stem: &str,
    temp_dir: Option<&Path>,
    s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
    resolver: &config::FilesystemResolver,
    elapsed_ms: u64,
) -> FloeResult<Option<WarnOutcome>> {
    if entity.policy.severity != "warn" {
        return Ok(None);
    }

    let cast_counts = if track_cast_errors {
        let raw_df = raw_df.ok_or_else(|| {
            Box::new(ConfigError(format!(
                "entity.name={} raw dataframe unavailable for cast checks",
                entity.name
            )))
        })?;
        check::cast_mismatch_counts(raw_df, df, normalized_columns)?
    } else {
        Vec::new()
    };
    let not_null_counts = check::not_null_counts(df, required_cols)?;
    let unique_counts = check::unique_counts(df, normalized_columns)?;
    let cast_total = cast_counts.iter().map(|(_, count, _)| *count).sum::<u64>();
    let not_null_total = not_null_counts.iter().map(|(_, count)| *count).sum::<u64>();
    let unique_total = unique_counts.iter().map(|(_, count)| *count).sum::<u64>();
    let violation_count = cast_total + not_null_total + unique_total;

    let mut rules = Vec::new();
    if cast_total > 0 {
        let columns = cast_counts
            .iter()
            .map(|(name, count, target_type)| report::ColumnSummary {
                column: name.clone(),
                violations: *count,
                target_type: Some(target_type.clone()),
            })
            .collect();
        rules.push(report::RuleSummary {
            rule: report::RuleName::CastError,
            severity,
            violations: cast_total,
            columns,
        });
    }
    if not_null_total > 0 {
        let columns = not_null_counts
            .iter()
            .map(|(name, count)| report::ColumnSummary {
                column: name.clone(),
                violations: *count,
                target_type: None,
            })
            .collect();
        rules.push(report::RuleSummary {
            rule: report::RuleName::NotNull,
            severity,
            violations: not_null_total,
            columns,
        });
    }
    if unique_total > 0 {
        let columns = unique_counts
            .iter()
            .map(|(name, count)| report::ColumnSummary {
                column: name.clone(),
                violations: *count,
                target_type: None,
            })
            .collect();
        rules.push(report::RuleSummary {
            rule: report::RuleName::Unique,
            severity,
            violations: unique_total,
            columns,
        });
    }

    let mut examples = report::ExampleSummary {
        max_examples_per_rule: 3,
        items: Vec::new(),
    };
    let mut archived_path = None;
    let mut sink_options_warnings = 0;
    if let Some(message) = sink_options_warning.as_deref() {
        sink_options_warnings = 1;
        if !*sink_options_warned {
            eprintln!("warn: {message}");
            *sink_options_warned = true;
        }
        append_sink_options_warning(&mut rules, &mut examples, message);
    }

    let output_path = write_accepted_output(
        entity.sink.accepted.format.as_str(),
        accepted_target,
        df,
        source_stem,
        temp_dir,
        s3_clients,
        resolver,
        entity,
    )?;
    let accepted_path = Some(output_path);

    if archive_enabled {
        if let Some(dir) = archive_dir {
            let archived_path_buf = io::write::archive_input(&input_file.local_path, dir)?;
            archived_path = Some(archived_path_buf.display().to_string());
        }
    }

    let errors = mismatch.errors;
    let warnings = violation_count + mismatch.warnings + sink_options_warnings;
    let status = report::FileStatus::Success;

    let file_report = report::FileReport {
        input_file: input_file.source_uri.clone(),
        status,
        row_count,
        accepted_count: row_count,
        rejected_count: 0,
        mismatch: mismatch.report.clone(),
        output: report::FileOutput {
            accepted_path,
            rejected_path: None,
            errors_path: None,
            archived_path,
        },
        validation: report::FileValidation {
            errors,
            warnings,
            rules,
            examples,
        },
    };

    Ok(Some(WarnOutcome {
        file_report,
        status,
        elapsed_ms,
    }))
}

fn sink_options_warning(entity: &config::EntityConfig) -> Option<String> {
    let options = entity.sink.accepted.options.as_ref()?;
    if entity.sink.accepted.format == "parquet" {
        return None;
    }
    let mut keys = Vec::new();
    if options.compression.is_some() {
        keys.push("compression");
    }
    if options.row_group_size.is_some() {
        keys.push("row_group_size");
    }
    let detail = if keys.is_empty() {
        "options".to_string()
    } else {
        keys.join(", ")
    };
    Some(format!(
        "entity.name={} sink.accepted.options ({detail}) ignored for format={}",
        entity.name, entity.sink.accepted.format
    ))
}

fn append_sink_options_warning(
    rules: &mut Vec<report::RuleSummary>,
    examples: &mut report::ExampleSummary,
    message: &str,
) {
    let column = "sink.accepted.options".to_string();
    if let Some(rule) = rules
        .iter_mut()
        .find(|rule| rule.rule == report::RuleName::SchemaError)
    {
        rule.violations += 1;
        rule.severity = report::Severity::Warn;
        if let Some(entry) = rule.columns.iter_mut().find(|entry| entry.column == column) {
            entry.violations += 1;
        } else {
            rule.columns.push(report::ColumnSummary {
                column: column.clone(),
                violations: 1,
                target_type: None,
            });
        }
    } else {
        rules.push(report::RuleSummary {
            rule: report::RuleName::SchemaError,
            severity: report::Severity::Warn,
            violations: 1,
            columns: vec![report::ColumnSummary {
                column: column.clone(),
                violations: 1,
                target_type: None,
            }],
        });
    }

    let existing = examples
        .items
        .iter()
        .filter(|item| item.rule == report::RuleName::SchemaError)
        .count() as u64;
    if existing < examples.max_examples_per_rule {
        examples.items.push(report::ValidationExample {
            rule: report::RuleName::SchemaError,
            column,
            row_index: 0,
            message: message.to_string(),
        });
    }
}

fn resolve_input_files(
    context: &RunContext,
    s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
    entity: &config::EntityConfig,
    input_adapter: &dyn InputAdapter,
    resolved_paths: &ResolvedEntityPaths,
    source_is_s3: bool,
    temp_dir: Option<&tempfile::TempDir>,
) -> FloeResult<(Vec<InputFile>, report::ResolvedInputMode)> {
    if source_is_s3 {
        let source_location = io::fs::s3::parse_s3_uri(&resolved_paths.source.uri)?;
        let s3_client = s3_client_for(
            s3_clients,
            &context.filesystem_resolver,
            &resolved_paths.source.filesystem,
            entity,
        )?;
        let temp_dir =
            temp_dir.ok_or_else(|| Box::new(ConfigError("s3 tempdir missing".to_string())))?;
        let inputs = build_s3_inputs(
            s3_client,
            &source_location.bucket,
            &source_location.key,
            input_adapter,
            temp_dir.path(),
            entity,
            &resolved_paths.source.filesystem,
        )?;
        return Ok((inputs, report::ResolvedInputMode::Directory));
    }

    let resolved_inputs = input_adapter.resolve_local_inputs(
        &context.config_dir,
        &entity.name,
        &entity.source,
        &resolved_paths.source.filesystem,
    )?;
    let inputs = build_local_inputs(&resolved_inputs.files, entity);
    let mode = match resolved_inputs.mode {
        io::fs::local::LocalInputMode::File => report::ResolvedInputMode::File,
        io::fs::local::LocalInputMode::Directory => report::ResolvedInputMode::Directory,
    };
    Ok((inputs, mode))
}

fn resolve_entity_paths(
    resolver: &config::FilesystemResolver,
    entity: &config::EntityConfig,
) -> FloeResult<ResolvedEntityPaths> {
    let source = resolver.resolve_path(
        &entity.name,
        "source.filesystem",
        entity.source.filesystem.as_deref(),
        &entity.source.path,
    )?;
    let accepted = resolver.resolve_path(
        &entity.name,
        "sink.accepted.filesystem",
        entity.sink.accepted.filesystem.as_deref(),
        &entity.sink.accepted.path,
    )?;
    let rejected = entity
        .sink
        .rejected
        .as_ref()
        .map(|rejected| {
            resolver.resolve_path(
                &entity.name,
                "sink.rejected.filesystem",
                rejected.filesystem.as_deref(),
                &rejected.path,
            )
        })
        .transpose()?;
    Ok(ResolvedEntityPaths {
        source,
        accepted,
        rejected,
    })
}

fn storage_target_from_resolved(resolved: &config::ResolvedPath) -> FloeResult<StorageTarget> {
    if let Some(path) = &resolved.local_path {
        return Ok(StorageTarget::Local {
            base_path: path.display().to_string(),
        });
    }
    let location = io::fs::s3::parse_s3_uri(&resolved.uri)?;
    Ok(StorageTarget::S3 {
        filesystem: resolved.filesystem.clone(),
        bucket: location.bucket,
        base_key: location.key,
    })
}

fn filesystem_definition(
    resolver: &config::FilesystemResolver,
    name: &str,
    entity: &config::EntityConfig,
) -> FloeResult<config::FilesystemDefinition> {
    resolver.definition(name).ok_or_else(|| {
        Box::new(ConfigError(format!(
            "entity.name={} filesystem {} is not defined",
            entity.name, name
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

pub(crate) fn s3_client_for<'a>(
    clients: &'a mut HashMap<String, io::fs::s3::S3Client>,
    resolver: &config::FilesystemResolver,
    filesystem: &str,
    entity: &config::EntityConfig,
) -> FloeResult<&'a mut io::fs::s3::S3Client> {
    if !clients.contains_key(filesystem) {
        let definition = filesystem_definition(resolver, filesystem, entity)?;
        if definition.fs_type != "s3" {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} filesystem {} is not s3",
                entity.name, filesystem
            ))));
        }
        let client = io::fs::s3::S3Client::new(definition.region.as_deref())?;
        clients.insert(filesystem.to_string(), client);
    }
    Ok(clients.get_mut(filesystem).expect("s3 client inserted"))
}

fn build_local_inputs(files: &[PathBuf], entity: &config::EntityConfig) -> Vec<InputFile> {
    files
        .iter()
        .map(|path| {
            let source_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(entity.name.as_str())
                .to_string();
            let source_stem = Path::new(&source_name)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or(entity.name.as_str())
                .to_string();
            InputFile {
                source_uri: path.display().to_string(),
                local_path: path.clone(),
                source_name,
                source_stem,
            }
        })
        .collect()
}

fn build_s3_inputs(
    client: &io::fs::s3::S3Client,
    bucket: &str,
    prefix: &str,
    adapter: &dyn InputAdapter,
    temp_dir: &Path,
    entity: &config::EntityConfig,
    filesystem: &str,
) -> FloeResult<Vec<InputFile>> {
    let suffixes = adapter.suffixes()?;
    let keys = client.list_objects(bucket, prefix)?;
    let keys = io::fs::s3::filter_keys_by_suffixes(keys, &suffixes);
    if keys.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} source.filesystem={} no input objects matched (bucket={}, prefix={}, suffixes={})",
            entity.name,
            filesystem,
            bucket,
            prefix,
            suffixes.join(",")
        ))));
    }
    let mut inputs = Vec::with_capacity(keys.len());
    for key in keys {
        let local_path = io::fs::s3::temp_path_for_key(temp_dir, &key);
        client.download_object(bucket, &key, &local_path)?;
        let source_name =
            io::fs::s3::file_name_from_key(&key).unwrap_or_else(|| entity.name.clone());
        let source_stem =
            io::fs::s3::file_stem_from_name(&source_name).unwrap_or_else(|| entity.name.clone());
        let source_uri = io::fs::s3::format_s3_uri(bucket, &key);
        inputs.push(InputFile {
            source_uri,
            local_path,
            source_name,
            source_stem,
        });
    }
    Ok(inputs)
}
