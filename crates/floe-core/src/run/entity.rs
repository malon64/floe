use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::{check, config, io, report, ConfigError, FloeResult};

use super::file::{collect_errors, read_inputs, required_columns, ReadInput};
use super::normalize::normalize_schema_columns;
use super::normalize::resolve_normalize_strategy;
use super::output::{
    append_rejection_columns, validate_rejected_target, write_accepted_output,
    write_error_report_output, write_rejected_csv_output, write_rejected_raw_output,
};
use super::reporting::{
    entity_metadata_json, project_metadata_json, source_options_json, summarize_validation,
};
use super::{EntityOutcome, RunContext, MAX_RESOLVED_INPUTS};

#[derive(Clone)]
pub(super) struct InputFile {
    pub source_uri: String,
    pub local_path: PathBuf,
    pub source_name: String,
    pub source_stem: String,
}

pub(super) enum StorageTarget {
    Local {
        base_path: String,
    },
    S3 {
        filesystem: String,
        bucket: String,
        base_key: String,
    },
}

pub(super) struct EntityRunResult {
    pub outcome: EntityOutcome,
    pub abort_run: bool,
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

    let (input_files, resolved_mode) = if source_is_s3 {
        let source_location = io::fs::s3::parse_s3_uri(&resolved_paths.source.uri)?;
        let s3_client = s3_client_for(
            s3_clients,
            &context.filesystem_resolver,
            &resolved_paths.source.filesystem,
            entity,
        )?;
        let temp_dir = temp_dir
            .as_ref()
            .ok_or_else(|| Box::new(ConfigError("s3 tempdir missing".to_string())))?;
        let inputs = build_s3_inputs(
            s3_client,
            &source_location.bucket,
            &source_location.key,
            input.format.as_str(),
            temp_dir.path(),
            entity,
            &resolved_paths.source.filesystem,
        )?;
        (inputs, report::ResolvedInputMode::Directory)
    } else {
        let resolved_inputs = io::fs::local::resolve_local_inputs(
            &context.config_dir,
            &entity.name,
            input,
            &resolved_paths.source.filesystem,
        )?;
        let inputs = build_local_inputs(&resolved_inputs.files, entity);
        let mode = match resolved_inputs.mode {
            io::fs::local::LocalInputMode::File => report::ResolvedInputMode::File,
            io::fs::local::LocalInputMode::Directory => report::ResolvedInputMode::Directory,
        };
        (inputs, mode)
    };

    let inputs = read_inputs(
        entity,
        &input_files,
        &normalized_columns,
        normalize_strategy.as_deref(),
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
    let mut abort_run = false;
    for input in inputs {
        let file_timer = Instant::now();
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
        let mismatch =
            check::apply_schema_mismatch(entity, &normalized_columns, &mut raw_df, &mut df)?;
        let row_count = raw_df.height() as u64;

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

        let (accept_rows, errors_json, error_lists) = collect_errors(
            &raw_df,
            &df,
            &required_cols,
            &normalized_columns,
            track_cast_errors,
        )?;
        let row_error_count = error_lists
            .iter()
            .filter(|errors| !errors.is_empty())
            .count() as u64;
        let violation_count = error_lists
            .iter()
            .map(|errors| errors.len() as u64)
            .sum::<u64>();
        let accept_count = accept_rows.iter().filter(|accepted| **accepted).count() as u64;
        let reject_count = row_count.saturating_sub(accept_count);
        let has_errors = row_error_count > 0;
        let mut accepted_path = None;
        let mut rejected_path = None;
        let mut errors_path = None;
        let mut archived_path = None;
        let (rules, examples) = summarize_validation(&error_lists, &normalized_columns, severity);

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
                    let rejected_target = rejected_target.as_ref().ok_or_else(|| {
                        Box::new(ConfigError(format!(
                            "entity.name={} sink.rejected.filesystem is required for rejection",
                            entity.name
                        )))
                    })?;
                    let rejected_path_value = write_rejected_csv_output(
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
        let warnings = warnings + mismatch.warnings;

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

    let report_path =
        report::ReportWriter::report_path(&context.report_dir, &context.run_id, &entity.name);
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
            path: context.report_base_path.clone(),
            report_file: report_path.display().to_string(),
        },
        policy: report::PolicyEcho { severity },
        results: totals,
        files: file_reports,
    };

    report::ReportWriter::write_report(
        &context.report_dir,
        &context.run_id,
        &entity.name,
        &run_report,
    )?;

    Ok(EntityRunResult {
        outcome: EntityOutcome {
            report: run_report,
            file_timings_ms,
        },
        abort_run,
    })
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

pub(super) fn s3_client_for<'a>(
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
    format: &str,
    temp_dir: &Path,
    entity: &config::EntityConfig,
    filesystem: &str,
) -> FloeResult<Vec<InputFile>> {
    let suffix = io::fs::s3::suffix_for_format(format)?;
    let keys = client.list_objects(bucket, prefix)?;
    let keys = io::fs::s3::filter_keys_by_suffix(keys, suffix);
    if keys.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} source.filesystem={} no input objects matched (bucket={}, prefix={}, suffix={})",
            entity.name, filesystem, bucket, prefix, suffix
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
