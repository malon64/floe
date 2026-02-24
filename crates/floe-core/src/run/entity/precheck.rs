use std::time::Instant;

use crate::checks::normalize::resolve_normalize_strategy;
use crate::{check, config, io, report, warnings, ConfigError, FloeResult};

use super::super::output::{validate_rejected_target, write_rejected_raw_output};
use super::ResolvedEntityTargets;
use crate::run::RunContext;
use io::format::InputAdapter;

pub(super) struct PrecheckedInput {
    pub(super) input_file: io::format::InputFile,
    pub(super) mismatch: check::MismatchOutcome,
    pub(super) file_timer: Instant,
}

pub(super) struct PrecheckResult {
    pub(super) prechecked: Vec<PrecheckedInput>,
    pub(super) abort_run: bool,
}

pub(super) struct PrecheckContext<'a> {
    pub(super) context: &'a RunContext,
    pub(super) entity: &'a config::EntityConfig,
    pub(super) input_adapter: &'a dyn InputAdapter,
    pub(super) normalized_columns: &'a [config::ColumnConfig],
    pub(super) resolved_targets: &'a ResolvedEntityTargets,
    pub(super) archive_target: Option<&'a io::storage::Target>,
    pub(super) temp_dir: Option<&'a tempfile::TempDir>,
    pub(super) cloud: &'a mut io::storage::CloudClient,
    pub(super) observer: &'a dyn crate::run::events::RunObserver,
    pub(super) file_reports: &'a mut Vec<report::FileReport>,
    pub(super) file_timings_ms: &'a mut Vec<Option<u64>>,
    pub(super) totals: &'a mut report::ResultsTotals,
}

pub(super) fn run_precheck(
    context: PrecheckContext<'_>,
    input_files: Vec<io::format::InputFile>,
) -> FloeResult<PrecheckResult> {
    let PrecheckContext {
        context,
        entity,
        input_adapter,
        normalized_columns,
        resolved_targets,
        archive_target,
        temp_dir,
        cloud,
        observer,
        file_reports,
        file_timings_ms,
        totals,
    } = context;
    let mut abort_run = false;
    let mut prechecked_inputs = Vec::with_capacity(input_files.len());
    let normalize_strategy = resolve_normalize_strategy(entity)?;
    let mismatch_columns =
        check::resolve_mismatch_columns(entity, normalized_columns, normalize_strategy.as_deref())?;
    for input_file in input_files {
        let file_timer = Instant::now();
        observer.on_event(crate::run::events::RunEvent::FileStarted {
            run_id: context.run_id.clone(),
            entity: entity.name.clone(),
            input: input_file.source_uri.clone(),
            ts_ms: crate::run::events::event_time_ms(),
        });
        let input_columns =
            match input_adapter.read_input_columns(entity, &input_file, normalized_columns) {
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

                    let rejected_path = resolved_targets
                        .rejected
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
                            declared_columns_count: mismatch_columns.len() as u64,
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
                    observer.on_event(crate::run::events::RunEvent::FileFinished {
                        run_id: context.run_id.clone(),
                        entity: entity.name.clone(),
                        input: input_file.source_uri.clone(),
                        status: if status == report::FileStatus::Aborted {
                            "aborted"
                        } else {
                            "rejected"
                        }
                        .to_string(),
                        rows: 0,
                        accepted: 0,
                        rejected: 0,
                        elapsed_ms: file_timer.elapsed().as_millis() as u64,
                        ts_ms: crate::run::events::event_time_ms(),
                    });

                    if status == report::FileStatus::Aborted {
                        abort_run = true;
                        break;
                    }
                    continue;
                }
            };

        let mismatch = check::plan_schema_mismatch(entity, &mismatch_columns, &input_columns)?;
        if let Some(message) = mismatch.report.warning.as_deref() {
            warnings::emit(
                &context.run_id,
                Some(&entity.name),
                Some(&input_file.source_uri),
                Some("schema_mismatch_warn_override"),
                message,
            );
        }

        if mismatch.rejected || mismatch.aborted {
            let accepted_path = None;
            let errors_path = None;
            let row_count = 0;

            validate_rejected_target(entity, if mismatch.aborted { "abort" } else { "reject" })?;
            let rejected_target = resolved_targets.rejected.as_ref().ok_or_else(|| {
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

            let archived_path = io::storage::ops::archive_input(
                cloud,
                &context.storage_resolver,
                &context.run_id,
                entity,
                archive_target,
                &input_file,
            )?;

            let status = if mismatch.aborted {
                report::FileStatus::Aborted
            } else {
                report::FileStatus::Rejected
            };
            let accepted_count = 0;
            let rejected_count = row_count;
            let mismatch_report = mismatch.report;
            let errors = mismatch.errors;
            let warnings = mismatch.warnings;

            let file_report = report::FileReport {
                input_file: input_file.source_uri.clone(),
                status,
                row_count,
                accepted_count,
                rejected_count,
                mismatch: mismatch_report,
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
            observer.on_event(crate::run::events::RunEvent::FileFinished {
                run_id: context.run_id.clone(),
                entity: entity.name.clone(),
                input: input_file.source_uri.clone(),
                status: if status == report::FileStatus::Aborted {
                    "aborted"
                } else {
                    "rejected"
                }
                .to_string(),
                rows: row_count,
                accepted: accepted_count,
                rejected: rejected_count,
                elapsed_ms: file_timer.elapsed().as_millis() as u64,
                ts_ms: crate::run::events::event_time_ms(),
            });

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

    Ok(PrecheckResult {
        prechecked: prechecked_inputs,
        abort_run,
    })
}
