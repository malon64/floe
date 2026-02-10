use std::collections::HashSet;
use std::path::Path;
use std::sync::Once;

use crate::errors::IoError;
use crate::report::build::project_metadata_json;
use crate::report::output::write_summary_report;
use crate::runtime::{DefaultRuntime, Runtime};
use crate::{config, io, report, ConfigError, FloeResult, RunOptions, ValidateOptions};

mod context;
pub(crate) mod entity;
pub mod events;
mod file;
mod output;

pub(crate) use context::RunContext;
use entity::{run_entity, EntityRunResult, ResolvedEntityTargets};
use events::{default_observer, event_time_ms, RunEvent};

pub(super) const MAX_RESOLVED_INPUTS: usize = 50;

pub(crate) struct EntityRunPlan<'a> {
    pub(crate) entity: &'a config::EntityConfig,
    pub(crate) resolved_targets: ResolvedEntityTargets,
    pub(crate) resolved_inputs: io::storage::inputs::ResolvedInputs,
    pub(crate) temp_dir: Option<tempfile::TempDir>,
}

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub run_id: String,
    pub report_base_path: Option<String>,
    pub entity_outcomes: Vec<EntityOutcome>,
    pub summary: report::RunSummaryReport,
    pub dry_run_previews: Option<Vec<DryRunEntityPreview>>,
}

#[derive(Debug, Clone)]
pub struct DryRunEntityPreview {
    pub name: String,
    pub input_path: String,
    pub input_format: String,
    pub accepted_path: String,
    pub accepted_format: String,
    pub rejected_path: Option<String>,
    pub rejected_format: Option<String>,
    pub archive_path: String,
    pub archive_storage: Option<String>,
    pub report_file: Option<String>,
    pub scanned_files: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct EntityOutcome {
    pub report: crate::report::RunReport,
    pub file_timings_ms: Vec<Option<u64>>,
}

pub(crate) fn validate_entities(
    config: &config::RootConfig,
    selected: &[String],
) -> FloeResult<()> {
    let missing: Vec<String> = selected
        .iter()
        .filter(|name| !config.entities.iter().any(|entity| &entity.name == *name))
        .cloned()
        .collect();

    if !missing.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entities not found: {}",
            missing.join(", ")
        ))));
    }
    Ok(())
}

pub fn run(config_path: &Path, options: RunOptions) -> FloeResult<RunOutcome> {
    let config_base = config::ConfigBase::local_from_path(config_path);
    run_with_base(config_path, config_base, options)
}

pub fn run_with_base(
    config_path: &Path,
    config_base: config::ConfigBase,
    options: RunOptions,
) -> FloeResult<RunOutcome> {
    let mut runtime = DefaultRuntime::new();
    run_with_runtime(config_path, config_base, options, &mut runtime)
}

pub fn run_with_runtime(
    config_path: &Path,
    config_base: config::ConfigBase,
    options: RunOptions,
    runtime: &mut dyn Runtime,
) -> FloeResult<RunOutcome> {
    init_thread_pool();
    let validate_options = ValidateOptions {
        entities: options.entities.clone(),
    };
    crate::validate_with_base(config_path, config_base.clone(), validate_options)?;

    let context = RunContext::new(config_path, config_base, &options)?;
    if !options.entities.is_empty() {
        validate_entities(&context.config, &options.entities)?;
    }

    let selected_entities = select_entities(&context, &options);
    let resolution_mode = if options.dry_run {
        io::storage::inputs::ResolveInputsMode::ListOnly
    } else {
        io::storage::inputs::ResolveInputsMode::Download
    };
    let plans = resolve_entity_plans(&context, runtime, &selected_entities, resolution_mode)?;
    if options.dry_run {
        return create_dry_run_outcome(&context, plans);
    }

    let mut entity_outcomes = Vec::new();
    let mut abort_run = false;
    let observer = default_observer();
    observer.on_event(RunEvent::RunStarted {
        run_id: context.run_id.clone(),
        config: context.config_path.display().to_string(),
        report_base: context.report_base_path.clone(),
        ts_ms: event_time_ms(),
    });

    for plan in plans {
        observer.on_event(RunEvent::EntityStarted {
            run_id: context.run_id.clone(),
            name: plan.entity.name.clone(),
            ts_ms: event_time_ms(),
        });
        let EntityRunResult {
            outcome,
            abort_run: aborted,
        } = run_entity(&context, runtime, observer, plan)?;
        let report = &outcome.report;
        let (mut status, _) = report::compute_run_outcome(
            &report
                .files
                .iter()
                .map(|file| file.status)
                .collect::<Vec<_>>(),
        );
        if status == report::RunStatus::Success && report.results.warnings_total > 0 {
            status = report::RunStatus::SuccessWithWarnings;
        }
        observer.on_event(RunEvent::EntityFinished {
            run_id: context.run_id.clone(),
            name: report.entity.name.clone(),
            status: run_status_str(status).to_string(),
            files: report.results.files_total,
            rows: report.results.rows_total,
            accepted: report.results.accepted_total,
            rejected: report.results.rejected_total,
            warnings: report.results.warnings_total,
            errors: report.results.errors_total,
            ts_ms: event_time_ms(),
        });
        entity_outcomes.push(outcome);
        abort_run = abort_run || aborted;
        if abort_run {
            break;
        }
    }
    let summary = build_run_summary(&context, &entity_outcomes);
    if let Some(report_target) = &context.report_target {
        write_summary_report(
            report_target,
            &context.run_id,
            &summary,
            runtime.storage(),
            &context.storage_resolver,
        )?;
    }
    observer.on_event(RunEvent::RunFinished {
        run_id: context.run_id.clone(),
        status: run_status_str(summary.run.status).to_string(),
        exit_code: summary.run.exit_code,
        files: summary.results.files_total,
        rows: summary.results.rows_total,
        accepted: summary.results.accepted_total,
        rejected: summary.results.rejected_total,
        warnings: summary.results.warnings_total,
        errors: summary.results.errors_total,
        summary_uri: context.report_target.as_ref().map(|target| {
            target.join_relative(&report::ReportWriter::summary_relative_path(
                &context.run_id,
            ))
        }),
        ts_ms: event_time_ms(),
    });

    Ok(RunOutcome {
        run_id: context.run_id.clone(),
        report_base_path: context.report_base_path.clone(),
        entity_outcomes,
        summary,
        dry_run_previews: None,
    })
}

fn init_thread_pool() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        if std::env::var("RAYON_NUM_THREADS").is_ok() {
            return;
        }
        let cap = std::env::var("FLOE_MAX_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4);
        let available = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        let threads = available.min(cap).max(1);
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global();
    });
}

fn select_entities<'a>(
    context: &'a RunContext,
    options: &RunOptions,
) -> Vec<&'a config::EntityConfig> {
    if options.entities.is_empty() {
        context.config.entities.iter().collect()
    } else {
        let selected: HashSet<&str> = options.entities.iter().map(|s| s.as_str()).collect();
        context
            .config
            .entities
            .iter()
            .filter(|entity| selected.contains(entity.name.as_str()))
            .collect()
    }
}

fn resolve_entity_plans<'a>(
    context: &'a RunContext,
    runtime: &mut dyn Runtime,
    entities: &[&'a config::EntityConfig],
    mode: io::storage::inputs::ResolveInputsMode,
) -> FloeResult<Vec<EntityRunPlan<'a>>> {
    let mut plans = Vec::with_capacity(entities.len());
    for entity in entities {
        let input_adapter = runtime.input_adapter(entity.source.format.as_str())?;
        let resolved_targets = entity::resolve_entity_targets(&context.storage_resolver, entity)?;
        let needs_temp = matches!(mode, io::storage::inputs::ResolveInputsMode::Download)
            && (resolved_targets.source.is_remote()
                || resolved_targets.accepted.is_remote()
                || resolved_targets
                    .rejected
                    .as_ref()
                    .is_some_and(io::storage::Target::is_remote));
        let temp_dir = if needs_temp {
            Some(
                tempfile::TempDir::new()
                    .map_err(|err| Box::new(IoError(format!("tempdir failed: {err}"))))?,
            )
        } else {
            None
        };
        let storage_client = Some(runtime.storage().client_for(
            &context.storage_resolver,
            resolved_targets.source.storage(),
            entity,
        )? as &dyn io::storage::StorageClient);
        let resolved_inputs = io::storage::ops::resolve_inputs(
            &context.config_dir,
            entity,
            input_adapter,
            &resolved_targets.source,
            mode,
            temp_dir.as_ref().map(|dir| dir.path()),
            storage_client,
        )?;
        plans.push(EntityRunPlan {
            entity,
            resolved_targets,
            resolved_inputs,
            temp_dir,
        });
    }
    Ok(plans)
}

fn build_run_summary(
    context: &RunContext,
    entity_outcomes: &[EntityOutcome],
) -> report::RunSummaryReport {
    let mut totals = report::ResultsTotals {
        files_total: 0,
        rows_total: 0,
        accepted_total: 0,
        rejected_total: 0,
        warnings_total: 0,
        errors_total: 0,
    };
    let mut statuses = Vec::new();
    let mut entities = Vec::with_capacity(entity_outcomes.len());

    for outcome in entity_outcomes {
        let report = &outcome.report;
        totals.files_total += report.results.files_total;
        totals.rows_total += report.results.rows_total;
        totals.accepted_total += report.results.accepted_total;
        totals.rejected_total += report.results.rejected_total;
        totals.warnings_total += report.results.warnings_total;
        totals.errors_total += report.results.errors_total;

        let file_statuses = report
            .files
            .iter()
            .map(|file| file.status)
            .collect::<Vec<_>>();
        let (mut status, _) = report::compute_run_outcome(&file_statuses);
        if status == report::RunStatus::Success && report.results.warnings_total > 0 {
            status = report::RunStatus::SuccessWithWarnings;
        }
        statuses.extend(file_statuses);

        let report_file = context
            .report_target
            .as_ref()
            .map(|target| {
                target.join_relative(&report::ReportWriter::report_relative_path(
                    &context.run_id,
                    &report.entity.name,
                ))
            })
            .unwrap_or_else(|| "disabled".to_string());
        entities.push(report::EntitySummary {
            name: report.entity.name.clone(),
            status,
            results: report.results.clone(),
            report_file,
        });
    }

    let (mut status, exit_code) = report::compute_run_outcome(&statuses);
    if status == report::RunStatus::Success && totals.warnings_total > 0 {
        status = report::RunStatus::SuccessWithWarnings;
    }

    let finished_at = report::now_rfc3339();
    let duration_ms = context.run_timer.elapsed().as_millis() as u64;
    let report_base_path = context
        .report_base_path
        .clone()
        .unwrap_or_else(|| "disabled".to_string());
    let report_file = context
        .report_target
        .as_ref()
        .map(|target| {
            target.join_relative(&report::ReportWriter::summary_relative_path(
                &context.run_id,
            ))
        })
        .unwrap_or_else(|| "disabled".to_string());

    report::RunSummaryReport {
        spec_version: context.config.version.clone(),
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
            status,
            exit_code,
        },
        config: report::ConfigEcho {
            path: context.config_path.display().to_string(),
            version: context.config.version.clone(),
            metadata: context.config.metadata.as_ref().map(project_metadata_json),
        },
        report: report::ReportEcho {
            path: report_base_path,
            report_file,
        },
        results: totals,
        entities,
    }
}

fn create_dry_run_outcome(
    context: &RunContext,
    plans: Vec<EntityRunPlan<'_>>,
) -> FloeResult<RunOutcome> {
    let mut previews: Vec<DryRunEntityPreview> = Vec::new();

    for plan in plans {
        let entity = plan.entity;
        let rejected_path = entity.sink.rejected.as_ref().map(|r| r.path.clone());
        let rejected_format = entity.sink.rejected.as_ref().map(|r| r.format.clone());
        let (archive_path, archive_storage) = entity
            .sink
            .archive
            .as_ref()
            .map(|a| (a.path.clone(), a.storage.clone()))
            .unwrap_or_else(|| (String::new(), None));

        let report_file = context.report_target.as_ref().map(|target| {
            target.join_relative(&report::ReportWriter::report_relative_path(
                &context.run_id,
                &entity.name,
            ))
        });

        previews.push(DryRunEntityPreview {
            name: entity.name.clone(),
            input_path: entity.source.path.clone(),
            input_format: entity.source.format.clone(),
            accepted_path: entity.sink.accepted.path.clone(),
            accepted_format: entity.sink.accepted.format.clone(),
            rejected_path,
            rejected_format,
            archive_path,
            archive_storage,
            report_file,
            scanned_files: plan.resolved_inputs.listed,
        });
    }

    Ok(RunOutcome {
        run_id: context.run_id.clone(),
        report_base_path: context.report_base_path.clone(),
        entity_outcomes: Vec::new(),
        summary: report::RunSummaryReport {
            spec_version: context.config.version.clone(),
            tool: report::ToolInfo {
                name: "floe".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                git: None,
            },
            run: report::RunInfo {
                run_id: context.run_id.clone(),
                started_at: context.started_at.clone(),
                finished_at: report::now_rfc3339(),
                duration_ms: 0,
                status: report::RunStatus::Success,
                exit_code: 0,
            },
            config: report::ConfigEcho {
                path: context.config_path.display().to_string(),
                version: context.config.version.clone(),
                metadata: context.config.metadata.as_ref().map(project_metadata_json),
            },
            report: report::ReportEcho {
                path: context
                    .report_base_path
                    .clone()
                    .unwrap_or_else(|| "disabled".to_string()),
                report_file: "disabled (dry-run)".to_string(),
            },
            results: report::ResultsTotals {
                files_total: 0,
                rows_total: 0,
                accepted_total: 0,
                rejected_total: 0,
                warnings_total: 0,
                errors_total: 0,
            },
            entities: Vec::new(),
        },
        dry_run_previews: Some(previews),
    })
}

fn run_status_str(status: report::RunStatus) -> &'static str {
    match status {
        report::RunStatus::Success => "success",
        report::RunStatus::SuccessWithWarnings => "success_with_warnings",
        report::RunStatus::Rejected => "rejected",
        report::RunStatus::Aborted => "aborted",
        report::RunStatus::Failed => "failed",
    }
}
