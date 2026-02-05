use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Once;

use crate::io::storage::CloudClient;
use crate::report::build::project_metadata_json;
use crate::report::output::write_summary_report;
use crate::{config, report, ConfigError, FloeResult, RunOptions, ValidateOptions};

mod context;
pub(crate) mod entity;
pub mod events;
mod file;
pub mod normalize;
mod output;

pub(crate) use context::RunContext;
use entity::{run_entity, EntityRunResult};
use events::{default_observer, event_time_ms, RunEvent};

pub(super) const MAX_RESOLVED_INPUTS: usize = 50;

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub run_id: String,
    pub report_base_path: Option<String>,
    pub entity_outcomes: Vec<EntityOutcome>,
    pub summary: report::RunSummaryReport,
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
    init_thread_pool();
    let validate_options = ValidateOptions {
        entities: options.entities.clone(),
    };
    crate::validate_with_base(config_path, config_base.clone(), validate_options)?;

    let context = RunContext::new(config_path, config_base, &options)?;
    if !options.entities.is_empty() {
        validate_entities(&context.config, &options.entities)?;
    }
    if options.dry_run {
        return Ok(create_dry_run_outcome(&context));
    }

    let mut entity_outcomes = Vec::new();
    let mut abort_run = false;
    let mut cloud = CloudClient::new();
    let observer = default_observer();
    observer.on_event(RunEvent::RunStarted {
        run_id: context.run_id.clone(),
        config: context.config_path.display().to_string(),
        report_base: context.report_base_path.clone(),
        ts_ms: event_time_ms(),
    });

    let selected_entities: Vec<&config::EntityConfig> = if options.entities.is_empty() {
        context.config.entities.iter().collect()
    } else {
        let selected: HashSet<&str> = options.entities.iter().map(|s| s.as_str()).collect();
        context
            .config
            .entities
            .iter()
            .filter(|entity| selected.contains(entity.name.as_str()))
            .collect()
    };

    for entity in selected_entities {
        observer.on_event(RunEvent::EntityStarted {
            run_id: context.run_id.clone(),
            name: entity.name.clone(),
            ts_ms: event_time_ms(),
        });
        let EntityRunResult {
            outcome,
            abort_run: aborted,
        } = run_entity(&context, &mut cloud, observer, entity)?;
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
            name: entity.name.clone(),
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
            &mut cloud,
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

// TODO keep for output reference, will be removed
#[allow(dead_code)]
fn print_dry_run_summary(context: &RunContext) -> FloeResult<()> {
    println!("DRY RUN MODE - No data will be processed\n");
    println!("Run ID: {}", context.run_id);
    println!("Config: {}", context.config_path.display());
    println!("Config Dir: {}", context.config_dir.display());
    println!("Entities: {}", context.config.entities.len());

    for entity in &context.config.entities {
        println!("\nEntity: {}", entity.name);
        // Input
        println!("  Input: {} ({})", entity.source.path, entity.source.format);
        println!(
            "  Output: {} ({})",
            entity.sink.accepted.path, entity.sink.accepted.format
        );
        // Accepted Output
        println!(
            "  Accepted Output: {} ({})",
            entity.sink.accepted.path, entity.sink.accepted.format
        );
        // Rejected Output
        if let Some(rejected) = &entity.sink.rejected {
            println!("  Rejected Output: {} ({})", rejected.path, rejected.format);
        } else {
            println!("  Rejected Output: None");
        }
        // Archive Path (if configured)
        if let Some(archive) = &entity.sink.archive {
            println!(
                "  Archive Path: {} ({})",
                archive.path,
                archive.storage.as_deref().unwrap_or("default")
            );
        }
        // Report path
        if let Some(report_target) = &context.report_target {
            let report_path = report_target.join_relative(
                &report::ReportWriter::report_relative_path(&context.run_id, &entity.name),
            );
            println!("  Report: {}", report_path);
        }
        // Scanned files
        println!("  Scanned Files:");
        match scan_input_files(&entity.source) {
            Ok(files) => {
                for file in files {
                    println!("   - {}", file.display());
                }
            }
            Err(e) => {
                println!("   (error scanning: {})", e);
            }
        }
        println!();
    }
    // Report base path
    if let Some(report_base) = &context.report_base_path {
        println!("Report Path: {}\n", report_base);
    }
    // Summary
    println!("Overall: success (exit_code=0)");
    if let Some(report_base) = &context.report_base_path {
        println!(
            "Run summary: {}/run.summary.json",
            report_base.trim_end_matches('/')
        );
    }
    Ok(())
}

// TODO keep for output reference, will be removed
#[allow(dead_code)]
fn scan_input_files(source: &config::SourceConfig) -> FloeResult<Vec<PathBuf>> {
    use std::fs;

    let path = Path::new(&source.path);
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if path.is_dir() {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            }
        }
        files.sort();
        return Ok(files);
    }
    Ok(Vec::new())
}

fn create_dry_run_outcome(context: &RunContext) -> RunOutcome {
    RunOutcome {
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
    }
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
