use std::path::Path;

use crate::io::storage::CloudClient;
use crate::report::build::project_metadata_json;
use crate::report::output::write_summary_report;
use crate::{config, report, ConfigError, FloeResult, RunOptions, ValidateOptions};

mod context;
pub(crate) mod entity;
mod file;
pub mod normalize;
mod output;

use context::RunContext;
use entity::{run_entity, EntityRunResult};

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
    let validate_options = ValidateOptions {
        entities: options.entities.clone(),
    };
    crate::validate(config_path, validate_options)?;

    let context = RunContext::new(config_path, &options)?;
    if !options.entities.is_empty() {
        validate_entities(&context.config, &options.entities)?;
    }

    let mut entity_outcomes = Vec::new();
    let mut abort_run = false;
    let mut cloud = CloudClient::new();
    for entity in &context.config.entities {
        let EntityRunResult {
            outcome,
            abort_run: aborted,
        } = run_entity(&context, &mut cloud, entity)?;
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

    Ok(RunOutcome {
        run_id: context.run_id.clone(),
        report_base_path: context.report_base_path.clone(),
        entity_outcomes,
        summary,
    })
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
