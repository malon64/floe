use crate::{config, report};

use super::resolve::ResolvedEntityTargets;
use crate::run::reporting::{entity_metadata_json, project_metadata_json, source_options_json};
use crate::run::RunContext;

pub(super) fn build_run_report(
    context: &RunContext,
    entity: &config::EntityConfig,
    input: &config::SourceConfig,
    resolved_targets: &ResolvedEntityTargets,
    resolved_mode: report::ResolvedInputMode,
    resolved_files: &[String],
    reported_files: Vec<String>,
    totals: report::ResultsTotals,
    file_reports: Vec<report::FileReport>,
    severity: report::Severity,
    run_status: report::RunStatus,
    exit_code: i32,
) -> report::RunReport {
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

    report::RunReport {
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
            status: run_status,
            exit_code,
        },
        config: report::ConfigEcho {
            path: context.config_path.display().to_string(),
            version: context.config.version.clone(),
            metadata: context.config.metadata.as_ref().map(project_metadata_json),
        },
        entity: report::EntityEcho {
            name: entity.name.clone(),
            metadata: entity.metadata.as_ref().map(entity_metadata_json),
        },
        source: report::SourceEcho {
            format: input.format.clone(),
            path: resolved_targets.source.uri().to_string(),
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
                path: resolved_targets.accepted.uri().to_string(),
            },
            rejected: entity
                .sink
                .rejected
                .as_ref()
                .map(|rejected| report::SinkTargetEcho {
                    format: rejected.format.clone(),
                    path: resolved_targets
                        .rejected
                        .as_ref()
                        .map(|target| target.uri().to_string())
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
    }
}
