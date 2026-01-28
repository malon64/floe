use crate::{config, report};

use super::resolve::ResolvedEntityTargets;
use crate::run::reporting::{entity_metadata_json, project_metadata_json, source_options_json};
use crate::run::RunContext;
pub(super) struct RunReportContext<'a> {
    pub context: &'a RunContext,
    pub entity: &'a config::EntityConfig,
    pub input: &'a config::SourceConfig,
    pub resolved_targets: &'a ResolvedEntityTargets,
    pub resolved_mode: report::ResolvedInputMode,
    pub resolved_files: &'a [String],
    pub reported_files: Vec<String>,
    pub totals: report::ResultsTotals,
    pub file_reports: Vec<report::FileReport>,
    pub severity: report::Severity,
    pub run_status: report::RunStatus,
    pub exit_code: i32,
}

pub(super) fn build_run_report(ctx: RunReportContext<'_>) -> report::RunReport {
    let report_path =
        ctx.context.report_dir.as_ref().map(|dir| {
            report::ReportWriter::report_path(dir, &ctx.context.run_id, &ctx.entity.name)
        });
    let report_base_path = ctx
        .context
        .report_base_path
        .clone()
        .unwrap_or_else(|| "disabled".to_string());
    let report_file_path = report_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "disabled".to_string());
    let finished_at = report::now_rfc3339();
    let duration_ms = ctx.context.run_timer.elapsed().as_millis() as u64;

    report::RunReport {
        spec_version: ctx.context.config.version.clone(),
        tool: report::ToolInfo {
            name: "floe".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git: None,
        },
        run: report::RunInfo {
            run_id: ctx.context.run_id.clone(),
            started_at: ctx.context.started_at.clone(),
            finished_at,
            duration_ms,
            status: ctx.run_status,
            exit_code: ctx.exit_code,
        },
        config: report::ConfigEcho {
            path: ctx.context.config_path.display().to_string(),
            version: ctx.context.config.version.clone(),
            metadata: ctx
                .context
                .config
                .metadata
                .as_ref()
                .map(project_metadata_json),
        },
        entity: report::EntityEcho {
            name: ctx.entity.name.clone(),
            metadata: ctx.entity.metadata.as_ref().map(entity_metadata_json),
        },
        source: report::SourceEcho {
            format: ctx.input.format.clone(),
            path: ctx.resolved_targets.source.target_uri().to_string(),
            options: ctx.input.options.as_ref().map(source_options_json),
            cast_mode: ctx.input.cast_mode.clone(),
            read_plan: report::SourceReadPlan::RawAndTyped,
            resolved_inputs: report::ResolvedInputs {
                mode: ctx.resolved_mode,
                file_count: ctx.resolved_files.len() as u64,
                files: ctx.reported_files,
            },
        },
        sink: report::SinkEcho {
            accepted: report::SinkTargetEcho {
                format: ctx.entity.sink.accepted.format.clone(),
                path: ctx.resolved_targets.accepted.target_uri().to_string(),
            },
            rejected: ctx
                .entity
                .sink
                .rejected
                .as_ref()
                .map(|rejected| report::SinkTargetEcho {
                    format: rejected.format.clone(),
                    path: ctx
                        .resolved_targets
                        .rejected
                        .as_ref()
                        .map(|target| target.target_uri().to_string())
                        .unwrap_or_else(|| rejected.path.clone()),
                }),
            archive: report::SinkArchiveEcho {
                enabled: ctx.entity.sink.archive.is_some(),
                path: ctx
                    .entity
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
        policy: report::PolicyEcho {
            severity: ctx.severity,
        },
        results: ctx.totals,
        files: ctx.file_reports,
    }
}
