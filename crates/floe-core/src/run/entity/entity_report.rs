use crate::{config, report};

use super::resolve::ResolvedEntityTargets;
use crate::run::reporting::{entity_metadata_json, source_options_json};
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
    pub accepted_parts_written: u64,
}

pub(super) fn build_run_report(ctx: RunReportContext<'_>) -> report::RunReport {
    report::RunReport {
        spec_version: ctx.context.config.version.clone(),
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
        policy: report::PolicyEcho {
            severity: ctx.severity,
        },
        accepted_output: report::AcceptedOutputSummary {
            path: ctx.resolved_targets.accepted.target_uri().to_string(),
            accepted_rows: ctx.totals.accepted_total,
            parts_written: ctx.accepted_parts_written,
        },
        results: ctx.totals,
        files: ctx.file_reports,
    }
}
