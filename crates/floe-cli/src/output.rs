use std::path::Path;

use floe_core::{report, EntityOutcome, RunOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Default,
    Quiet,
    Verbose,
}

pub fn format_run_output(outcome: &RunOutcome, mode: OutputMode) -> String {
    let mut lines = Vec::new();

    if mode != OutputMode::Quiet {
        lines.push(format!("run id: {}", &outcome.run_id));
        lines.push(format!(
            "report base: {}",
            outcome.report_base_path.as_deref().unwrap_or("(disabled)")
        ));
    }

    if mode != OutputMode::Quiet {
        for entity in &outcome.entity_outcomes {
            lines.extend(format_entity_output(entity, mode));
        }
    }

    lines.extend(format_run_summary(outcome, mode == OutputMode::Quiet));

    lines.join("\n")
}

fn format_entity_output(entity: &EntityOutcome, mode: OutputMode) -> Vec<String> {
    let mut lines = Vec::new();
    let report = &entity.report;
    lines.push(format!(
        "==> entity {} (severity={}, format={})",
        &report.entity.name,
        format_severity(report.policy.severity),
        &report.source.format
    ));

    if mode == OutputMode::Verbose {
        lines.push(format!(
            "  source: {} ({})",
            &report.source.format, &report.source.path
        ));
        lines.push(format!(
            "  sink accepted: {} ({})",
            &report.sink.accepted.format, &report.sink.accepted.path
        ));
        if let Some(rejected) = &report.sink.rejected {
            lines.push(format!(
                "  sink rejected: {} ({})",
                &rejected.format, &rejected.path
            ));
        }
        if let Some(cast_mode) = &report.source.cast_mode {
            lines.push(format!("  cast_mode: {}", cast_mode));
        }
        if !report.source.resolved_inputs.files.is_empty() {
            lines.push("  inputs:".to_string());
            for file in &report.source.resolved_inputs.files {
                lines.push(format!("    {}", file));
            }
        }
    }

    for (file_report, elapsed_ms) in report.files.iter().zip(entity.file_timings_ms.iter()) {
        lines.push(format_file_line(file_report, *elapsed_ms));
    }

    if report.files.len() > entity.file_timings_ms.len() {
        for file_report in report.files.iter().skip(entity.file_timings_ms.len()) {
            lines.push(format_file_line(file_report, None));
        }
    }

    lines
}

fn format_file_line(file: &report::FileReport, elapsed_ms: Option<u64>) -> String {
    let mut line = format!(
        "  {} {} rows={} accepted={} rejected={}",
        format_file_status(file.status),
        short_path(&file.input_file),
        file.row_count,
        file.accepted_count,
        file.rejected_count
    );
    if let Some(ms) = elapsed_ms {
        line.push_str(&format!(" elapsed_ms={ms}"));
    }
    line.push_str(&format!(
        " accepted_out={} rejected_out={}",
        short_optional_path(&file.output.accepted_path),
        short_optional_path(&file.output.rejected_path)
    ));
    line
}

fn format_run_summary(outcome: &RunOutcome, include_run_info: bool) -> Vec<String> {
    let mut lines = Vec::new();
    if include_run_info {
        lines.push(format!("run id: {}", &outcome.run_id));
        lines.push(format!(
            "report base: {}",
            outcome.report_base_path.as_deref().unwrap_or("(disabled)")
        ));
    }
    lines.push(format!(
        "Totals: files={} rows={} accepted={} rejected={}",
        outcome.summary.results.files_total,
        outcome.summary.results.rows_total,
        outcome.summary.results.accepted_total,
        outcome.summary.results.rejected_total
    ));
    lines.push(format!(
        "Overall: {} (exit_code={})",
        format_run_status(outcome.summary.run.status),
        outcome.summary.run.exit_code
    ));
    lines.push(format!(
        "Run summary: {}",
        run_summary_path(&outcome.run_id, outcome.report_base_path.as_deref())
    ));
    lines
}

fn run_summary_path(run_id: &str, report_base_path: Option<&str>) -> String {
    let Some(report_base_path) = report_base_path else {
        return "(disabled)".to_string();
    };
    let run_dir = report::ReportWriter::run_dir_name(run_id);
    Path::new(report_base_path)
        .join(run_dir)
        .join("run.summary.json")
        .display()
        .to_string()
}

fn format_file_status(status: report::FileStatus) -> &'static str {
    match status {
        report::FileStatus::Success => "SUCCESS",
        report::FileStatus::Rejected => "REJECTED",
        report::FileStatus::Aborted => "ABORTED",
        report::FileStatus::Failed => "FAILED",
    }
}

fn format_run_status(status: report::RunStatus) -> &'static str {
    match status {
        report::RunStatus::Success => "success",
        report::RunStatus::SuccessWithWarnings => "success_with_warnings",
        report::RunStatus::Rejected => "rejected",
        report::RunStatus::Aborted => "aborted",
        report::RunStatus::Failed => "failed",
    }
}

fn format_severity(severity: report::Severity) -> &'static str {
    match severity {
        report::Severity::Warn => "warn",
        report::Severity::Reject => "reject",
        report::Severity::Abort => "abort",
    }
}

fn short_optional_path(path: &Option<String>) -> String {
    path.as_deref()
        .map(short_path)
        .unwrap_or_else(|| "-".to_string())
}

fn short_path(path: &str) -> String {
    let trimmed = path.trim_end_matches(std::path::MAIN_SEPARATOR);
    Path::new(trimmed)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(trimmed)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_outcome() -> RunOutcome {
        let run_id = "run-123".to_string();
        let report_base_path = "/tmp/reports".to_string();
        let report_file = "/tmp/reports/run_run-123/customer/run.json".to_string();
        let version = env!("CARGO_PKG_VERSION").to_string();

        let report = report::RunReport {
            spec_version: version.clone(),
            entity: report::EntityEcho {
                name: "customer".to_string(),
                metadata: None,
            },
            source: report::SourceEcho {
                format: "csv".to_string(),
                path: "/tmp/input".to_string(),
                options: None,
                cast_mode: None,
                read_plan: report::SourceReadPlan::RawAndTyped,
                resolved_inputs: report::ResolvedInputs {
                    mode: report::ResolvedInputMode::Directory,
                    file_count: 1,
                    files: vec!["/tmp/input/customers.csv".to_string()],
                },
            },
            sink: report::SinkEcho {
                accepted: report::SinkTargetEcho {
                    format: "parquet".to_string(),
                    path: "/tmp/out/accepted".to_string(),
                },
                rejected: Some(report::SinkTargetEcho {
                    format: "csv".to_string(),
                    path: "/tmp/out/rejected".to_string(),
                }),
                archive: report::SinkArchiveEcho {
                    enabled: false,
                    path: None,
                },
            },
            policy: report::PolicyEcho {
                severity: report::Severity::Reject,
            },
            accepted_output: report::AcceptedOutputSummary {
                path: "/tmp/out/accepted".to_string(),
                accepted_rows: 8,
                parts_written: 1,
            },
            results: report::ResultsTotals {
                files_total: 1,
                rows_total: 10,
                accepted_total: 8,
                rejected_total: 2,
                warnings_total: 0,
                errors_total: 0,
            },
            files: vec![report::FileReport {
                input_file: "/tmp/input/customers.csv".to_string(),
                status: report::FileStatus::Rejected,
                row_count: 10,
                accepted_count: 8,
                rejected_count: 2,
                mismatch: report::FileMismatch {
                    declared_columns_count: 2,
                    input_columns_count: 2,
                    missing_columns: Vec::new(),
                    extra_columns: Vec::new(),
                    mismatch_action: report::MismatchAction::None,
                    error: None,
                    warning: None,
                },
                output: report::FileOutput {
                    accepted_path: Some("/tmp/out/accepted".to_string()),
                    rejected_path: Some("/tmp/out/rejected/customers.rejected.csv".to_string()),
                    errors_path: None,
                    archived_path: None,
                },
                validation: report::FileValidation {
                    errors: 1,
                    warnings: 0,
                    rules: Vec::new(),
                },
            }],
        };

        let summary = report::RunSummaryReport {
            spec_version: version.clone(),
            tool: report::ToolInfo {
                name: "floe".to_string(),
                version: version.clone(),
                git: None,
            },
            run: report::RunInfo {
                run_id: run_id.clone(),
                started_at: "2026-01-19T10-23-45Z".to_string(),
                finished_at: "2026-01-19T10-23-46Z".to_string(),
                duration_ms: 1000,
                status: report::RunStatus::Rejected,
                exit_code: 0,
            },
            config: report::ConfigEcho {
                path: "/tmp/config.yml".to_string(),
                version,
                metadata: None,
            },
            report: report::ReportEcho {
                path: report_base_path.clone(),
                report_file,
            },
            results: report::ResultsTotals {
                files_total: 1,
                rows_total: 10,
                accepted_total: 8,
                rejected_total: 2,
                warnings_total: 0,
                errors_total: 0,
            },
            entities: vec![report::EntitySummary {
                name: "customer".to_string(),
                status: report::RunStatus::Rejected,
                results: report::ResultsTotals {
                    files_total: 1,
                    rows_total: 10,
                    accepted_total: 8,
                    rejected_total: 2,
                    warnings_total: 0,
                    errors_total: 0,
                },
                report_file: "/tmp/reports/run_run-123/customer/run.json".to_string(),
            }],
        };

        RunOutcome {
            run_id,
            report_base_path: Some(report_base_path),
            entity_outcomes: vec![EntityOutcome {
                report,
                file_timings_ms: vec![Some(12)],
            }],
            summary,
        }
    }

    #[test]
    fn format_run_output_default_mode() {
        let outcome = sample_outcome();
        let output = format_run_output(&outcome, OutputMode::Default);
        let expected = [
            "run id: run-123",
            "report base: /tmp/reports",
            "==> entity customer (severity=reject, format=csv)",
            "  REJECTED customers.csv rows=10 accepted=8 rejected=2 elapsed_ms=12 accepted_out=accepted rejected_out=customers.rejected.csv",
            "Totals: files=1 rows=10 accepted=8 rejected=2",
            "Overall: rejected (exit_code=0)",
            "Run summary: /tmp/reports/run_run-123/run.summary.json",
        ]
        .join("\n");

        assert_eq!(output, expected);
    }
}
