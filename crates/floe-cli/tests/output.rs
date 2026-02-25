#[path = "../src/output.rs"]
mod output;

use floe_core::{report, DryRunEntityPreview, EntityOutcome, RunOutcome};

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
            table_root_uri: Some("/tmp/out/accepted".to_string()),
            write_mode: Some("overwrite".to_string()),
            accepted_rows: 8,
            files_written: 1,
            parts_written: 1,
            part_files: vec!["part-00000.parquet".to_string()],
            table_version: None,
            snapshot_id: None,
            iceberg_catalog_name: None,
            iceberg_database: None,
            iceberg_namespace: None,
            iceberg_table: None,
            total_bytes_written: None,
            avg_file_size_mb: None,
            small_files_count: None,
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
        dry_run_previews: None,
    }
}

fn sample_dry_run_outcome() -> RunOutcome {
    let run_id = "run-456".to_string();
    let report_base_path = "/tmp/reports-dry".to_string();
    let version = env!("CARGO_PKG_VERSION").to_string();

    let summary = report::RunSummaryReport {
        spec_version: version.clone(),
        tool: report::ToolInfo {
            name: "floe".to_string(),
            version: version.clone(),
            git: None,
        },
        run: report::RunInfo {
            run_id: run_id.clone(),
            started_at: "2026-01-19T11-00-00Z".to_string(),
            finished_at: "2026-01-19T11-00-01Z".to_string(),
            duration_ms: 0,
            status: report::RunStatus::Success,
            exit_code: 0,
        },
        config: report::ConfigEcho {
            path: "/tmp/config.yml".to_string(),
            version,
            metadata: None,
        },
        report: report::ReportEcho {
            path: report_base_path.clone(),
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
    };

    let previews = vec![DryRunEntityPreview {
        name: "orders".to_string(),
        input_path: "/tmp/in/orders".to_string(),
        input_format: "json".to_string(),
        accepted_path: "/tmp/out/orders.parquet".to_string(),
        accepted_format: "parquet".to_string(),
        rejected_path: Some("/tmp/out/orders.rejected".to_string()),
        rejected_format: Some("csv".to_string()),
        archive_path: "/tmp/archive/orders".to_string(),
        archive_storage: Some("s3".to_string()),
        report_file: Some("/tmp/reports-dry/run_run-456/orders/run.json".to_string()),
        scanned_files: vec!["/tmp/in/orders/2026-01-01.json".to_string()],
    }];

    RunOutcome {
        run_id,
        report_base_path: Some(report_base_path),
        entity_outcomes: Vec::new(),
        summary,
        dry_run_previews: Some(previews),
    }
}

#[test]
fn format_run_output_default_mode() {
    let outcome = sample_outcome();
    let output = output::format_run_output(&outcome, output::OutputMode::Default, false);
    let expected = [
        "run id: run-123",
        "report base: /tmp/reports",
        "",
        "==> entity customer (severity=reject, format=csv)",
        "  REJECTED customers.csv rows=10 accepted=8 rejected=2 elapsed_ms=12 accepted_out=accepted rejected_out=customers.rejected.csv",
        "",
        "Totals: files=1 rows=10 accepted=8 rejected=2",
        "Overall: rejected (exit_code=0)",
        "Run summary: /tmp/reports/run_run-123/run.summary.json",
    ]
    .join("\n");

    assert_eq!(output, expected);
}

#[test]
fn format_run_output_dry_run_default_mode() {
    let outcome = sample_dry_run_outcome();
    let output = output::format_run_output(&outcome, output::OutputMode::Default, true);
    let expected = [
        "DRY RUN MODE - No actual execution performed",
        "run id: run-456",
        "report base: /tmp/reports-dry",
        "",
        "==> entity orders (format=json)",
        "  Input: /tmp/in/orders (json)",
        "  Accepted Output: /tmp/out/orders.parquet (parquet)",
        "  Rejected Output: /tmp/out/orders.rejected (csv)",
        "  Archive Path: /tmp/archive/orders (s3)",
        "  Resolved Inputs: 1",
        "  Resolved Files:",
        "    /tmp/in/orders/2026-01-01.json",
        "",
        "Totals: files=0 rows=0 accepted=0 rejected=0",
        "Overall: success (exit_code=0)",
        "Run summary: /tmp/reports-dry/run_run-456/run.summary.json",
    ]
    .join("\n");

    assert_eq!(output, expected);
}

#[test]
fn format_run_output_dry_run_verbose_mode() {
    let outcome = sample_dry_run_outcome();
    let output = output::format_run_output(&outcome, output::OutputMode::Verbose, true);
    let expected = [
        "DRY RUN MODE - No actual execution performed",
        "run id: run-456",
        "report base: /tmp/reports-dry",
        "",
        "==> entity orders (format=json)",
        "  Input: /tmp/in/orders (json)",
        "  Accepted Output: /tmp/out/orders.parquet (parquet)",
        "  Rejected Output: /tmp/out/orders.rejected (csv)",
        "  Archive Path: /tmp/archive/orders (s3)",
        "  Resolved Inputs: 1",
        "  Resolved Files:",
        "    /tmp/in/orders/2026-01-01.json",
        "  report: /tmp/reports-dry/run_run-456/orders/run.json",
        "",
        "Totals: files=0 rows=0 accepted=0 rejected=0",
        "Overall: success (exit_code=0)",
        "Run summary: /tmp/reports-dry/run_run-456/run.summary.json",
    ]
    .join("\n");

    assert_eq!(output, expected);
}
