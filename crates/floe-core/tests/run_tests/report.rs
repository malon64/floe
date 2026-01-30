use floe_core::report::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{}-{}", std::process::id(), nanos)
}

fn sample_report() -> RunReport {
    RunReport {
        spec_version: "0.1".to_string(),
        entity: EntityEcho {
            name: "customer".to_string(),
            metadata: None,
        },
        source: SourceEcho {
            format: "csv".to_string(),
            path: "/tmp/input".to_string(),
            options: None,
            cast_mode: Some("strict".to_string()),
            read_plan: SourceReadPlan::RawAndTyped,
            resolved_inputs: ResolvedInputs {
                mode: ResolvedInputMode::Directory,
                file_count: 1,
                files: vec!["/tmp/input/file.csv".to_string()],
            },
        },
        sink: SinkEcho {
            accepted: SinkTargetEcho {
                format: "parquet".to_string(),
                path: "/tmp/out/accepted".to_string(),
            },
            rejected: Some(SinkTargetEcho {
                format: "csv".to_string(),
                path: "/tmp/out/rejected".to_string(),
            }),
            archive: SinkArchiveEcho {
                enabled: false,
                path: None,
            },
        },
        policy: PolicyEcho {
            severity: Severity::Warn,
        },
        accepted_output: AcceptedOutputSummary {
            path: "/tmp/out/accepted".to_string(),
            accepted_rows: 10,
            parts_written: 1,
            part_files: vec!["part-00000.parquet".to_string()],
            table_version: None,
        },
        results: ResultsTotals {
            files_total: 1,
            rows_total: 10,
            accepted_total: 10,
            rejected_total: 0,
            warnings_total: 0,
            errors_total: 0,
        },
        files: vec![FileReport {
            input_file: "/tmp/input/file.csv".to_string(),
            status: FileStatus::Success,
            row_count: 10,
            accepted_count: 10,
            rejected_count: 0,
            mismatch: FileMismatch {
                declared_columns_count: 1,
                input_columns_count: 1,
                missing_columns: Vec::new(),
                extra_columns: Vec::new(),
                mismatch_action: MismatchAction::None,
                error: None,
                warning: None,
            },
            output: FileOutput {
                accepted_path: Some("/tmp/out/accepted".to_string()),
                rejected_path: None,
                errors_path: None,
                archived_path: None,
            },
            validation: FileValidation {
                errors: 0,
                warnings: 0,
                rules: Vec::new(),
            },
        }],
    }
}

fn sample_summary() -> RunSummaryReport {
    RunSummaryReport {
        spec_version: "0.1".to_string(),
        tool: ToolInfo {
            name: "floe".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git: None,
        },
        run: RunInfo {
            run_id: "2026-01-19T10-23-45Z".to_string(),
            started_at: "2026-01-19T10-23-45Z".to_string(),
            finished_at: "2026-01-19T10-23-46Z".to_string(),
            duration_ms: 1000,
            status: RunStatus::Success,
            exit_code: 0,
        },
        config: ConfigEcho {
            path: "/tmp/config.yml".to_string(),
            version: "0.1".to_string(),
            metadata: None,
        },
        report: ReportEcho {
            path: "/tmp/out/reports".to_string(),
            report_file: "/tmp/out/reports/run_2026-01-19T10-23-45Z/run.summary.json".to_string(),
        },
        results: ResultsTotals {
            files_total: 1,
            rows_total: 10,
            accepted_total: 10,
            rejected_total: 0,
            warnings_total: 0,
            errors_total: 0,
        },
        entities: vec![EntitySummary {
            name: "customer".to_string(),
            status: RunStatus::Success,
            results: ResultsTotals {
                files_total: 1,
                rows_total: 10,
                accepted_total: 10,
                rejected_total: 0,
                warnings_total: 0,
                errors_total: 0,
            },
            report_file: "/tmp/out/reports/run_2026-01-19T10-23-45Z/customer/run.json".to_string(),
        }],
    }
}

#[test]
fn report_serializes_expected_keys() {
    let report = sample_report();
    let value = serde_json::to_value(&report).expect("serialize report");
    let object = value.as_object().expect("report object");
    assert!(object.contains_key("spec_version"));
    assert!(object.contains_key("entity"));
    assert!(object.contains_key("source"));
    assert!(object.contains_key("sink"));
    assert!(object.contains_key("policy"));
    assert!(object.contains_key("accepted_output"));
    assert!(object.contains_key("results"));
    assert!(object.contains_key("files"));
}

#[test]
fn report_file_name_matches_format() {
    let run_dir = ReportWriter::run_dir_name("2026-01-19T10-23-45Z");
    assert_eq!(run_dir, "run_2026-01-19T10-23-45Z");
    let name = ReportWriter::report_file_name();
    assert_eq!(name, "run.json");
}

#[test]
fn compute_run_outcome_table() {
    let (status, code) = compute_run_outcome(&[]);
    assert_eq!(status, RunStatus::Success);
    assert_eq!(code, 0);

    let (status, code) = compute_run_outcome(&[FileStatus::Success]);
    assert_eq!(status, RunStatus::Success);
    assert_eq!(code, 0);

    let (status, code) = compute_run_outcome(&[FileStatus::Rejected]);
    assert_eq!(status, RunStatus::Rejected);
    assert_eq!(code, 0);

    let (status, code) = compute_run_outcome(&[FileStatus::Aborted]);
    assert_eq!(status, RunStatus::Aborted);
    assert_eq!(code, 2);

    let (status, code) = compute_run_outcome(&[FileStatus::Failed]);
    assert_eq!(status, RunStatus::Failed);
    assert_eq!(code, 1);

    let (status, code) = compute_run_outcome(&[
        FileStatus::Success,
        FileStatus::Rejected,
        FileStatus::Aborted,
    ]);
    assert_eq!(status, RunStatus::Aborted);
    assert_eq!(code, 2);

    let (status, code) = compute_run_outcome(&[
        FileStatus::Success,
        FileStatus::Rejected,
        FileStatus::Failed,
    ]);
    assert_eq!(status, RunStatus::Failed);
    assert_eq!(code, 1);
}

#[test]
fn write_report_writes_json_file() {
    let report = sample_report();
    let run_id = "2026-01-19T10-23-45Z";
    let mut dir = std::env::temp_dir();
    dir.push(format!("floe-report-tests-{}", unique_suffix()));
    std::fs::create_dir_all(&dir).expect("create temp dir");

    let report_path =
        ReportWriter::write_report(&dir, run_id, "customer", &report).expect("write report");

    assert!(report_path.exists());
    let expected = dir
        .join(format!("run_{run_id}"))
        .join("customer")
        .join("run.json");
    assert_eq!(report_path, expected);
    let contents = std::fs::read_to_string(&report_path).expect("read report");
    let value: serde_json::Value = serde_json::from_str(&contents).expect("parse report");
    assert!(value.get("entity").is_some());

    let temp_files: Vec<_> = std::fs::read_dir(expected.parent().expect("entity dir"))
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.contains(".tmp-"))
                .unwrap_or(false)
        })
        .collect();
    assert!(temp_files.is_empty());
}

#[test]
fn write_summary_writes_json_file() {
    let summary = sample_summary();
    let run_id = "2026-01-19T10-23-45Z";
    let mut dir = std::env::temp_dir();
    dir.push(format!("floe-summary-tests-{}", unique_suffix()));
    std::fs::create_dir_all(&dir).expect("create temp dir");

    let report_path = ReportWriter::write_summary(&dir, run_id, &summary).expect("write summary");

    assert!(report_path.exists());
    let expected = dir.join(format!("run_{run_id}")).join("run.summary.json");
    assert_eq!(report_path, expected);
    let contents = std::fs::read_to_string(&report_path).expect("read summary");
    let value: serde_json::Value = serde_json::from_str(&contents).expect("parse summary");
    assert!(value.get("run").is_some());
}
