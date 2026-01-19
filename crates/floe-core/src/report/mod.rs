use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunReport {
    pub spec_version: String,
    pub tool: ToolInfo,
    pub run: RunInfo,
    pub config: ConfigEcho,
    pub entity: EntityEcho,
    pub source: SourceEcho,
    pub sink: SinkEcho,
    pub report: ReportEcho,
    pub policy: PolicyEcho,
    pub results: ResultsTotals,
    pub files: Vec<FileReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolInfo {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git: Option<GitInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GitInfo {
    pub commit: String,
    pub dirty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunInfo {
    pub run_id: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    pub status: RunStatus,
    pub exit_code: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ConfigEcho {
    pub path: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EntityEcho {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SourceEcho {
    pub format: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast_mode: Option<String>,
    pub read_plan: SourceReadPlan,
    pub resolved_inputs: ResolvedInputs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ResolvedInputs {
    pub mode: ResolvedInputMode,
    pub file_count: u64,
    pub files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SinkEcho {
    pub accepted: SinkTargetEcho,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected: Option<SinkTargetEcho>,
    pub archive: SinkArchiveEcho,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SinkTargetEcho {
    pub format: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReportEcho {
    pub path: String,
    pub report_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SinkArchiveEcho {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PolicyEcho {
    pub severity: Severity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ResultsTotals {
    pub files_total: u64,
    pub rows_total: u64,
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub warnings_total: u64,
    pub errors_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FileReport {
    pub input_file: String,
    pub status: FileStatus,
    pub row_count: u64,
    pub accepted_count: u64,
    pub rejected_count: u64,
    pub output: FileOutput,
    pub validation: FileValidation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FileOutput {
    pub accepted_path: Option<String>,
    pub rejected_path: Option<String>,
    pub errors_path: Option<String>,
    pub archived_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FileValidation {
    pub errors: u64,
    pub warnings: u64,
    pub rules: Vec<RuleSummary>,
    pub examples: ExampleSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RuleSummary {
    pub rule: RuleName,
    pub severity: Severity,
    pub violations: u64,
    pub columns: Vec<ColumnSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ColumnSummary {
    pub column: String,
    pub violations: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ExampleSummary {
    pub max_examples_per_rule: u64,
    pub items: Vec<ValidationExample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ValidationExample {
    pub rule: RuleName,
    pub column: String,
    pub row_index: u64,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    Success,
    Rejected,
    Aborted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Success,
    SuccessWithWarnings,
    Rejected,
    Aborted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Warn,
    Reject,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleName {
    NotNull,
    CastError,
    Unique,
    SchemaError,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedInputMode {
    Directory,
    File,
    Glob,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceReadPlan {
    RawAndTyped,
}

#[derive(Debug)]
pub enum ReportError {
    Io(std::io::Error),
    Serialize(serde_json::Error),
}

impl std::fmt::Display for ReportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReportError::Io(err) => write!(f, "report io error: {err}"),
            ReportError::Serialize(err) => write!(f, "report serialize error: {err}"),
        }
    }
}

impl std::error::Error for ReportError {}

impl From<std::io::Error> for ReportError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for ReportError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialize(err)
    }
}

pub fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

pub fn run_id_from_timestamp(timestamp: &str) -> String {
    timestamp.replace(':', "-")
}

pub struct ReportWriter;

impl ReportWriter {
    pub fn run_dir_name(run_id: &str) -> String {
        format!("run_{run_id}")
    }

    pub fn report_file_name() -> String {
        "run.json".to_string()
    }

    pub fn entity_report_dir(report_dir: &Path, run_id: &str, entity_name: &str) -> PathBuf {
        report_dir
            .join(Self::run_dir_name(run_id))
            .join(entity_name)
    }

    pub fn report_path(report_dir: &Path, run_id: &str, entity_name: &str) -> PathBuf {
        Self::entity_report_dir(report_dir, run_id, entity_name).join(Self::report_file_name())
    }

    pub fn write_report(
        report_dir: &Path,
        run_id: &str,
        entity_name: &str,
        report: &RunReport,
    ) -> Result<PathBuf, ReportError> {
        let entity_dir = Self::entity_report_dir(report_dir, run_id, entity_name);
        std::fs::create_dir_all(&entity_dir)?;
        let report_path = Self::report_path(report_dir, run_id, entity_name);
        let tmp_path = entity_dir.join(format!(
            "{}.tmp-{}",
            Self::report_file_name(),
            unique_suffix()
        ));

        let json = serde_json::to_string_pretty(report)?;
        let mut file = File::create(&tmp_path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, &report_path)?;

        Ok(report_path)
    }
}

pub fn compute_run_outcome(file_statuses: &[FileStatus]) -> (RunStatus, i32) {
    if file_statuses.iter().any(|status| *status == FileStatus::Failed) {
        return (RunStatus::Failed, 1);
    }
    if file_statuses
        .iter()
        .any(|status| *status == FileStatus::Aborted)
    {
        return (RunStatus::Aborted, 2);
    }
    if file_statuses
        .iter()
        .any(|status| *status == FileStatus::Rejected)
    {
        return (RunStatus::Rejected, 0);
    }
    (RunStatus::Success, 0)
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{}-{}", std::process::id(), nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> RunReport {
        RunReport {
            spec_version: "0.1".to_string(),
            tool: ToolInfo {
                name: "floe".to_string(),
                version: "0.1.0".to_string(),
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
            report: ReportEcho {
                path: "/tmp/out/reports".to_string(),
                report_file:
                    "/tmp/out/reports/run_2026-01-19T10-23-45Z/customer/run.json".to_string(),
            },
            policy: PolicyEcho {
                severity: Severity::Warn,
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
                output: FileOutput {
                    accepted_path: Some("/tmp/out/accepted/file.parquet".to_string()),
                    rejected_path: None,
                    errors_path: None,
                    archived_path: None,
                },
                validation: FileValidation {
                    errors: 0,
                    warnings: 0,
                    rules: Vec::new(),
                    examples: ExampleSummary {
                        max_examples_per_rule: 3,
                        items: Vec::new(),
                    },
                },
            }],
        }
    }

    #[test]
    fn report_serializes_expected_keys() {
        let report = sample_report();
        let value = serde_json::to_value(&report).expect("serialize report");
        let object = value.as_object().expect("report object");
        assert!(object.contains_key("spec_version"));
        assert!(object.contains_key("tool"));
        assert!(object.contains_key("run"));
        assert!(object.contains_key("config"));
        assert!(object.contains_key("entity"));
        assert!(object.contains_key("source"));
        assert!(object.contains_key("sink"));
        assert!(object.contains_key("report"));
        assert!(object.contains_key("policy"));
        assert!(object.contains_key("results"));
        assert!(object.contains_key("files"));

        let report_obj = object.get("report").expect("report");
        let report_map = report_obj.as_object().expect("report map");
        assert!(report_map.contains_key("report_file"));
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
        let mut dir = std::env::temp_dir();
        dir.push(format!("floe-report-tests-{}", unique_suffix()));
        std::fs::create_dir_all(&dir).expect("create temp dir");

        let report_path = ReportWriter::write_report(
            &dir,
            &report.run.run_id,
            "customer",
            &report,
        )
        .expect("write report");

        assert!(report_path.exists());
        let expected = dir
            .join("run_2026-01-19T10-23-45Z")
            .join("customer")
            .join("run.json");
        assert_eq!(report_path, expected);
        let contents = std::fs::read_to_string(&report_path).expect("read report");
        let value: serde_json::Value =
            serde_json::from_str(&contents).expect("parse report");
        assert_eq!(
            value
                .get("run")
                .and_then(|run| run.get("run_id"))
                .and_then(|run_id| run_id.as_str()),
            Some("2026-01-19T10-23-45Z")
        );

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
}
