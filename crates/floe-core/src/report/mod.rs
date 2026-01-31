use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub mod build;
pub mod entity;
pub mod output;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunReport {
    pub spec_version: String,
    pub entity: EntityEcho,
    pub source: SourceEcho,
    pub sink: SinkEcho,
    pub policy: PolicyEcho,
    pub accepted_output: AcceptedOutputSummary,
    pub results: ResultsTotals,
    pub files: Vec<FileReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunSummaryReport {
    pub spec_version: String,
    pub tool: ToolInfo,
    pub run: RunInfo,
    pub config: ConfigEcho,
    pub report: ReportEcho,
    pub results: ResultsTotals,
    pub entities: Vec<EntitySummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EntitySummary {
    pub name: String,
    pub status: RunStatus,
    pub results: ResultsTotals,
    pub report_file: String,
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
pub struct AcceptedOutputSummary {
    pub path: String,
    pub accepted_rows: u64,
    pub parts_written: u64,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub part_files: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_version: Option<i64>,
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
    pub mismatch: FileMismatch,
    pub output: FileOutput,
    pub validation: FileValidation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FileMismatch {
    pub declared_columns_count: u64,
    pub input_columns_count: u64,
    pub missing_columns: Vec<String>,
    pub extra_columns: Vec<String>,
    pub mismatch_action: MismatchAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<MismatchIssue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MismatchIssue {
    pub rule: String,
    pub message: String,
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
pub enum MismatchAction {
    None,
    FilledNulls,
    IgnoredExtras,
    RejectedFile,
    Aborted,
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

pub trait ReportFormatter {
    fn format_name(&self) -> &'static str;
    fn serialize_run(&self, report: &RunReport) -> Result<String, ReportError>;
    fn serialize_summary(&self, report: &RunSummaryReport) -> Result<String, ReportError>;
}

pub struct JsonReportFormatter;

impl ReportFormatter for JsonReportFormatter {
    fn format_name(&self) -> &'static str {
        "json"
    }

    fn serialize_run(&self, report: &RunReport) -> Result<String, ReportError> {
        Ok(serde_json::to_string_pretty(report)?)
    }

    fn serialize_summary(&self, report: &RunSummaryReport) -> Result<String, ReportError> {
        Ok(serde_json::to_string_pretty(report)?)
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

    pub fn summary_file_name() -> String {
        "run.summary.json".to_string()
    }

    pub fn report_relative_path(run_id: &str, entity_name: &str) -> String {
        format!(
            "{}/{}/{}",
            Self::run_dir_name(run_id),
            entity_name,
            Self::report_file_name()
        )
    }

    pub fn summary_relative_path(run_id: &str) -> String {
        format!(
            "{}/{}",
            Self::run_dir_name(run_id),
            Self::summary_file_name()
        )
    }

    pub fn entity_report_dir(report_dir: &Path, run_id: &str, entity_name: &str) -> PathBuf {
        report_dir
            .join(Self::run_dir_name(run_id))
            .join(entity_name)
    }

    pub fn report_path(report_dir: &Path, run_id: &str, entity_name: &str) -> PathBuf {
        Self::entity_report_dir(report_dir, run_id, entity_name).join(Self::report_file_name())
    }

    pub fn summary_path(report_dir: &Path, run_id: &str) -> PathBuf {
        report_dir
            .join(Self::run_dir_name(run_id))
            .join(Self::summary_file_name())
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

    pub fn write_summary(
        report_dir: &Path,
        run_id: &str,
        report: &RunSummaryReport,
    ) -> Result<PathBuf, ReportError> {
        let run_dir = report_dir.join(Self::run_dir_name(run_id));
        std::fs::create_dir_all(&run_dir)?;
        let report_path = Self::summary_path(report_dir, run_id);
        let tmp_path = run_dir.join(format!(
            "{}.tmp-{}",
            Self::summary_file_name(),
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
    if file_statuses.contains(&FileStatus::Failed) {
        return (RunStatus::Failed, 1);
    }
    if file_statuses.contains(&FileStatus::Aborted) {
        return (RunStatus::Aborted, 2);
    }
    if file_statuses.contains(&FileStatus::Rejected) {
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
