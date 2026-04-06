use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct CommonManifest {
    pub schema: &'static str,
    pub generated_at_ts_ms: u64,
    pub floe_version: &'static str,
    pub spec_version: String,
    pub manifest_id: String,
    pub config_uri: String,
    pub config_checksum: Option<String>,
    pub report_base_uri: String,
    pub domains: Vec<ManifestDomain>,
    pub execution: ManifestExecution,
    pub runners: ManifestRunners,
    pub entities: Vec<ManifestEntity>,
}

#[derive(Debug, Serialize)]
pub struct ManifestDomain {
    pub name: String,
    pub incoming_dir: String,
}

#[derive(Debug, Serialize)]
pub struct ManifestExecution {
    pub entrypoint: &'static str,
    pub base_args: Vec<&'static str>,
    pub per_entity_args: Vec<&'static str>,
    pub log_format: &'static str,
    pub result_contract: ManifestResultContract,
    pub defaults: ManifestExecutionDefaults,
}

#[derive(Debug, Serialize)]
pub struct ManifestResultContract {
    pub run_finished_event: bool,
    pub summary_uri_field: &'static str,
    pub exit_codes: HashMap<&'static str, &'static str>,
}

#[derive(Debug, Serialize)]
pub struct ManifestExecutionDefaults {
    pub env: HashMap<String, String>,
    pub workdir: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunners {
    pub default: &'static str,
    pub definitions: HashMap<&'static str, ManifestRunnerDefinition>,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunnerDefinition {
    #[serde(rename = "type")]
    pub runner_type: &'static str,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub timeout_seconds: Option<u64>,
    pub ttl_seconds_after_finished: Option<u64>,
    pub poll_interval_seconds: Option<u64>,
    pub secrets: Option<Vec<String>>,
    pub image: Option<String>,
    pub namespace: Option<String>,
    pub service_account: Option<String>,
    pub resources: Option<ManifestRunnerResources>,
    pub env: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunnerResources {
    pub cpu: Option<String>,
    pub memory_mb: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ManifestEntity {
    pub name: String,
    pub domain: Option<String>,
    pub group_name: String,
    pub asset_key: Vec<String>,
    pub source_format: String,
    pub accepted_sink_uri: String,
    pub rejected_sink_uri: Option<String>,
    pub tags: Option<HashMap<String, String>>,
    pub source: ManifestSource,
    pub sinks: ManifestSinks,
    pub runner: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSource {
    pub format: String,
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub resolved: bool,
    pub cast_mode: Option<String>,
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSinks {
    pub accepted: ManifestSinkTarget,
    pub rejected: Option<ManifestSinkTarget>,
    pub archive: Option<ManifestArchiveTarget>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSinkTarget {
    pub format: String,
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub resolved: bool,
}

#[derive(Debug, Serialize)]
pub struct ManifestArchiveTarget {
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub resolved: bool,
}
