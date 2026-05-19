use std::collections::HashMap;

use crate::config::{CatalogsConfig, LineageConfig, StoragesConfig};

/// Top-level profile document (apiVersion + kind + sections).
#[derive(Debug, Clone)]
pub struct ProfileConfig {
    pub api_version: String,
    pub kind: String,
    pub metadata: ProfileMetadata,
    pub execution: Option<ProfileExecution>,
    pub variables: HashMap<String, String>,
    pub catalogs: Option<CatalogsConfig>,
    pub storages: Option<StoragesConfig>,
    pub lineage: Option<LineageConfig>,
    pub validation: Option<ProfileValidation>,
}

#[derive(Debug, Clone)]
pub struct ProfileMetadata {
    pub name: String,
    pub description: Option<String>,
    pub env: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ProfileExecution {
    pub runner: ProfileRunner,
}

#[derive(Debug, Clone)]
pub struct ProfileRunner {
    pub runner_type: String,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub timeout_seconds: Option<u64>,
    pub ttl_seconds_after_finished: Option<u64>,
    pub poll_interval_seconds: Option<u64>,
    pub secrets: Option<Vec<ProfileRunnerSecret>>,
    // Kubernetes runner fields
    pub image: Option<String>,
    pub namespace: Option<String>,
    pub service_account: Option<String>,
    pub resources: Option<ProfileRunnerResources>,
    pub env: Option<HashMap<String, String>>,
    // Databricks runner fields
    pub workspace_url: Option<String>,
    pub existing_cluster_id: Option<String>,
    pub config_uri: Option<String>,
    pub python_file_uri: Option<String>,
    pub job_name: Option<String>,
    pub auth: Option<ProfileRunnerAuth>,
    pub env_parameters: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct ProfileRunnerSecret {
    pub name: String,
    pub secret_name: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct ProfileRunnerResources {
    pub cpu: Option<String>,
    pub memory_mb: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ProfileRunnerAuth {
    pub service_principal_oauth_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProfileValidation {
    pub strict: Option<bool>,
}

/// Expected value for `kind`.
pub const PROFILE_KIND: &str = "EnvironmentProfile";

/// Expected value for `apiVersion`.
pub const PROFILE_API_VERSION: &str = "floe/v1";
