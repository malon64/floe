use std::collections::HashMap;

/// Top-level profile document (apiVersion + kind + sections).
#[derive(Debug, Clone)]
pub struct ProfileConfig {
    pub api_version: String,
    pub kind: String,
    pub metadata: ProfileMetadata,
    pub execution: Option<ProfileExecution>,
    pub variables: HashMap<String, String>,
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
    pub secrets: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ProfileValidation {
    pub strict: Option<bool>,
}

/// Expected value for `kind`.
pub const PROFILE_KIND: &str = "EnvironmentProfile";

/// Expected value for `apiVersion`.
pub const PROFILE_API_VERSION: &str = "floe/v1";
