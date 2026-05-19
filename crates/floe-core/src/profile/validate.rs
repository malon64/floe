use std::collections::{HashMap, HashSet};

use crate::config::{LineageConfig, StoragesConfig};
use crate::profile::types::{ProfileConfig, ProfileRunner};
use crate::{ConfigError, FloeResult};

/// Validate a parsed profile.
///
/// Checks performed:
/// - `metadata.name` is non-empty (guaranteed by parser, double-checked here).
/// - `execution.runner.type` is one of the recognized values (`local`, `kubernetes_job`, `databricks_job`) when present.
///   Orchestration/job-submission for each runner type belongs to connector crates, not floe-core.
/// - No variable value contains a *syntactically malformed* `${...}` placeholder.
///   Valid `${KEY}` cross-references are allowed — they are expanded by `resolve_vars` later.
pub fn validate_profile(profile: &ProfileConfig) -> FloeResult<()> {
    if profile.metadata.name.trim().is_empty() {
        return Err(Box::new(ConfigError(
            "profile.metadata.name must not be empty".to_string(),
        )));
    }

    if let Some(execution) = &profile.execution {
        validate_runner_type(&execution.runner.runner_type)?;
        validate_runner_contract(&execution.runner)?;
    }

    validate_no_malformed_vars(&profile.variables)?;
    validate_profile_catalogs(profile)?;
    if let Some(storages) = &profile.storages {
        validate_profile_storages(storages)?;
    }
    if let Some(lineage) = &profile.lineage {
        validate_profile_lineage(lineage)?;
    }

    Ok(())
}

fn validate_profile_catalogs(profile: &ProfileConfig) -> FloeResult<()> {
    let Some(catalogs) = &profile.catalogs else {
        return Ok(());
    };
    if catalogs.definitions.is_empty() {
        return Err(Box::new(ConfigError(
            "profile.catalogs.definitions must not be empty".to_string(),
        )));
    }

    let mut names = HashSet::new();
    for definition in &catalogs.definitions {
        if definition.name.trim().is_empty() {
            return Err(Box::new(ConfigError(
                "profile.catalogs.definitions.name must not be empty".to_string(),
            )));
        }
        if !names.insert(definition.name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "profile.catalogs.definitions name={} is duplicated",
                definition.name
            ))));
        }
    }

    if let Some(default_name) = &catalogs.default {
        if !names.contains(default_name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "profile.catalogs.default={} does not match any definition",
                default_name
            ))));
        }
    }

    Ok(())
}

fn validate_profile_storages(storages: &StoragesConfig) -> FloeResult<()> {
    if storages.definitions.is_empty() {
        return Err(Box::new(ConfigError(
            "profile.storages.definitions must not be empty".to_string(),
        )));
    }
    const ALLOWED_STORAGE_TYPES: &[&str] = &["local", "s3", "adls", "gcs"];
    let mut names = HashSet::new();
    for definition in &storages.definitions {
        if definition.name.trim().is_empty() {
            return Err(Box::new(ConfigError(
                "profile.storages.definitions.name must not be empty".to_string(),
            )));
        }
        if !names.insert(definition.name.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "profile.storages.definitions name={} is duplicated",
                definition.name
            ))));
        }
        if !ALLOWED_STORAGE_TYPES.contains(&definition.fs_type.as_str()) {
            return Err(Box::new(ConfigError(format!(
                "profile.storages.definitions name={} type={} is unsupported (allowed: {})",
                definition.name,
                definition.fs_type,
                ALLOWED_STORAGE_TYPES.join(", ")
            ))));
        }
        if definition.fs_type == "s3" {
            if definition.bucket.is_none() {
                return Err(Box::new(ConfigError(format!(
                    "profile.storages.definitions name={} requires bucket for type s3",
                    definition.name
                ))));
            }
            if definition.region.is_none() {
                return Err(Box::new(ConfigError(format!(
                    "profile.storages.definitions name={} requires region for type s3",
                    definition.name
                ))));
            }
        }
        if definition.fs_type == "adls" {
            if definition.account.is_none() {
                return Err(Box::new(ConfigError(format!(
                    "profile.storages.definitions name={} requires account for type adls",
                    definition.name
                ))));
            }
            if definition.container.is_none() {
                return Err(Box::new(ConfigError(format!(
                    "profile.storages.definitions name={} requires container for type adls",
                    definition.name
                ))));
            }
        }
        if definition.fs_type == "gcs" && definition.bucket.is_none() {
            return Err(Box::new(ConfigError(format!(
                "profile.storages.definitions name={} requires bucket for type gcs",
                definition.name
            ))));
        }
    }
    let Some(default_name) = &storages.default else {
        return Err(Box::new(ConfigError(
            "profile.storages.default is required when storages is set".to_string(),
        )));
    };
    if !names.contains(default_name.as_str()) {
        return Err(Box::new(ConfigError(format!(
            "profile.storages.default={default_name} does not match any definition"
        ))));
    }
    Ok(())
}

fn validate_profile_lineage(lineage: &LineageConfig) -> FloeResult<()> {
    if lineage.url.trim().is_empty() {
        return Err(Box::new(ConfigError(
            "profile.lineage.url must not be empty".to_string(),
        )));
    }
    if lineage.namespace.trim().is_empty() {
        return Err(Box::new(ConfigError(
            "profile.lineage.namespace must not be empty".to_string(),
        )));
    }
    if lineage.max_failures == Some(0) {
        return Err(Box::new(ConfigError(
            "profile.lineage.max_failures must be at least 1".to_string(),
        )));
    }
    Ok(())
}

/// Validate a variable map produced by merging sources in precedence order:
///
///   config variables  >  CLI overrides  >  profile variables
///
/// Callers are expected to build `merged` with that precedence (higher-priority
/// sources inserted last / overwriting earlier values).  This function then
/// checks that no value in the final merged map still contains an unresolved
/// `${...}` placeholder.
pub fn validate_merged_vars(merged: &HashMap<String, String>) -> FloeResult<()> {
    validate_no_unresolved_vars(merged)
}

/// Detect unresolved `${VAR}` placeholders in any variable value.
///
/// A placeholder is considered unresolved when the text `${` appears in a
/// value and is not closed by `}`.  Values that do not contain `${` are
/// accepted unchanged (they may be plain strings or already-substituted).
pub fn detect_unresolved_placeholders(value: &str) -> Option<String> {
    let rest = value;
    if let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let key = after[..end].trim();
                if key.is_empty() {
                    return Some("${} (empty placeholder)".to_string());
                }
                return Some(format!("${{{key}}}"));
            }
            None => {
                // `${` without closing `}` — also unresolved
                return Some(
                    format!("${{... (unclosed placeholder in: {value:?})}}")
                        .chars()
                        .take(120)
                        .collect(),
                );
            }
        }
    }
    None
}

/// Detect *syntactically malformed* `${...}` placeholders in a variable value.
///
/// Well-formed `${KEY}` references (non-empty key, properly closed brace) are
/// intentional cross-variable references expanded by `resolve_vars` — they pass
/// through as `None`.  Only structural errors are returned:
/// - `${}` — empty key inside the placeholder
/// - `${UNCLOSED` — `${` with no matching `}`
pub fn detect_malformed_placeholder(value: &str) -> Option<String> {
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        rest = &rest[start + 2..];
        match rest.find('}') {
            Some(end) => {
                let key = rest[..end].trim();
                if key.is_empty() {
                    return Some("${} (empty placeholder)".to_string());
                }
                rest = &rest[end + 1..];
            }
            None => {
                return Some(
                    format!("${{... (unclosed placeholder in: {value:?})}}")
                        .chars()
                        .take(120)
                        .collect(),
                );
            }
        }
    }
    None
}

fn validate_no_malformed_vars(vars: &HashMap<String, String>) -> FloeResult<()> {
    for (key, value) in vars {
        if let Some(placeholder) = detect_malformed_placeholder(value) {
            return Err(Box::new(ConfigError(format!(
                "profile variable \"{key}\" contains malformed placeholder: {placeholder}"
            ))));
        }
    }
    Ok(())
}

fn validate_no_unresolved_vars(vars: &HashMap<String, String>) -> FloeResult<()> {
    for (key, value) in vars {
        if let Some(placeholder) = detect_unresolved_placeholders(value) {
            return Err(Box::new(ConfigError(format!(
                "profile variable \"{key}\" contains unresolved placeholder: {placeholder}"
            ))));
        }
    }
    Ok(())
}

fn validate_runner_type(runner_type: &str) -> FloeResult<()> {
    const KNOWN_RUNNERS: &[&str] = &["local", "kubernetes_job", "databricks_job"];
    if !KNOWN_RUNNERS.contains(&runner_type) {
        return Err(Box::new(ConfigError(format!(
            "profile.execution.runner.type: unknown runner \"{runner_type}\"; \
             known runners: {}",
            KNOWN_RUNNERS.join(", ")
        ))));
    }
    Ok(())
}

fn validate_runner_contract(runner: &ProfileRunner) -> FloeResult<()> {
    if runner.runner_type == "kubernetes_job" {
        if runner
            .image
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
        {
            return Err(Box::new(ConfigError(
                "profile.execution.runner.image is required for kubernetes_job".to_string(),
            )));
        }
        if runner
            .namespace
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
        {
            return Err(Box::new(ConfigError(
                "profile.execution.runner.namespace is required for kubernetes_job".to_string(),
            )));
        }
    }
    if runner.runner_type != "databricks_job" {
        return Ok(());
    }

    if runner
        .workspace_url
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Err(Box::new(ConfigError(
            "profile.execution.runner.workspace_url is required for databricks_job".to_string(),
        )));
    }
    if runner
        .existing_cluster_id
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Err(Box::new(ConfigError(
            "profile.execution.runner.existing_cluster_id is required for databricks_job"
                .to_string(),
        )));
    }
    if runner
        .config_uri
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Err(Box::new(ConfigError(
            "profile.execution.runner.config_uri is required for databricks_job".to_string(),
        )));
    }
    if runner
        .python_file_uri
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
    {
        return Err(Box::new(ConfigError(
            "profile.execution.runner.python_file_uri is required for databricks_job".to_string(),
        )));
    }

    let auth_ref = runner
        .auth
        .as_ref()
        .and_then(|auth| auth.service_principal_oauth_ref.as_deref())
        .map(str::trim)
        .unwrap_or("");
    if auth_ref.is_empty() {
        return Err(Box::new(ConfigError(
            "profile.execution.runner.auth.service_principal_oauth_ref is required for databricks_job"
                .to_string(),
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_simple_placeholder() {
        assert_eq!(
            detect_unresolved_placeholders("${MY_VAR}"),
            Some("${MY_VAR}".to_string())
        );
    }

    #[test]
    fn detect_no_placeholder() {
        assert_eq!(detect_unresolved_placeholders("plain_value"), None);
        assert_eq!(detect_unresolved_placeholders(""), None);
    }

    #[test]
    fn detect_unclosed_placeholder() {
        let result = detect_unresolved_placeholders("${UNCLOSED");
        assert!(result.is_some());
    }

    #[test]
    fn detect_empty_placeholder() {
        let result = detect_unresolved_placeholders("${}");
        assert!(result.is_some());
    }
}
