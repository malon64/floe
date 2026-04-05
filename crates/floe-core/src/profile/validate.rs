use std::collections::HashMap;

use crate::profile::types::ProfileConfig;
use crate::{ConfigError, FloeResult};

/// Validate a parsed profile.
///
/// Checks performed:
/// - `metadata.name` is non-empty (guaranteed by parser, double-checked here).
/// - `execution.runner.type` is a known runner kind when present.
/// - No variable value contains an unresolved `${...}` placeholder.
pub fn validate_profile(profile: &ProfileConfig) -> FloeResult<()> {
    if profile.metadata.name.trim().is_empty() {
        return Err(Box::new(ConfigError(
            "profile.metadata.name must not be empty".to_string(),
        )));
    }

    if let Some(execution) = &profile.execution {
        validate_runner_type(&execution.runner.runner_type)?;
    }

    validate_no_unresolved_vars(&profile.variables)?;

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
    // Delegate to RunnerKind::from_profile_str so the accepted set of runner
    // names is defined in a single place and profile validation stays in sync
    // automatically whenever a new runner variant is added.
    crate::runner::RunnerKind::from_profile_str(runner_type)
        .map(|_| ())
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(ConfigError(format!("profile.execution.runner.type: {e}")))
        })
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
