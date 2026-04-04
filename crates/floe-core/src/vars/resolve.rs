use std::collections::HashMap;

use crate::{ConfigError, FloeResult};

/// Maximum recursive expansion depth.  Prevents stack exhaustion from
/// pathologically deep (non-cyclic) variable chains and acts as a second
/// safety net alongside the explicit cycle check.
const MAX_DEPTH: usize = 32;

/// The three sources of variables, listed from **lowest** to **highest**
/// precedence.  When the same key appears in multiple sources the value
/// from the highest-precedence source wins.
///
/// ```text
/// config variables  >  CLI overrides  >  profile variables
/// ```
pub struct VarSources<'a> {
    /// Variables defined in the environment profile (`variables:` section).
    /// Lowest precedence.
    pub profile: &'a HashMap<String, String>,
    /// Variables supplied via the CLI (`--var KEY=VALUE`).
    /// Mid precedence.
    pub cli: &'a HashMap<String, String>,
    /// Variables defined in the main Floe config (`env.vars:`).
    /// Highest precedence.
    pub config: &'a HashMap<String, String>,
}

/// Merge variable sources with correct precedence and expand all `${VAR}`
/// placeholders in variable *values*, including nested references.
///
/// # Resolution rules
///
/// 1. **Merge** – build a raw map applying the precedence chain:
///    `config > cli > profile`.  Higher-precedence keys overwrite lower ones.
/// 2. **Expand** – for every value, substitute `${KEY}` with the resolved
///    value of `KEY` from the merged map, recursively.  A value may reference
///    another variable whose value also contains `${…}` references.
/// 3. **Fail-fast** – any placeholder that cannot be resolved (key not present
///    in the merged map) or that forms a cycle produces an immediate,
///    actionable error.
///
/// # Errors
///
/// Returns an error if:
/// - A `${KEY}` reference in a variable value has no corresponding key in the
///   merged map.
/// - A cycle is detected (e.g., `A = ${B}`, `B = ${A}`).
/// - An unclosed `${` or empty `${}` placeholder is found.
pub fn resolve_vars(sources: VarSources<'_>) -> FloeResult<HashMap<String, String>> {
    let raw = merge(sources);
    expand_all(&raw)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build the merged raw map (no expansion yet).
fn merge(sources: VarSources<'_>) -> HashMap<String, String> {
    // Start from lowest precedence and let each higher layer overwrite.
    let mut merged =
        HashMap::with_capacity(sources.profile.len() + sources.cli.len() + sources.config.len());
    merged.extend(sources.profile.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged.extend(sources.cli.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged.extend(sources.config.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged
}

/// Expand every value in `raw` by resolving `${VAR}` references.
fn expand_all(raw: &HashMap<String, String>) -> FloeResult<HashMap<String, String>> {
    let mut resolved: HashMap<String, String> = HashMap::with_capacity(raw.len());
    for key in raw.keys() {
        if !resolved.contains_key(key.as_str()) {
            let mut in_progress: Vec<String> = Vec::new();
            expand_key(key, raw, &mut resolved, &mut in_progress, 0)?;
        }
    }
    Ok(resolved)
}

/// Recursively expand the value of `key`, caching the result in `resolved`.
fn expand_key(
    key: &str,
    raw: &HashMap<String, String>,
    resolved: &mut HashMap<String, String>,
    in_progress: &mut Vec<String>,
    depth: usize,
) -> FloeResult<String> {
    // Already expanded — return cached result.
    if let Some(v) = resolved.get(key) {
        return Ok(v.clone());
    }

    // Depth guard.
    if depth > MAX_DEPTH {
        return Err(Box::new(ConfigError(format!(
            "variable expansion exceeded maximum depth ({MAX_DEPTH}); \
             check for deeply nested or circular references near \"${{{key}}}\""
        ))));
    }

    // Cycle detection.
    if in_progress.iter().any(|k| k == key) {
        let chain: Vec<&str> = in_progress.iter().map(|s| s.as_str()).collect();
        return Err(Box::new(ConfigError(format!(
            "circular variable reference detected: {} -> {}",
            chain.join(" -> "),
            key
        ))));
    }

    let raw_value = raw.get(key).ok_or_else(|| {
        Box::new(ConfigError(format!(
            "variable \"${{{key}}}\" is referenced but not defined"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;

    in_progress.push(key.to_string());
    let expanded = expand_value(raw_value, key, raw, resolved, in_progress, depth + 1)?;
    in_progress.pop();

    resolved.insert(key.to_string(), expanded.clone());
    Ok(expanded)
}

/// Substitute every `${VAR}` occurrence in `value`, recursively resolving
/// each referenced variable.
fn expand_value(
    value: &str,
    owner_key: &str,
    raw: &HashMap<String, String>,
    resolved: &mut HashMap<String, String>,
    in_progress: &mut Vec<String>,
    depth: usize,
) -> FloeResult<String> {
    let mut result = String::with_capacity(value.len());
    let mut rest = value;

    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        rest = &rest[start + 2..];

        let end = rest.find('}').ok_or_else(|| {
            Box::new(ConfigError(format!(
                "variable \"{owner_key}\": unclosed placeholder in value {value:?}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let ref_key = rest[..end].trim();
        if ref_key.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "variable \"{owner_key}\": empty placeholder ${{}}"
            ))));
        }

        let ref_value = expand_key(ref_key, raw, resolved, in_progress, depth)?;
        result.push_str(&ref_value);
        rest = &rest[end + 1..];
    }

    result.push_str(rest);
    Ok(result)
}
