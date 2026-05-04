use std::collections::HashMap;

use floe_core::{
    detect_unresolved_placeholders, parse_profile_from_str, resolve_vars, validate_merged_vars,
    validate_profile, VarSources,
};

// ---------------------------------------------------------------------------
// detect_unresolved_placeholders
// ---------------------------------------------------------------------------

#[test]
fn no_placeholder_returns_none() {
    assert_eq!(detect_unresolved_placeholders("plain_value"), None);
    assert_eq!(detect_unresolved_placeholders(""), None);
    assert_eq!(detect_unresolved_placeholders("path/to/something"), None);
}

#[test]
fn simple_placeholder_detected() {
    let result = detect_unresolved_placeholders("${MY_VAR}");
    assert!(result.is_some(), "expected Some, got None");
    assert!(result.unwrap().contains("MY_VAR"));
}

#[test]
fn placeholder_in_path_detected() {
    let result = detect_unresolved_placeholders("/data/${CATALOG}/table");
    assert!(result.is_some());
}

#[test]
fn empty_placeholder_detected() {
    let result = detect_unresolved_placeholders("${}");
    assert!(result.is_some());
}

#[test]
fn unclosed_placeholder_detected() {
    let result = detect_unresolved_placeholders("${UNCLOSED");
    assert!(result.is_some());
}

// ---------------------------------------------------------------------------
// validate_profile – success
// ---------------------------------------------------------------------------

fn make_minimal_profile_yaml(name: &str) -> String {
    format!(
        r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: {name}
"#
    )
}

#[test]
fn validate_minimal_profile_ok() {
    let profile = parse_profile_from_str(&make_minimal_profile_yaml("dev")).expect("parse");
    validate_profile(&profile).expect("validate");
}

#[test]
fn validate_full_profile_ok() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
  env: prod
execution:
  runner:
    type: local
variables:
  CATALOG: prod_catalog
  SCHEMA: prod_schema
validation:
  strict: true
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    validate_profile(&profile).expect("validate");
}

// ---------------------------------------------------------------------------
// validate_profile – cross-variable references are allowed in raw profiles
// ---------------------------------------------------------------------------

#[test]
fn validate_profile_allows_cross_references() {
    // ${VAR} syntax is valid in a raw profile — it is a reference to another
    // variable that resolve_vars will expand later. validate_profile must not
    // reject it.
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
variables:
  BASE_DIR: /data
  CATALOG: ${BASE_DIR}/catalog
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    validate_profile(&profile).expect("cross-references must be accepted by validate_profile");
}

#[test]
fn resolve_vars_fails_on_undefined_reference() {
    // resolve_vars must catch references to keys that are not defined.
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
variables:
  CATALOG: ${UNDEFINED_VAR}
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    let empty = HashMap::new();
    let err = resolve_vars(VarSources {
        profile: &profile.variables,
        cli: &empty,
        config: &empty,
    })
    .unwrap_err();
    assert!(err.to_string().contains("UNDEFINED_VAR"), "got: {err}");
}

#[test]
fn resolve_vars_fails_on_unclosed_placeholder() {
    // resolve_vars must catch unclosed ${ syntax in variable values.
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
variables:
  PATH_VAR: /some/${UNCLOSED
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    let empty = HashMap::new();
    let err = resolve_vars(VarSources {
        profile: &profile.variables,
        cli: &empty,
        config: &empty,
    })
    .unwrap_err();
    assert!(err.to_string().contains("unclosed"), "got: {err}");
}

#[test]
fn validate_profile_rejects_empty_placeholder() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
variables:
  BAD: /path/${}
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    let err = validate_profile(&profile).unwrap_err();
    assert!(err.to_string().contains("malformed"), "got: {err}");
}

#[test]
fn validate_profile_rejects_unclosed_placeholder() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
variables:
  BAD: /some/${UNCLOSED
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    let err = validate_profile(&profile).unwrap_err();
    assert!(err.to_string().contains("malformed"), "got: {err}");
}

// ---------------------------------------------------------------------------
// validate_profile – runner type acceptance
// ---------------------------------------------------------------------------

#[test]
fn validate_kubernetes_job_runner_type_is_accepted() {
    // "kubernetes_job" must be recognized at the profile validation layer so that
    // connector-side profiles are valid without requiring the adapter in floe-core.
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: k8s-prod
execution:
  runner:
    type: kubernetes_job
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    validate_profile(&profile).expect("kubernetes_job runner type must pass profile validation");
}

#[test]
fn validate_databricks_job_runner_type_is_accepted() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dbx-prod
execution:
  runner:
    type: databricks_job
    workspace_url: https://adb-1234.5.azuredatabricks.net
    existing_cluster_id: 1111-222222-abc123
    config_uri: dbfs:/floe/configs/prod.yml
    python_file_uri: dbfs:/floe/bin/floe_entry.py
    auth:
      service_principal_oauth_ref: env://DATABRICKS_TOKEN
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    validate_profile(&profile).expect("databricks_job runner type must pass profile validation");
}

#[test]
fn validate_databricks_job_missing_required_field_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dbx-prod
execution:
  runner:
    type: databricks_job
    workspace_url: https://adb-1234.5.azuredatabricks.net
    config_uri: dbfs:/floe/configs/prod.yml
    python_file_uri: dbfs:/floe/bin/floe_entry.py
    auth:
      service_principal_oauth_ref: env://DATABRICKS_TOKEN
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    let err = validate_profile(&profile).unwrap_err();
    assert!(
        err.to_string().contains("existing_cluster_id"),
        "got: {err}"
    );
}

#[test]
fn validate_databricks_job_missing_python_file_uri_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dbx-prod
execution:
  runner:
    type: databricks_job
    workspace_url: https://adb-1234.5.azuredatabricks.net
    existing_cluster_id: 1111-222222-abc123
    config_uri: dbfs:/floe/configs/prod.yml
    auth:
      service_principal_oauth_ref: env://DATABRICKS_TOKEN
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    let err = validate_profile(&profile).unwrap_err();
    assert!(err.to_string().contains("python_file_uri"), "got: {err}");
}

#[test]
fn validate_unknown_runner_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
execution:
  runner:
    type: databricks
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    let err = validate_profile(&profile).unwrap_err();
    assert!(
        err.to_string().contains("databricks") || err.to_string().contains("runner"),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------------
// validate_merged_vars – precedence logic
// ---------------------------------------------------------------------------

/// Demonstrates the precedence model:
///   config variables  >  CLI overrides  >  profile variables
///
/// The caller is responsible for building the merged map with correct
/// precedence.  This test verifies that validate_merged_vars accepts a
/// correctly merged map and rejects one that still has unresolved placeholders.
#[test]
fn validate_merged_vars_clean_map_ok() {
    let mut vars = HashMap::new();
    // profile variables (lowest precedence)
    vars.insert("CATALOG".to_string(), "dev_catalog".to_string());
    // CLI override (mid precedence) — overwrites profile value
    vars.insert("CATALOG".to_string(), "cli_catalog".to_string());
    // config variable (highest precedence) — would overwrite CLI
    vars.insert("SCHEMA".to_string(), "config_schema".to_string());

    validate_merged_vars(&vars).expect("clean merged vars should pass");
    assert_eq!(vars["CATALOG"], "cli_catalog");
    assert_eq!(vars["SCHEMA"], "config_schema");
}

#[test]
fn validate_merged_vars_unresolved_fails() {
    let mut vars = HashMap::new();
    vars.insert("PATH".to_string(), "${STILL_UNRESOLVED}".to_string());
    let err = validate_merged_vars(&vars).unwrap_err();
    assert!(
        err.to_string().contains("unresolved placeholder"),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Precedence documentation test
// ---------------------------------------------------------------------------

/// Illustrates the three-level precedence chain without any real run:
///   config vars  >  CLI vars  >  profile vars
#[test]
fn precedence_config_beats_cli_beats_profile() {
    // Start from profile variables (lowest priority).
    let mut merged: HashMap<String, String> = HashMap::new();
    merged.insert("KEY".to_string(), "from_profile".to_string());

    // CLI override wins over profile.
    let cli_vars = [("KEY".to_string(), "from_cli".to_string())];
    for (k, v) in &cli_vars {
        merged.insert(k.clone(), v.clone());
    }
    assert_eq!(merged["KEY"], "from_cli");

    // Config variable wins over CLI (highest precedence).
    let config_vars = [("KEY".to_string(), "from_config".to_string())];
    for (k, v) in &config_vars {
        merged.insert(k.clone(), v.clone());
    }
    assert_eq!(merged["KEY"], "from_config");

    validate_merged_vars(&merged).expect("no unresolved vars");
}
