use floe_core::parse_profile_from_str;

// ---------------------------------------------------------------------------
// Success cases
// ---------------------------------------------------------------------------

#[test]
fn parse_minimal_profile() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
"#;
    let profile = parse_profile_from_str(yaml).expect("parse minimal profile");
    assert_eq!(profile.api_version, "floe/v1");
    assert_eq!(profile.kind, "EnvironmentProfile");
    assert_eq!(profile.metadata.name, "dev");
    assert!(profile.metadata.description.is_none());
    assert!(profile.metadata.env.is_none());
    assert!(profile.metadata.tags.is_none());
    assert!(profile.execution.is_none());
    assert!(profile.variables.is_empty());
    assert!(profile.validation.is_none());
}

#[test]
fn parse_full_profile() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
  description: "Production environment"
  env: prod
  tags:
    - production
    - stable
execution:
  runner:
    type: local
variables:
  CATALOG: prod_catalog
  SCHEMA: prod_schema
  TABLE_PREFIX: prod
validation:
  strict: true
"#;
    let profile = parse_profile_from_str(yaml).expect("parse full profile");
    assert_eq!(profile.metadata.name, "prod");
    assert_eq!(
        profile.metadata.description.as_deref(),
        Some("Production environment")
    );
    assert_eq!(profile.metadata.env.as_deref(), Some("prod"));
    assert_eq!(
        profile.metadata.tags.as_ref().unwrap(),
        &vec!["production".to_string(), "stable".to_string()]
    );
    let exec = profile.execution.as_ref().expect("execution");
    assert_eq!(exec.runner.runner_type, "local");
    assert_eq!(
        profile.variables.get("CATALOG").map(|s| s.as_str()),
        Some("prod_catalog")
    );
    assert_eq!(
        profile.variables.get("SCHEMA").map(|s| s.as_str()),
        Some("prod_schema")
    );
    assert_eq!(
        profile.variables.get("TABLE_PREFIX").map(|s| s.as_str()),
        Some("prod")
    );
    assert_eq!(profile.validation.as_ref().unwrap().strict, Some(true));
}

#[test]
fn parse_kubernetes_runner_with_extended_fields() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-k8s
execution:
  runner:
    type: kubernetes
    command: floe
    args:
      - run
      - -c
      - /config/config.yml
    timeout_seconds: 3600
    ttl_seconds_after_finished: 600
    poll_interval_seconds: 15
    secrets:
      - floe-db
      - floe-warehouse
"#;
    let profile = parse_profile_from_str(yaml).expect("parse k8s profile");
    let runner = &profile.execution.as_ref().expect("execution").runner;
    assert_eq!(runner.runner_type, "kubernetes");
    assert_eq!(runner.command.as_deref(), Some("floe"));
    assert_eq!(
        runner.args.as_ref().unwrap(),
        &vec![
            "run".to_string(),
            "-c".to_string(),
            "/config/config.yml".to_string()
        ]
    );
    assert_eq!(runner.timeout_seconds, Some(3600));
    assert_eq!(runner.ttl_seconds_after_finished, Some(600));
    assert_eq!(runner.poll_interval_seconds, Some(15));
    assert_eq!(
        runner.secrets.as_ref().unwrap(),
        &vec!["floe-db".to_string(), "floe-warehouse".to_string()]
    );
}

#[test]
fn parse_profile_no_variables() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: uat
  env: uat
execution:
  runner:
    type: local
"#;
    let profile = parse_profile_from_str(yaml).expect("parse uat profile");
    assert_eq!(profile.metadata.name, "uat");
    assert!(profile.variables.is_empty());
}

#[test]
fn parse_profile_validation_strict_false() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
validation:
  strict: false
"#;
    let profile = parse_profile_from_str(yaml).expect("parse");
    assert_eq!(profile.validation.as_ref().unwrap().strict, Some(false));
}

// ---------------------------------------------------------------------------
// Failure cases
// ---------------------------------------------------------------------------

#[test]
fn parse_empty_yaml_fails() {
    let err = parse_profile_from_str("").unwrap_err();
    assert!(err.to_string().contains("empty"), "got: {err}");
}

#[test]
fn parse_wrong_api_version_fails() {
    let yaml = r#"
apiVersion: floe/v2
kind: EnvironmentProfile
metadata:
  name: dev
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("apiVersion"),
        "expected apiVersion error, got: {err}"
    );
}

#[test]
fn parse_wrong_kind_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: SomethingElse
metadata:
  name: dev
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("kind"),
        "expected kind error, got: {err}"
    );
}

#[test]
fn parse_missing_metadata_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("metadata"),
        "expected metadata error, got: {err}"
    );
}

#[test]
fn parse_missing_metadata_name_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  description: "no name"
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("name"),
        "expected name error, got: {err}"
    );
}

#[test]
fn parse_missing_api_version_fails() {
    let yaml = r#"
kind: EnvironmentProfile
metadata:
  name: dev
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("apiVersion"),
        "expected apiVersion error, got: {err}"
    );
}

#[test]
fn parse_unknown_field_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
unknownField: oops
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("unknownField"),
        "expected unknown field error, got: {err}"
    );
}

// P1: non-string optional metadata fields must fail, not silently become None
#[test]
fn parse_metadata_description_non_string_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
  description: 123
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("description"),
        "expected description type error, got: {err}"
    );
}

#[test]
fn parse_metadata_env_non_string_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
  env: true
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("env"),
        "expected env type error, got: {err}"
    );
}

// P2: parse_profile_from_str must reject multi-document YAML
#[test]
fn parse_multi_document_yaml_fails() {
    let yaml = "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata:\n  name: dev\n---\napiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata:\n  name: prod\n";
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("multiple documents"),
        "expected multi-document error, got: {err}"
    );
}

#[test]
fn parse_missing_runner_type_fails() {
    let yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
execution:
  runner:
    other_key: something
"#;
    let err = parse_profile_from_str(yaml).unwrap_err();
    assert!(
        err.to_string().contains("type") || err.to_string().contains("unknown"),
        "expected runner type error, got: {err}"
    );
}
