use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn validate_default_output_is_human_text() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["validate", "-c"])
        .arg(&config_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Config valid:"))
        .stdout(predicate::str::contains("Next: floe run"));
}

#[test]
fn validate_with_profile_includes_profile_in_next_command() {
    let config_path = repo_root().join("example/config.yml");
    let profile_path = repo_root().join("example/profiles/dev.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["validate", "-c"])
        .arg(&config_path)
        .args(["--profile"])
        .arg(&profile_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Next: floe run -c "))
        .stdout(predicate::str::contains(" -p "));
}

#[test]
fn validate_with_output_flag_is_rejected() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["validate", "-c"])
        .arg(&config_path)
        .args(["--output", "json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument '--output'"));
}

#[test]
fn validate_invalid_config_sets_exit_1_and_error_text() {
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/invalid_config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["validate", "-c"])
        .arg(&fixture_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error:"));
}

#[test]
fn validate_profile_only_output_is_human_text() {
    let profile_path = repo_root().join("example/profiles/dev.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["validate", "--profile"])
        .arg(&profile_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Profile valid: dev"))
        .stdout(predicate::str::contains(
            "Schema: floe/v1/EnvironmentProfile",
        ));
}
