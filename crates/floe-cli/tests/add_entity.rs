use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn add_entity_writes_config_that_validates() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("customers.csv");

    fs::write(&config_path, "version: \"0.2\"\nentities: []\n").expect("write config");
    fs::write(&input_path, "id,name\n1,Alice\n2,Bob\n").expect("write csv");

    let mut add_cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    add_cmd
        .args(["add-entity", "-c"])
        .arg(&config_path)
        .args(["--input"])
        .arg(&input_path)
        .args(["--format", "csv", "--name", "customers"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Entity added: customers"));

    let mut validate_cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    validate_cmd
        .args(["validate", "-c"])
        .arg(&config_path)
        .args(["--entities", "customers"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Config valid:"));
}
