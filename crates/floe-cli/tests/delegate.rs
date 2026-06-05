//! Delegation behavior of the lean `floe` binary toward the `floe-duckdb`
//! companion. These tests build against default features (no `duckdb`), so the
//! test binary is the lean variant that must delegate.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

fn write_duckdb_config(dir: &std::path::Path) -> std::path::PathBuf {
    let config_path = dir.join("config.yml");
    let body = format!(
        r#"version: "0.1"
report:
  path: "{report}"
entities:
  - name: customers
    source:
      format: csv
      path: "{input}"
    sink:
      accepted:
        format: duckdb
        path: "{out}"
        duckdb:
          table: customers
          schema: main
    policy:
      severity: warn
    schema:
      columns:
        - name: id
          type: string
"#,
        report = dir.join("report").display(),
        input = dir.join("in.csv").display(),
        out = dir.join("out.duckdb").display(),
    );
    fs::write(&config_path, body).expect("write config");
    config_path
}

#[test]
fn lean_run_with_duckdb_sink_and_no_companion_errors_clearly() {
    let dir = tempdir().expect("tempdir");
    let config_path = write_duckdb_config(dir.path());

    // PATH points at an empty dir so no stray `floe-duckdb` is discovered.
    let empty_path = dir.path().join("emptybin");
    fs::create_dir_all(&empty_path).expect("mkdir");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["run", "-c"])
        .arg(&config_path)
        .env("PATH", &empty_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("DuckDB sink"))
        .stderr(predicate::str::contains("floe-duckdb"))
        .stderr(predicate::str::contains("--features duckdb"));
}

#[test]
fn lean_run_does_not_resolve_companion_from_cwd() {
    let dir = tempdir().expect("tempdir");
    let config_path = write_duckdb_config(dir.path());

    // Drop an executable named like the companion into the CWD. A correct
    // resolver must ignore it (git-lfs CVE GHSA-6rw3-3whw-jvjj) and still error.
    let cwd_companion = dir.path().join("floe-duckdb");
    fs::write(&cwd_companion, "#!/bin/sh\nexit 0\n").expect("write fake companion");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&cwd_companion).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&cwd_companion, perms).unwrap();
    }

    let empty_path = dir.path().join("emptybin");
    fs::create_dir_all(&empty_path).expect("mkdir");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["run", "-c"])
        .arg(&config_path)
        .current_dir(dir.path())
        .env("PATH", &empty_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("floe-duckdb"));
}

#[cfg(unix)]
#[test]
fn lean_run_delegates_to_companion_on_path() {
    let dir = tempdir().expect("tempdir");
    let config_path = write_duckdb_config(dir.path());

    // A stub companion on PATH that prints a marker and exits 0 stands in for the
    // real floe-duckdb. Proves the lean binary locates and re-execs it.
    let bin_dir = dir.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("mkdir");
    let stub = bin_dir.join("floe-duckdb");
    fs::write(&stub, "#!/bin/sh\necho DELEGATED_OK\nexit 0\n").expect("write stub");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&stub).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&stub, perms).unwrap();
    }

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["run", "-c"])
        .arg(&config_path)
        .env("PATH", &bin_dir)
        .assert()
        .success()
        .stdout(predicate::str::contains("DELEGATED_OK"));
}
