use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::sleep;
use std::time::Duration;

use floe_core::report::{FileStatus, RunStatus};
use floe_core::state::{
    read_entity_state, write_entity_state_atomic, EntityState, ENTITY_STATE_SCHEMA_V1,
};
use floe_core::{run, set_observer, RunEvent, RunObserver, RunOptions};

#[derive(Default)]
struct TestObserver {
    events: Mutex<Vec<RunEvent>>,
}

impl TestObserver {
    fn reset(&self) {
        self.events.lock().expect("observer lock").clear();
    }

    fn events_for_run(&self, run_id: &str) -> Vec<RunEvent> {
        self.events
            .lock()
            .expect("observer lock")
            .iter()
            .filter(|event| match event {
                RunEvent::Log {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::RunStarted {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::EntityStarted {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::FileStarted {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::FileFinished {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::SchemaEvolutionApplied {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::EntityFinished {
                    run_id: event_run_id,
                    ..
                }
                | RunEvent::RunFinished {
                    run_id: event_run_id,
                    ..
                } => event_run_id == run_id,
            })
            .cloned()
            .collect()
    }
}

impl RunObserver for TestObserver {
    fn on_event(&self, event: RunEvent) {
        self.events.lock().expect("observer lock").push(event);
    }
}

fn test_observer() -> &'static TestObserver {
    static OBSERVER: OnceLock<Arc<TestObserver>> = OnceLock::new();
    let observer = OBSERVER.get_or_init(|| {
        let observer = Arc::new(TestObserver::default());
        let _ = set_observer(observer.clone());
        observer
    });
    observer.as_ref()
}

fn write_csv(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write csv");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

fn config_yaml(
    input_dir: &Path,
    accepted_dir: &Path,
    rejected_dir: Option<&Path>,
    report_dir: &Path,
    severity: &str,
    mismatch_block: &str,
    write_mode: Option<&str>,
) -> String {
    let rejected_block = rejected_dir
        .map(|path| {
            format!(
                "      rejected:\n        format: \"csv\"\n        path: \"{}\"\n",
                path.display()
            )
        })
        .unwrap_or_default();
    let write_mode_block = write_mode
        .map(|mode| format!("      write_mode: \"{mode}\"\n"))
        .unwrap_or_default();
    format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
{write_mode_block}{rejected_block}    policy:
      severity: "{severity}"
    schema:
{mismatch_block}      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        write_mode_block = write_mode_block,
        rejected_block = rejected_block,
        severity = severity,
        mismatch_block = mismatch_block,
    )
}

fn list_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(dir)
        .expect("read dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn run_config(path: &Path, run_id: &str) -> floe_core::RunOutcome {
    run(
        path,
        RunOptions { profile: None,
            run_id: Some(run_id.to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config")
}

fn state_path(input_dir: &Path) -> PathBuf {
    input_dir.join(".floe/state/customer/state.json")
}

#[test]
fn incremental_file_mode_skips_seen_files_and_commits_once() {
    let observer = test_observer();
    observer.reset();

    let root = tempfile::TempDir::new().expect("temp dir");
    let input_dir = root.path().join("in");
    let accepted_dir = root.path().join("out/accepted");
    let report_dir = root.path().join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "customers.csv", "id;name\n1;alice\n");
    let config_path = write_config(
        root.path(),
        &config_yaml(
            &input_dir,
            &accepted_dir,
            None,
            &report_dir,
            "warn",
            "",
            None,
        ),
    );

    let first = run_config(&config_path, "incremental-first");
    assert_eq!(first.summary.run.status, RunStatus::Success);
    let state_file = state_path(&input_dir);
    let state = read_entity_state(&state_file)
        .expect("read state")
        .expect("state exists");
    assert_eq!(state.files.len(), 1);
    let recorded = state.files.values().next().expect("recorded file");
    assert_eq!(recorded.size, Some(16));
    assert!(recorded.processed_at.contains('T'));

    let second = run_config(&config_path, "incremental-second");
    assert_eq!(second.summary.run.status, RunStatus::Success);
    let report = &second.entity_outcomes[0].report;
    assert_eq!(report.results.files_total, 1);
    assert_eq!(report.results.rows_total, 0);
    assert_eq!(report.files[0].status, FileStatus::Success);
    assert_eq!(report.files[0].validation.warnings, 0);
    let second_state = read_entity_state(&state_file)
        .expect("read state again")
        .expect("state exists again");
    assert_eq!(second_state.files, state.files);
}

#[test]
fn incremental_file_mode_warns_and_skips_changed_files() {
    let observer = test_observer();
    observer.reset();

    let root = tempfile::TempDir::new().expect("temp dir");
    let input_dir = root.path().join("in");
    let accepted_dir = root.path().join("out/accepted");
    let report_dir = root.path().join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    let source = write_csv(&input_dir, "customers.csv", "id;name\n1;alice\n");
    let config_path = write_config(
        root.path(),
        &config_yaml(
            &input_dir,
            &accepted_dir,
            None,
            &report_dir,
            "warn",
            "",
            None,
        ),
    );

    let _ = run_config(&config_path, "incremental-changed-first");
    sleep(Duration::from_secs(1));
    fs::write(&source, "id;name\n1;alice\n2;bob\n").expect("rewrite csv");

    let outcome = run_config(&config_path, "incremental-changed-second");
    assert_eq!(outcome.summary.run.status, RunStatus::SuccessWithWarnings);
    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.files_total, 1);
    assert_eq!(report.results.rows_total, 0);
    assert_eq!(report.results.warnings_total, 1);
    assert_eq!(report.files[0].validation.warnings, 1);
    let warning = report.files[0]
        .mismatch
        .warning
        .as_deref()
        .expect("warning text");
    assert!(warning.contains("changed metadata"));

    let events = observer.events_for_run("incremental-changed-second");
    assert!(events.iter().any(|event| matches!(
        event,
        RunEvent::Log {
            code: Some(code),
            message,
            ..
        } if code == "incremental_file_changed" && message.contains("changed metadata")
    )));
}

#[test]
fn incremental_file_mode_accepts_matching_state_file() {
    let root = tempfile::TempDir::new().expect("temp dir");
    let input_dir = root.path().join("in");
    let accepted_dir = root.path().join("out/accepted");
    let report_dir = root.path().join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "customers.csv", "id;name\n1;alice\n");
    let state_file = state_path(&input_dir);
    write_entity_state_atomic(&state_file, &EntityState::new("customer")).expect("write state");
    let config_path = write_config(
        root.path(),
        &config_yaml(
            &input_dir,
            &accepted_dir,
            None,
            &report_dir,
            "warn",
            "",
            None,
        ),
    );

    let outcome = run_config(&config_path, "incremental-matching-state");
    assert_eq!(outcome.summary.run.status, RunStatus::Success);
    assert_eq!(outcome.entity_outcomes[0].report.results.rows_total, 1);
}

#[test]
fn incremental_file_mode_fails_on_mismatched_state_entity() {
    let root = tempfile::TempDir::new().expect("temp dir");
    let input_dir = root.path().join("in");
    let accepted_dir = root.path().join("out/accepted");
    let report_dir = root.path().join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "customers.csv", "id;name\n1;alice\n");
    let state_file = state_path(&input_dir);
    write_entity_state_atomic(&state_file, &EntityState::new("orders")).expect("write state");
    let config_path = write_config(
        root.path(),
        &config_yaml(
            &input_dir,
            &accepted_dir,
            None,
            &report_dir,
            "warn",
            "",
            None,
        ),
    );

    let err = run(
        &config_path,
        RunOptions { profile: None,
            run_id: Some("incremental-entity-mismatch".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect_err("mismatched entity state should fail");
    let msg = err.to_string();
    assert!(msg.contains("state entity mismatch"));
    assert!(msg.contains("expected customer, got orders"));
}

#[test]
fn incremental_file_mode_fails_on_mismatched_state_schema() {
    let root = tempfile::TempDir::new().expect("temp dir");
    let input_dir = root.path().join("in");
    let accepted_dir = root.path().join("out/accepted");
    let report_dir = root.path().join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "customers.csv", "id;name\n1;alice\n");
    let state_file = state_path(&input_dir);
    write_entity_state_atomic(
        &state_file,
        &EntityState {
            schema: "floe.state.file-ingest.v0".to_string(),
            entity: "customer".to_string(),
            updated_at: None,
            files: Default::default(),
        },
    )
    .expect("write state");
    let config_path = write_config(
        root.path(),
        &config_yaml(
            &input_dir,
            &accepted_dir,
            None,
            &report_dir,
            "warn",
            "",
            None,
        ),
    );

    let err = run(
        &config_path,
        RunOptions { profile: None,
            run_id: Some("incremental-schema-mismatch".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect_err("mismatched schema state should fail");
    let msg = err.to_string();
    assert!(msg.contains("state schema mismatch"));
    assert!(msg.contains(ENTITY_STATE_SCHEMA_V1));
    assert!(msg.contains("floe.state.file-ingest.v0"));
}

#[test]
fn incremental_file_mode_overwrite_noop_preserves_accepted_output() {
    let root = tempfile::TempDir::new().expect("temp dir");
    let input_dir = root.path().join("in");
    let accepted_dir = root.path().join("out/accepted");
    let report_dir = root.path().join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "customers.csv", "id;name\n1;alice\n");
    let config_path = write_config(
        root.path(),
        &config_yaml(
            &input_dir,
            &accepted_dir,
            None,
            &report_dir,
            "warn",
            "",
            Some("overwrite"),
        ),
    );

    let first = run_config(&config_path, "incremental-overwrite-first");
    assert_eq!(first.summary.run.status, RunStatus::Success);
    let first_outputs = list_files(&accepted_dir);
    assert!(!first_outputs.is_empty());
    let first_sizes = first_outputs
        .iter()
        .map(|path| fs::metadata(path).expect("metadata").len())
        .collect::<Vec<_>>();

    let second = run_config(&config_path, "incremental-overwrite-second");
    assert_eq!(second.summary.run.status, RunStatus::Success);
    let report = &second.entity_outcomes[0].report;
    assert_eq!(report.results.rows_total, 0);
    assert_eq!(report.results.accepted_total, 0);

    let second_outputs = list_files(&accepted_dir);
    assert_eq!(second_outputs, first_outputs);
    let second_sizes = second_outputs
        .iter()
        .map(|path| fs::metadata(path).expect("metadata").len())
        .collect::<Vec<_>>();
    assert_eq!(second_sizes, first_sizes);
}

#[test]
fn incremental_file_mode_does_not_commit_state_after_unsuccessful_entity() {
    let root = tempfile::TempDir::new().expect("temp dir");
    let input_dir = root.path().join("in");
    let accepted_dir = root.path().join("out/accepted");
    let rejected_dir = root.path().join("out/rejected");
    let report_dir = root.path().join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "customers.csv", "id\n1\n");
    let mismatch_block = "      mismatch:\n        missing_columns: \"reject_file\"\n";
    let config_path = write_config(
        root.path(),
        &config_yaml(
            &input_dir,
            &accepted_dir,
            Some(&rejected_dir),
            &report_dir,
            "reject",
            mismatch_block,
            None,
        ),
    );

    let outcome = run_config(&config_path, "incremental-rejected");
    assert_eq!(outcome.summary.run.status, RunStatus::Rejected);
    assert!(read_entity_state(&state_path(&input_dir))
        .expect("read state")
        .is_none());
}
