use std::fs;
use std::path::{Path, PathBuf};

use floe_core::{run, RunOptions};

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

fn archive_entity_dir(archive_root: &Path, entity: &str) -> PathBuf {
    archive_root.join(entity)
}

fn list_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if dir.exists() {
        for entry in fs::read_dir(dir).expect("read dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

#[test]
fn archive_moves_only_processed_input_not_local_siblings() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let archive_dir = root.join("archive");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    let processed = write_csv(&input_dir, "data.csv", "id,name\n1,alice\n");
    let sibling = write_csv(&input_dir, "sibling.csv", "id,name\n99,keep\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{processed}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      archive:
        path: "{archive_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        processed = processed.display(),
        accepted_dir = accepted_dir.display(),
        archive_dir = archive_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("archive-sibling-it".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    assert!(!processed.exists(), "processed file should be archived");
    assert!(
        sibling.exists(),
        "non-ingested sibling must remain in place"
    );

    let archived_files = list_files(&archive_entity_dir(&archive_dir, "customer"));
    assert_eq!(archived_files.len(), 1);
    let archived_name = archived_files[0]
        .file_name()
        .and_then(|name| name.to_str())
        .expect("archived filename");
    assert!(archived_name.starts_with("data__run-archive-sibling-it__src-"));
    assert!(archived_name.ends_with(".csv"));

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.files.len(), 1);
    let archived_path = report.files[0]
        .output
        .archived_path
        .as_ref()
        .expect("archived path in report");
    assert_eq!(archived_path, &archived_files[0].display().to_string());
}

#[test]
fn archive_repeated_runs_do_not_overwrite_same_source_filename() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let archive_dir = root.join("archive");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    let source_path = input_dir.join("data.csv");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{source_path}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      archive:
        path: "{archive_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        source_path = source_path.display(),
        accepted_dir = accepted_dir.display(),
        archive_dir = archive_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    fs::write(&source_path, "id,name\n1,first\n").expect("write first source");
    run(
        &config_path,
        RunOptions {
            run_id: Some("archive-run-1".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("first run");

    fs::write(&source_path, "id,name\n2,second\n").expect("write second source");
    run(
        &config_path,
        RunOptions {
            run_id: Some("archive-run-2".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("second run");

    let archived_files = list_files(&archive_entity_dir(&archive_dir, "customer"));
    assert_eq!(archived_files.len(), 2, "expected two archived artifacts");

    let names = archived_files
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .expect("name")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert!(names
        .iter()
        .any(|name| name.contains("__run-archive-run-1__src-")));
    assert!(names
        .iter()
        .any(|name| name.contains("__run-archive-run-2__src-")));

    let contents = archived_files
        .iter()
        .map(|path| fs::read_to_string(path).expect("read archived file"))
        .collect::<Vec<_>>();
    assert!(contents.iter().any(|content| content.contains("1,first")));
    assert!(contents.iter().any(|content| content.contains("2,second")));
}
