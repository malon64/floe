use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::config;
use floe_core::io::storage::extensions::glob_patterns_for_format;
use floe_core::io::storage::local::{resolve_local_inputs, LocalClient};
use floe_core::io::storage::StorageClient;

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.push(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, contents).expect("write file");
}

fn default_globs(format: &str) -> Vec<String> {
    glob_patterns_for_format(format).expect("default globs")
}

fn source_config(
    format: &str,
    path: &Path,
    options: Option<config::SourceOptions>,
) -> config::SourceConfig {
    config::SourceConfig {
        format: format.to_string(),
        path: path.display().to_string(),
        storage: None,
        options,
        cast_mode: None,
    }
}

#[test]
fn default_glob_filters_by_format() {
    let root = temp_dir("floe-resolve-default-glob");
    write_file(&root.join("a.csv"), "id\n1\n");
    write_file(&root.join("B.CSV"), "id\n2\n");
    write_file(&root.join("c.txt"), "id\n3\n");
    let source = source_config("csv", &root, None);
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 2);
}

#[test]
fn glob_override_is_used() {
    let root = temp_dir("floe-resolve-glob-override");
    write_file(&root.join("a.csv"), "id\n1\n");
    write_file(&root.join("b.data"), "id\n2\n");
    let options = config::SourceOptions {
        glob: Some("*.data".to_string()),
        ..Default::default()
    };
    let source = source_config("csv", &root, Some(options));
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 1);
    assert!(resolved.files[0].to_string_lossy().ends_with("b.data"));
}

#[test]
fn recursive_lists_nested_files() {
    let root = temp_dir("floe-resolve-recursive");
    write_file(&root.join("nested/a.csv"), "id\n1\n");
    let options = config::SourceOptions {
        recursive: Some(true),
        ..Default::default()
    };
    let source = source_config("csv", &root, Some(options));
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 1);
}

#[test]
fn glob_path_input_is_resolved() {
    let root = temp_dir("floe-resolve-glob-input");
    write_file(&root.join("a.csv"), "id\n1\n");
    let pattern = root.join("*.csv");
    let source = source_config("csv", &pattern, None);
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 1);
}

#[test]
fn list_is_sorted() {
    let root = temp_dir("floe-resolve-sorted");
    write_file(&root.join("b.csv"), "id\n1\n");
    write_file(&root.join("a.csv"), "id\n2\n");
    let source = source_config("csv", &root, None);
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert!(resolved.files[0].to_string_lossy().ends_with("a.csv"));
    assert!(resolved.files[1].to_string_lossy().ends_with("b.csv"));
}

#[test]
fn missing_path_errors() {
    let root = temp_dir("floe-resolve-missing");
    let missing = root.join("missing");
    let source = source_config("csv", &missing, None);
    let err = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect_err("error");
    assert!(err.to_string().contains("entity.name=customers"));
}

#[test]
fn local_client_upload_copies_file() {
    let root = temp_dir("floe-local-upload");
    let src = root.join("src.txt");
    let dest = root.join("dest.txt");
    write_file(&src, "hello");
    let client = LocalClient::new();
    client
        .upload_from_path(&src, dest.to_string_lossy().as_ref())
        .expect("upload");
    assert_eq!(fs::read_to_string(dest).expect("read"), "hello");
}

#[test]
fn local_client_download_copies_file() {
    let root = temp_dir("floe-local-download");
    let src = root.join("src.txt");
    let dest_dir = root.join("dest");
    write_file(&src, "hello");
    let client = LocalClient::new();
    let downloaded = client
        .download_to_temp(src.to_string_lossy().as_ref(), &dest_dir)
        .expect("download");
    assert_eq!(fs::read_to_string(downloaded).expect("read"), "hello");
}
