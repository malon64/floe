pub mod csv;
pub mod delta;
pub mod iceberg;
pub mod parquet;

use std::path::{Path, PathBuf};

use crate::{io, ConfigError, FloeResult};

pub fn write_error_report(
    base_path: &str,
    source_stem: &str,
    errors_per_row: &[Option<String>],
) -> FloeResult<PathBuf> {
    let filename = io::storage::paths::build_output_filename(source_stem, "_reject_errors", "json");
    let output_path = io::storage::paths::resolve_sibling_path(base_path, &filename);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut items = Vec::new();
    for (idx, err) in errors_per_row.iter().enumerate() {
        if let Some(err) = err {
            items.push(format!("{{\"row_index\":{},\"errors\":{}}}", idx, err));
        }
    }
    let content = format!("[{}]", items.join(","));
    std::fs::write(&output_path, content)?;
    Ok(output_path)
}

pub fn write_rejected_raw(source_path: &Path, base_path: &str) -> FloeResult<PathBuf> {
    let file_name = source_path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("rejected.csv"))
        .to_string_lossy()
        .to_string();
    let output_path = io::storage::paths::resolve_output_path(base_path, &file_name);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(source_path, &output_path)?;
    Ok(output_path)
}

pub fn archive_input(source_path: &Path, archive_dir: &Path) -> FloeResult<PathBuf> {
    if let Some(parent) = archive_dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(archive_dir)?;
    let file_name = source_path.file_name().ok_or_else(|| {
        Box::new(ConfigError("source file name missing".to_string()))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    let destination = archive_dir.join(file_name);
    if std::fs::rename(source_path, &destination).is_err() {
        std::fs::copy(source_path, &destination)?;
        std::fs::remove_file(source_path)?;
    }
    Ok(destination)
}
