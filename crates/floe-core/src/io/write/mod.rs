pub mod csv;
pub mod delta;
pub mod iceberg;
pub mod parquet;

use std::path::{Path, PathBuf};

use crate::FloeResult;

pub fn write_error_report(
    output_path: &Path,
    errors_per_row: &[Option<String>],
) -> FloeResult<PathBuf> {
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
    std::fs::write(output_path, content)?;
    Ok(output_path.to_path_buf())
}

pub fn write_rejected_raw(source_path: &Path, output_path: &Path) -> FloeResult<PathBuf> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(source_path, output_path)?;
    Ok(output_path.to_path_buf())
}
