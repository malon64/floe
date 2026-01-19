use std::path::{Path, PathBuf};

use polars::prelude::{CsvWriter, DataFrame, ParquetWriter, SerWriter};

use crate::{ConfigError, FloeResult};

pub fn write_parquet(
    df: &mut DataFrame,
    base_path: &str,
    source_stem: &str,
) -> FloeResult<PathBuf> {
    let output_path = build_parquet_path(base_path, source_stem);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&output_path)?;
    ParquetWriter::new(file)
        .finish(df)
        .map_err(|err| Box::new(ConfigError(format!("parquet write failed: {err}"))))?;
    Ok(output_path)
}

pub fn write_rejected_csv(
    df: &mut DataFrame,
    base_path: &str,
    source_stem: &str,
) -> FloeResult<PathBuf> {
    let output_path = build_rejected_path(base_path, source_stem);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&output_path)?;
    CsvWriter::new(file)
        .finish(df)
        .map_err(|err| Box::new(ConfigError(format!("rejected csv write failed: {err}"))))?;
    Ok(output_path)
}

pub fn write_error_report(
    base_path: &str,
    source_stem: &str,
    errors_per_row: &[Option<String>],
) -> FloeResult<PathBuf> {
    let output_path = build_reject_errors_path(base_path, source_stem);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut items = Vec::new();
    for (idx, err) in errors_per_row.iter().enumerate() {
        if let Some(err) = err {
            items.push(format!(
                "{{\"row_index\":{},\"errors\":{}}}",
                idx, err
            ));
        }
    }
    let content = format!("[{}]", items.join(","));
    std::fs::write(&output_path, content)?;
    Ok(output_path)
}

pub fn write_rejected_raw(
    source_path: &Path,
    base_path: &str,
) -> FloeResult<PathBuf> {
    let output_path = build_rejected_raw_path(base_path, source_path);
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
    let file_name = source_path
        .file_name()
        .ok_or_else(|| {
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

fn build_parquet_path(base_path: &str, source_stem: &str) -> PathBuf {
    let path = Path::new(base_path);
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join(format!("{source_stem}.parquet"))
    }
}

fn build_rejected_path(base_path: &str, source_stem: &str) -> PathBuf {
    let path = Path::new(base_path);
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join(format!("{source_stem}_rejected.csv"))
    }
}

fn build_reject_errors_path(base_path: &str, source_stem: &str) -> PathBuf {
    let base = Path::new(base_path);
    let dir = if base.extension().is_some() {
        base.parent().unwrap_or(base)
    } else {
        base
    };
    dir.join(format!("{source_stem}_reject_errors.json"))
}

fn build_rejected_raw_path(base_path: &str, source_path: &Path) -> PathBuf {
    let base = Path::new(base_path);
    if base.extension().is_some() {
        base.to_path_buf()
    } else {
        let file_name = source_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("rejected.csv"));
        base.join(file_name)
    }
}
