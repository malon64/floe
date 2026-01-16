use std::path::{Path, PathBuf};

use polars::prelude::{CsvWriter, DataFrame, ParquetWriter, SerWriter};

use crate::{ConfigError, FloeResult};

pub fn write_parquet(df: &mut DataFrame, base_path: &str, entity_name: &str) -> FloeResult<()> {
    let output_path = build_parquet_path(base_path, entity_name);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&output_path)?;
    ParquetWriter::new(file)
        .finish(df)
        .map_err(|err| Box::new(ConfigError(format!("parquet write failed: {err}"))))?;
    Ok(())
}

pub fn write_rejected_csv(
    df: &mut DataFrame,
    base_path: &str,
    entity_name: &str,
) -> FloeResult<()> {
    let output_path = build_rejected_path(base_path, entity_name);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&output_path)?;
    CsvWriter::new(file)
        .finish(df)
        .map_err(|err| Box::new(ConfigError(format!("rejected csv write failed: {err}"))))?;
    Ok(())
}

pub fn write_error_report(
    base_path: &str,
    entity_name: &str,
    errors_per_row: &[Option<String>],
) -> FloeResult<()> {
    let output_path = build_reject_errors_path(base_path, entity_name);
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
    std::fs::write(output_path, content)?;
    Ok(())
}

fn build_parquet_path(base_path: &str, entity_name: &str) -> PathBuf {
    let path = Path::new(base_path);
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join(format!("{entity_name}.parquet"))
    }
}

fn build_rejected_path(base_path: &str, entity_name: &str) -> PathBuf {
    let path = Path::new(base_path);
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join(format!("{entity_name}_rejected.csv"))
    }
}

fn build_reject_errors_path(base_path: &str, entity_name: &str) -> PathBuf {
    let base = Path::new(base_path);
    let dir = if base.extension().is_some() {
        base.parent().unwrap_or(base)
    } else {
        base
    };
    dir.join(format!("{entity_name}_reject_errors.json"))
}
