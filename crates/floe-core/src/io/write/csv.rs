use std::path::{Path, PathBuf};

use polars::prelude::{CsvWriter, DataFrame, SerWriter};

use crate::io::format::RejectedSinkAdapter;
use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

struct CsvRejectedAdapter;

static CSV_REJECTED_ADAPTER: CsvRejectedAdapter = CsvRejectedAdapter;

pub(crate) fn csv_rejected_adapter() -> &'static dyn RejectedSinkAdapter {
    &CSV_REJECTED_ADAPTER
}

pub fn write_rejected_csv(
    df: &mut DataFrame,
    base_path: &str,
    source_stem: &str,
) -> FloeResult<PathBuf> {
    let filename = io::storage::paths::build_output_filename(source_stem, "_rejected", "csv");
    let output_path = io::storage::paths::resolve_output_path(base_path, &filename);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&output_path)?;
    CsvWriter::new(file)
        .finish(df)
        .map_err(|err| Box::new(ConfigError(format!("rejected csv write failed: {err}"))))?;
    Ok(output_path)
}

impl RejectedSinkAdapter for CsvRejectedAdapter {
    fn write_rejected(
        &self,
        target: &Target,
        df: &mut DataFrame,
        source_stem: &str,
        _temp_dir: Option<&Path>,
        _cloud: &mut io::storage::CloudClient,
        _resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String> {
        match target {
            Target::Local { base_path, .. } => {
                let output_path = write_rejected_csv(df, base_path, source_stem)?;
                Ok(output_path.display().to_string())
            }
            Target::S3 { .. } => Err(Box::new(ConfigError(format!(
                "entity.name={} rejected csv writer does not handle s3 targets",
                entity.name
            )))),
        }
    }
}

// Filename construction is shared via io::storage::paths helpers.
