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

impl RejectedSinkAdapter for CsvRejectedAdapter {
    fn write_rejected(
        &self,
        target: &Target,
        df: &mut DataFrame,
        source_stem: &str,
        temp_dir: Option<&Path>,
        cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String> {
        match target {
            Target::Local { base_path, .. } => {
                let output_path = write_rejected_csv(df, base_path, source_stem)?;
                Ok(output_path.display().to_string())
            }
            Target::S3 {
                storage,
                bucket,
                base_key,
                ..
            } => {
                let temp_dir = temp_dir.ok_or_else(|| {
                    Box::new(ConfigError(format!(
                        "entity.name={} missing temp dir for s3 output",
                        entity.name
                    )))
                })?;
                let temp_base = temp_dir.display().to_string();
                let local_path = write_rejected_csv(df, &temp_base, source_stem)?;
                let key = io::storage::s3_paths::build_rejected_csv_key(base_key, source_stem);
                let client = cloud.client_for(resolver, storage, entity)?;
                client.upload(&key, &local_path)?;
                Ok(io::storage::s3_paths::format_s3_uri(bucket, &key))
            }
        }
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
