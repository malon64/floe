use std::path::{Path, PathBuf};

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter};

use crate::io::format::AcceptedSinkAdapter;
use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

struct ParquetAcceptedAdapter;

static PARQUET_ACCEPTED_ADAPTER: ParquetAcceptedAdapter = ParquetAcceptedAdapter;

pub(crate) fn parquet_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &PARQUET_ACCEPTED_ADAPTER
}

pub fn write_parquet(
    df: &mut DataFrame,
    base_path: &str,
    source_stem: &str,
    options: Option<&config::SinkOptions>,
) -> FloeResult<PathBuf> {
    let output_path = build_parquet_path(base_path, source_stem);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&output_path)?;
    let mut writer = ParquetWriter::new(file);
    if let Some(options) = options {
        if let Some(compression) = &options.compression {
            writer = writer.with_compression(parse_parquet_compression(compression)?);
        }
        if let Some(row_group_size) = options.row_group_size {
            let row_group_size = usize::try_from(row_group_size).map_err(|_| {
                Box::new(ConfigError(format!(
                    "parquet row_group_size is too large: {row_group_size}"
                )))
            })?;
            writer = writer.with_row_group_size(Some(row_group_size));
        }
    }
    writer
        .finish(df)
        .map_err(|err| Box::new(ConfigError(format!("parquet write failed: {err}"))))?;
    Ok(output_path)
}

impl AcceptedSinkAdapter for ParquetAcceptedAdapter {
    fn write_accepted(
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
                let output_path = write_parquet(
                    df,
                    base_path,
                    source_stem,
                    entity.sink.accepted.options.as_ref(),
                )?;
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
                let local_path = write_parquet(
                    df,
                    &temp_base,
                    source_stem,
                    entity.sink.accepted.options.as_ref(),
                )?;
                let key = io::fs::s3::build_parquet_key(base_key, source_stem);
                let client = cloud.client_for(resolver, storage, entity)?;
                client.upload(&key, &local_path)?;
                Ok(io::fs::s3::format_s3_uri(bucket, &key))
            }
        }
    }
}

fn build_parquet_path(base_path: &str, source_stem: &str) -> PathBuf {
    let path = Path::new(base_path);
    if path.extension().is_some() {
        path.to_path_buf()
    } else {
        path.join(format!("{source_stem}.parquet"))
    }
}

fn parse_parquet_compression(value: &str) -> FloeResult<ParquetCompression> {
    match value {
        "snappy" => Ok(ParquetCompression::Snappy),
        "gzip" => Ok(ParquetCompression::Gzip(None)),
        "zstd" => Ok(ParquetCompression::Zstd(None)),
        "uncompressed" => Ok(ParquetCompression::Uncompressed),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported parquet compression: {value}"
        )))),
    }
}
