use std::path::Path;

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter};

use crate::errors::IoError;
use crate::io::format::AcceptedSinkAdapter;
use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

struct ParquetAcceptedAdapter;

static PARQUET_ACCEPTED_ADAPTER: ParquetAcceptedAdapter = ParquetAcceptedAdapter;

pub(crate) fn parquet_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &PARQUET_ACCEPTED_ADAPTER
}

pub fn write_parquet_to_path(
    df: &mut DataFrame,
    output_path: &Path,
    options: Option<&config::SinkOptions>,
) -> FloeResult<()> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(output_path)?;
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
        .map_err(|err| Box::new(IoError(format!("parquet write failed: {err}"))))?;
    Ok(())
}

impl AcceptedSinkAdapter for ParquetAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &Target,
        df: &mut DataFrame,
        output_stem: &str,
        temp_dir: Option<&Path>,
        cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String> {
        let filename = io::storage::paths::build_output_filename(output_stem, "", "parquet");
        if let Target::Local { base_path, .. } = target {
            clear_local_output_dir(base_path)?;
        } else if let Target::S3 {
            storage, base_key, ..
        } = target
        {
            clear_s3_output_prefix(cloud, resolver, entity, storage, base_key, &filename)?;
        }
        io::storage::output::write_output(
            target,
            io::storage::output::OutputPlacement::Directory,
            &filename,
            temp_dir,
            cloud,
            resolver,
            entity,
            |path| write_parquet_to_path(df, path, entity.sink.accepted.options.as_ref()),
        )
    }
}

fn clear_local_output_dir(base_path: &str) -> FloeResult<()> {
    let path = Path::new(base_path);
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    if path.exists() {
        if path.is_file() {
            std::fs::remove_file(path)?;
        } else {
            std::fs::remove_dir_all(path)?;
        }
    }
    std::fs::create_dir_all(path)?;
    Ok(())
}

fn clear_s3_output_prefix(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    base_key: &str,
    sample_filename: &str,
) -> FloeResult<()> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for s3 outputs",
            entity.name
        ))));
    }
    let client = cloud.client_for(resolver, storage, entity)?;
    let mut keys = client.list(prefix)?;
    if keys.is_empty() {
        return Ok(());
    }
    let sample = io::storage::paths::resolve_output_dir_key(prefix, sample_filename);
    keys.retain(|key| key.starts_with(prefix) && !key.is_empty());
    if keys.len() == 1 && keys[0] == sample {
        return Ok(());
    }
    for key in keys {
        client.delete(&key)?;
    }
    Ok(())
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
