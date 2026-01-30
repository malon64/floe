use std::path::Path;

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter};

use crate::errors::IoError;
use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteOutput};
use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

struct ParquetAcceptedAdapter;

static PARQUET_ACCEPTED_ADAPTER: ParquetAcceptedAdapter = ParquetAcceptedAdapter;
const DEFAULT_MAX_SIZE_PER_FILE_BYTES: u64 = 256 * 1024 * 1024;

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
    ) -> FloeResult<AcceptedWriteOutput> {
        let filename = io::storage::paths::build_output_filename(output_stem, "", "parquet");
        match target {
            Target::Local { base_path, .. } => {
                clear_local_output_dir(base_path)?;
            }
            Target::S3 {
                storage, base_key, ..
            } => {
                clear_s3_output_prefix(cloud, resolver, entity, storage, base_key, &filename)?;
            }
            Target::Gcs {
                storage,
                bucket,
                base_key,
                ..
            } => {
                clear_gcs_output_prefix(
                    cloud, resolver, entity, storage, bucket, base_key, &filename,
                )?;
            }
            Target::Adls {
                storage,
                container,
                account,
                base_path,
                ..
            } => {
                clear_adls_output_prefix(
                    cloud, resolver, entity, storage, container, account, base_path, &filename,
                )?;
            }
        }
        let options = entity.sink.accepted.options.as_ref();
        let max_size_per_file = options
            .and_then(|options| options.max_size_per_file)
            .unwrap_or(DEFAULT_MAX_SIZE_PER_FILE_BYTES);
        let mut parts_written = 0;
        let mut part_files = Vec::new();
        let total_rows = df.height();
        if total_rows > 0 {
            let estimated_size = df.estimated_size() as u64;
            let avg_row_size = if estimated_size == 0 {
                1
            } else {
                estimated_size.div_ceil(total_rows as u64)
            };
            let max_rows = std::cmp::max(1, max_size_per_file / avg_row_size) as usize;
            let mut offset = 0usize;
            let mut part_index = 0usize;
            while offset < total_rows {
                let len = std::cmp::min(max_rows, total_rows - offset);
                let mut chunk = df.slice(offset as i64, len);
                let part_stem = io::storage::paths::build_part_stem(part_index);
                let part_filename =
                    io::storage::paths::build_output_filename(&part_stem, "", "parquet");
                io::storage::output::write_output(
                    target,
                    io::storage::output::OutputPlacement::Directory,
                    &part_filename,
                    temp_dir,
                    cloud,
                    resolver,
                    entity,
                    |path| write_parquet_to_path(&mut chunk, path, options),
                )?;
                if part_files.len() < 50 {
                    part_files.push(part_filename);
                }
                parts_written += 1;
                part_index += 1;
                offset += len;
            }
        } else {
            io::storage::output::write_output(
                target,
                io::storage::output::OutputPlacement::Directory,
                &filename,
                temp_dir,
                cloud,
                resolver,
                entity,
                |path| write_parquet_to_path(df, path, options),
            )?;
            parts_written = 1;
            part_files.push(filename);
        }

        Ok(AcceptedWriteOutput {
            parts_written,
            part_files,
            table_version: None,
        })
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
    let keys = client.list(prefix)?;
    if keys.is_empty() {
        return Ok(());
    }
    let sample = io::storage::paths::resolve_output_dir_key(prefix, sample_filename);
    let keys = keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(prefix) && !obj.key.is_empty())
        .collect::<Vec<_>>();
    if keys.len() == 1 && keys[0].key == sample {
        return Ok(());
    }
    for object in keys {
        client.delete(&object.uri)?;
    }
    Ok(())
}

fn clear_gcs_output_prefix(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    bucket: &str,
    base_key: &str,
    sample_filename: &str,
) -> FloeResult<()> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for gcs outputs (bucket={})",
            entity.name, bucket
        ))));
    }
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(prefix)?;
    if keys.is_empty() {
        return Ok(());
    }
    let sample = io::storage::paths::resolve_output_dir_key(prefix, sample_filename);
    let keys = keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(prefix) && !obj.key.is_empty())
        .collect::<Vec<_>>();
    if keys.len() == 1 && keys[0].key == sample {
        return Ok(());
    }
    for object in keys {
        client.delete(&object.uri)?;
    }
    Ok(())
}

fn clear_adls_output_prefix(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    container: &str,
    account: &str,
    base_path: &str,
    sample_filename: &str,
) -> FloeResult<()> {
    let prefix = base_path.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be container root for adls outputs (container={}, account={})",
            entity.name, container, account
        ))));
    }
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(prefix)?;
    if keys.is_empty() {
        return Ok(());
    }
    let sample = io::storage::paths::resolve_output_dir_key(prefix, sample_filename);
    let keys = keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(prefix) && !obj.key.is_empty())
        .collect::<Vec<_>>();
    if keys.len() == 1 && keys[0].key == sample {
        return Ok(());
    }
    for object in keys {
        client.delete(&object.uri)?;
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
