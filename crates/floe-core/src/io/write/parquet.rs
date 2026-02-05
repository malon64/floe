use std::path::Path;

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter};

use crate::errors::IoError;
use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteOutput};
use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

use super::parts;

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
        mode: config::WriteMode,
        _output_stem: &str,
        temp_dir: Option<&Path>,
        cloud: &mut io::storage::CloudClient,
        resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        let mut part_allocator = prepare_part_allocator(target, mode, cloud, resolver, entity)?;
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
            while offset < total_rows {
                let len = std::cmp::min(max_rows, total_rows - offset);
                let mut chunk = df.slice(offset as i64, len);
                let part_filename = part_allocator.allocate_next();
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
                offset += len;
            }
        } else {
            let part_filename = part_allocator.allocate_next();
            io::storage::output::write_output(
                target,
                io::storage::output::OutputPlacement::Directory,
                &part_filename,
                temp_dir,
                cloud,
                resolver,
                entity,
                |path| write_parquet_to_path(df, path, options),
            )?;
            parts_written = 1;
            part_files.push(part_filename);
        }

        Ok(AcceptedWriteOutput {
            parts_written,
            part_files,
            table_version: None,
        })
    }
}

fn prepare_part_allocator(
    target: &Target,
    mode: config::WriteMode,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<parts::PartNameAllocator> {
    match mode {
        config::WriteMode::Overwrite => {
            clear_output_parts(target, cloud, resolver, entity)?;
            Ok(parts::PartNameAllocator::from_next_index(0, "parquet"))
        }
        config::WriteMode::Append => match target {
            Target::Local { base_path, .. } => {
                parts::PartNameAllocator::from_local_path(Path::new(base_path), "parquet")
            }
            Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
                let next_index = next_cloud_part_index(target, cloud, resolver, entity)?;
                Ok(parts::PartNameAllocator::from_next_index(
                    next_index, "parquet",
                ))
            }
        },
    }
}

fn clear_output_parts(
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<()> {
    match target {
        Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let _ = parts::clear_local_part_files(base_path, "parquet")?;
        }
        Target::S3 {
            storage, base_key, ..
        } => {
            clear_s3_output_prefix(cloud, resolver, entity, storage, base_key)?;
        }
        Target::Gcs {
            storage,
            bucket,
            base_key,
            ..
        } => {
            clear_gcs_output_prefix(cloud, resolver, entity, storage, bucket, base_key)?;
        }
        Target::Adls {
            storage,
            container,
            account,
            base_path,
            ..
        } => {
            clear_adls_output_prefix(
                cloud, resolver, entity, storage, container, account, base_path,
            )?;
        }
    }
    Ok(())
}

fn next_cloud_part_index(
    target: &Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<usize> {
    match target {
        Target::Local { .. } => Ok(0),
        Target::S3 {
            storage, base_key, ..
        } => {
            let list_prefix = s3_list_prefix(entity, base_key)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let keys = client.list(&list_prefix)?;
            next_part_index_from_objects(keys.into_iter().filter_map(|obj| {
                if obj.key.starts_with(&list_prefix) {
                    parse_part_index_from_key(&obj.key)
                } else {
                    None
                }
            }))
        }
        Target::Gcs {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let list_prefix = gcs_list_prefix(entity, bucket, base_key)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let keys = client.list(&list_prefix)?;
            next_part_index_from_objects(keys.into_iter().filter_map(|obj| {
                if obj.key.starts_with(&list_prefix) {
                    parse_part_index_from_key(&obj.key)
                } else {
                    None
                }
            }))
        }
        Target::Adls {
            storage,
            container,
            account,
            base_path,
            ..
        } => {
            let list_prefix = adls_list_prefix(entity, container, account, base_path)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let keys = client.list(&list_prefix)?;
            next_part_index_from_objects(keys.into_iter().filter_map(|obj| {
                if obj.key.starts_with(&list_prefix) {
                    parse_part_index_from_key(&obj.key)
                } else {
                    None
                }
            }))
        }
    }
}

fn clear_s3_output_prefix(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    base_key: &str,
) -> FloeResult<()> {
    let list_prefix = s3_list_prefix(entity, base_key)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(&list_prefix)?;
    for object in keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(&list_prefix))
        .filter(|obj| parse_part_index_from_key(&obj.key).is_some())
    {
        client.delete_object(&object.uri)?;
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
) -> FloeResult<()> {
    let list_prefix = gcs_list_prefix(entity, bucket, base_key)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(&list_prefix)?;
    for object in keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(&list_prefix))
        .filter(|obj| parse_part_index_from_key(&obj.key).is_some())
    {
        client.delete_object(&object.uri)?;
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
) -> FloeResult<()> {
    let list_prefix = adls_list_prefix(entity, container, account, base_path)?;
    let client = cloud.client_for(resolver, storage, entity)?;
    let keys = client.list(&list_prefix)?;
    for object in keys
        .into_iter()
        .filter(|obj| obj.key.starts_with(&list_prefix))
        .filter(|obj| parse_part_index_from_key(&obj.key).is_some())
    {
        client.delete_object(&object.uri)?;
    }
    Ok(())
}

fn parse_part_index_from_key(key: &str) -> Option<usize> {
    let file_name = Path::new(key).file_name()?.to_str()?;
    let stem = file_name.strip_suffix(".parquet")?;
    let index = stem.strip_prefix("part-")?;
    if index.is_empty() || !index.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    index.parse::<usize>().ok()
}

fn next_part_index_from_objects(indexes: impl IntoIterator<Item = usize>) -> FloeResult<usize> {
    match indexes.into_iter().max() {
        Some(index) => index.checked_add(1).ok_or_else(|| {
            Box::new(ConfigError(
                "parquet part index overflow while preparing append write".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        }),
        None => Ok(0),
    }
}

fn s3_list_prefix(entity: &config::EntityConfig, base_key: &str) -> FloeResult<String> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for s3 parquet outputs",
            entity.name
        ))));
    }
    Ok(format!("{prefix}/"))
}

fn gcs_list_prefix(
    entity: &config::EntityConfig,
    bucket: &str,
    base_key: &str,
) -> FloeResult<String> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be bucket root for gcs parquet outputs (bucket={})",
            entity.name, bucket
        ))));
    }
    Ok(format!("{prefix}/"))
}

fn adls_list_prefix(
    entity: &config::EntityConfig,
    container: &str,
    account: &str,
    base_path: &str,
) -> FloeResult<String> {
    let prefix = base_path.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.accepted.path must not be container root for adls parquet outputs (container={}, account={})",
            entity.name, container, account
        ))));
    }
    Ok(format!("{prefix}/"))
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
