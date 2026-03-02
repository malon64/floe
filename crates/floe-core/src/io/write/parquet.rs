use std::path::Path;

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter};

use crate::errors::IoError;
use crate::io::format::{AcceptedSinkAdapter, AcceptedWriteOutput};
use crate::io::storage::Target;
use crate::{config, io, ConfigError, FloeResult};

use super::{metrics, strategy};

struct ParquetAcceptedAdapter;

static PARQUET_ACCEPTED_ADAPTER: ParquetAcceptedAdapter = ParquetAcceptedAdapter;
const DEFAULT_MAX_SIZE_PER_FILE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetWriteRuntimeOptions {
    pub max_size_per_file_bytes: u64,
    pub small_file_threshold_bytes: u64,
}

pub(crate) fn parquet_accepted_adapter() -> &'static dyn AcceptedSinkAdapter {
    &PARQUET_ACCEPTED_ADAPTER
}

pub fn parquet_write_runtime_options(target: &config::SinkTarget) -> ParquetWriteRuntimeOptions {
    let max_size_per_file_bytes = target
        .options
        .as_ref()
        .and_then(|options| options.max_size_per_file)
        .unwrap_or(DEFAULT_MAX_SIZE_PER_FILE_BYTES);
    ParquetWriteRuntimeOptions {
        max_size_per_file_bytes,
        small_file_threshold_bytes: metrics::default_small_file_threshold_bytes(Some(
            max_size_per_file_bytes,
        )),
    }
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
        _catalogs: &config::CatalogResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<AcceptedWriteOutput> {
        let mut ctx = strategy::WriteContext {
            target,
            cloud,
            resolver,
            entity,
        };
        let spec = strategy::accepted_parquet_spec();
        let mut part_allocator = strategy::strategy_for(mode).part_allocator(&mut ctx, spec)?;
        let options = entity.sink.accepted.options.as_ref();
        let runtime_options = parquet_write_runtime_options(&entity.sink.accepted);
        let max_size_per_file = runtime_options.max_size_per_file_bytes;
        let mut parts_written = 0;
        let mut part_files = Vec::new();
        let mut file_sizes = Vec::new();
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
                    io::storage::OutputPlacement::Directory,
                    &part_filename,
                    temp_dir,
                    cloud,
                    resolver,
                    entity,
                    |path| write_parquet_to_path(&mut chunk, path, options),
                )?;
                if let Some(size) = stat_written_output_file_size(target, temp_dir, &part_filename)
                {
                    file_sizes.push(size);
                }
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
                io::storage::OutputPlacement::Directory,
                &part_filename,
                temp_dir,
                cloud,
                resolver,
                entity,
                |path| write_parquet_to_path(df, path, options),
            )?;
            if let Some(size) = stat_written_output_file_size(target, temp_dir, &part_filename) {
                file_sizes.push(size);
            }
            parts_written = 1;
            part_files.push(part_filename);
        }

        let metrics = metrics::summarize_written_file_sizes(
            &file_sizes,
            runtime_options.small_file_threshold_bytes,
        );

        Ok(AcceptedWriteOutput {
            files_written: parts_written,
            parts_written,
            part_files,
            table_version: None,
            snapshot_id: None,
            table_root_uri: None,
            iceberg_catalog_name: None,
            iceberg_database: None,
            iceberg_namespace: None,
            iceberg_table: None,
            metrics,
            merge: None,
        })
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

fn stat_written_output_file_size(
    target: &Target,
    temp_dir: Option<&Path>,
    filename: &str,
) -> Option<u64> {
    let path = match target {
        Target::Local { base_path, .. } => {
            crate::io::storage::paths::resolve_output_dir_path(base_path, filename)
        }
        Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => temp_dir?.join(filename),
    };
    std::fs::metadata(path).ok().map(|meta| meta.len())
}
