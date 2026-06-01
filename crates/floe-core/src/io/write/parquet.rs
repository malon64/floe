use std::path::Path;

use polars::polars_utils::plpath::PlPathRef;
use polars::prelude::{
    DataFrame, IntoLazy, ParquetCompression, ParquetWriteOptions, SinkOptions as PolarsSinkOptions,
    SinkTarget,
};

use crate::checks::normalize::rename_output_columns;
use crate::errors::{IoError, StorageError};
use crate::io::format::{AcceptedWriteOutput, AcceptedWriteRequest};
use crate::io::read::parquet::read_parquet_lazy;
use crate::io::storage::Target;
use crate::io::write::sink_format::{SeedContext, SinkFormat};
use crate::io::write::{parts, strategy};
use crate::{check, config, io, ConfigError, FloeResult};

use super::metrics;

pub(crate) struct ParquetSinkFormat;

pub(crate) static PARQUET_SINK_FORMAT: ParquetSinkFormat = ParquetSinkFormat;
const DEFAULT_MAX_SIZE_PER_FILE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetWriteRuntimeOptions {
    pub max_size_per_file_bytes: u64,
    pub small_file_threshold_bytes: u64,
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
    let write_options = build_parquet_write_options(options)?;
    let sink_options = PolarsSinkOptions {
        mkdir: false,
        ..PolarsSinkOptions::default()
    };
    let target = SinkTarget::Path(PlPathRef::from_local_path(output_path).into_owned());
    // The outer chunking loop in `ParquetSinkFormat::write` discards each
    // chunk after the write, so taking the DataFrame here is safe. Using
    // `std::mem::take` lets us hand ownership to `LazyFrame` without an
    // extra clone of the Arrow buffers.
    let frame = std::mem::take(df);
    frame
        .lazy()
        .sink_parquet(target, write_options, None, sink_options)
        .and_then(|lf| lf.with_new_streaming(true).collect())
        .map_err(|err| Box::new(IoError(format!("parquet write failed: {err}"))))?;
    Ok(())
}

fn build_parquet_write_options(
    options: Option<&config::SinkOptions>,
) -> FloeResult<ParquetWriteOptions> {
    let mut write_options = ParquetWriteOptions::default();
    if let Some(options) = options {
        if let Some(compression) = &options.compression {
            write_options.compression = parse_parquet_compression(compression)?;
        }
        if let Some(row_group_size) = options.row_group_size {
            let row_group_size = usize::try_from(row_group_size).map_err(|_| {
                Box::new(ConfigError(format!(
                    "parquet row_group_size is too large: {row_group_size}"
                )))
            })?;
            write_options.row_group_size = Some(row_group_size);
        }
    }
    Ok(write_options)
}

impl SinkFormat for ParquetSinkFormat {
    fn format_name(&self) -> &'static str {
        "parquet"
    }

    fn supported_modes(&self) -> &'static [config::WriteMode] {
        &[config::WriteMode::Overwrite, config::WriteMode::Append]
    }

    fn supported_storages(&self) -> &'static [&'static str] {
        &["local", "s3", "gcs", "adls"]
    }

    fn write(&self, req: AcceptedWriteRequest<'_>) -> FloeResult<AcceptedWriteOutput> {
        let AcceptedWriteRequest {
            target,
            df,
            mode,
            temp_dir,
            cloud,
            resolver,
            entity,
            ..
        } = req;
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
            parts_written,
            runtime_options.small_file_threshold_bytes,
        );

        Ok(AcceptedWriteOutput {
            files_written: Some(parts_written),
            parts_written,
            part_files,
            table_version: None,
            snapshot_id: None,
            table_root_uri: None,
            catalog: None,
            metrics,
            merge: None,
            schema_evolution: io::format::AcceptedSchemaEvolution {
                enabled: false,
                mode: entity
                    .schema
                    .resolved_schema_evolution()
                    .mode
                    .as_str()
                    .to_string(),
                applied: false,
                added_columns: Vec::new(),
                incompatible_changes_detected: false,
            },
            perf: None,
        })
    }

    fn seed_unique_tracker(
        &self,
        tracker: &mut check::UniqueTracker,
        ctx: &mut SeedContext<'_>,
    ) -> FloeResult<()> {
        match ctx.target {
            Target::Local { base_path, .. } => {
                let base = Path::new(base_path);
                let part_files = parts::list_local_part_paths(base, "parquet")?;
                for part_path in part_files {
                    seed_from_parquet_path(tracker, &part_path, ctx.scan_cols, ctx.rename_back)?;
                }
            }
            Target::S3 { .. } | Target::Gcs { .. } | Target::Adls { .. } => {
                let temp_dir = ctx.temp_dir.ok_or_else(|| {
                    Box::new(StorageError(format!(
                        "entity.name={} missing temp dir for parquet seed",
                        ctx.entity.name
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?;
                let spec = strategy::accepted_parquet_spec();
                let (list_prefix, objects) = {
                    let mut wctx = strategy::WriteContext {
                        target: ctx.target,
                        cloud: ctx.cloud,
                        resolver: ctx.resolver,
                        entity: ctx.entity,
                    };
                    strategy::list_part_objects(&mut wctx, spec)?
                };
                let client =
                    ctx.cloud
                        .client_for(ctx.resolver, ctx.target.storage(), ctx.entity)?;
                for object in objects
                    .into_iter()
                    .filter(|obj| obj.key.starts_with(&list_prefix))
                    .filter(|obj| parts::is_part_key(&obj.key, spec.extension))
                {
                    let local_path = client.download_to_temp(&object.uri, temp_dir)?;
                    seed_from_parquet_path(tracker, &local_path, ctx.scan_cols, ctx.rename_back)?;
                }
            }
        }
        Ok(())
    }
}

fn seed_from_parquet_path(
    tracker: &mut check::UniqueTracker,
    path: &Path,
    scan_cols: &[String],
    rename_back: &std::collections::HashMap<String, String>,
) -> FloeResult<()> {
    let mut df = read_parquet_lazy(path, Some(scan_cols))?;
    rename_output_columns(&mut df, rename_back)?;
    tracker.seed_from_df(&df)?;
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
