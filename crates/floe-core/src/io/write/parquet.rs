use std::path::Path;

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter};

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
        .map_err(|err| Box::new(ConfigError(format!("parquet write failed: {err}"))))?;
    Ok(())
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
        let filename = io::storage::paths::build_output_filename(source_stem, "", "parquet");
        io::storage::output::write_output(
            target,
            io::storage::output::OutputPlacement::Output,
            &filename,
            temp_dir,
            cloud,
            resolver,
            entity,
            |path| write_parquet_to_path(df, path, entity.sink.accepted.options.as_ref()),
        )
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
