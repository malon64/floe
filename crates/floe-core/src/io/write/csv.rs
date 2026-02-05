use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use polars::prelude::{CsvWriter, DataFrame, SerWriter};

use crate::errors::IoError;
use crate::io::format::{RejectedSinkAdapter, RejectedWriteRequest};
use crate::{config, io, FloeResult};

struct CsvRejectedAdapter;

static CSV_REJECTED_ADAPTER: CsvRejectedAdapter = CsvRejectedAdapter;

pub(crate) fn csv_rejected_adapter() -> &'static dyn RejectedSinkAdapter {
    &CSV_REJECTED_ADAPTER
}

pub fn write_rejected_csv(
    df: &mut DataFrame,
    output_path: &Path,
    mode: config::WriteMode,
) -> FloeResult<PathBuf> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let include_header = match mode {
        config::WriteMode::Overwrite => true,
        config::WriteMode::Append => {
            !output_path.exists() || std::fs::metadata(output_path)?.len() == 0
        }
    };
    let file = match mode {
        config::WriteMode::Overwrite => OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_path)?,
        config::WriteMode::Append => OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)?,
    };
    CsvWriter::new(file)
        .include_header(include_header)
        .finish(df)
        .map_err(|err| Box::new(IoError(format!("rejected csv write failed: {err}"))))?;
    Ok(output_path.to_path_buf())
}

impl RejectedSinkAdapter for CsvRejectedAdapter {
    fn write_rejected(&self, request: RejectedWriteRequest<'_>) -> FloeResult<String> {
        let base_filename =
            io::storage::paths::build_output_filename(request.source_stem, "_rejected", "csv");
        let (placement, filename) = match request.mode {
            config::WriteMode::Overwrite => {
                (io::storage::output::OutputPlacement::Output, base_filename)
            }
            config::WriteMode::Append => (
                io::storage::output::OutputPlacement::Directory,
                io::storage::paths::run_partition_relative(request.run_id, &base_filename),
            ),
        };
        io::storage::output::write_output(
            request.target,
            placement,
            &filename,
            request.temp_dir,
            request.cloud,
            request.resolver,
            request.entity,
            |path| {
                write_rejected_csv(request.df, path, config::WriteMode::Overwrite)?;
                Ok(())
            },
        )
    }
}

// Filename construction is shared via io::storage::paths helpers.
