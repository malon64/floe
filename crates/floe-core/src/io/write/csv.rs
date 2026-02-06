use std::path::{Path, PathBuf};

use polars::prelude::{CsvWriter, DataFrame, SerWriter};

use crate::errors::IoError;
use crate::io::format::{RejectedSinkAdapter, RejectedWriteRequest};
use crate::{config, io, FloeResult};

use super::{append, overwrite};

struct CsvRejectedAdapter;

static CSV_REJECTED_ADAPTER: CsvRejectedAdapter = CsvRejectedAdapter;

pub(crate) fn csv_rejected_adapter() -> &'static dyn RejectedSinkAdapter {
    &CSV_REJECTED_ADAPTER
}

pub fn write_rejected_csv(df: &mut DataFrame, output_path: &Path) -> FloeResult<PathBuf> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(output_path)?;
    CsvWriter::new(file)
        .finish(df)
        .map_err(|err| Box::new(IoError(format!("rejected csv write failed: {err}"))))?;
    Ok(output_path.to_path_buf())
}

impl RejectedSinkAdapter for CsvRejectedAdapter {
    fn write_rejected(&self, request: RejectedWriteRequest<'_>) -> FloeResult<String> {
        let RejectedWriteRequest {
            target,
            df,
            source_stem: _,
            temp_dir,
            cloud,
            resolver,
            entity,
            mode,
        } = request;
        let mut part_allocator = match mode {
            config::WriteMode::Overwrite => {
                overwrite::rejected_csv_part_allocator(target, cloud, resolver, entity)?
            }
            config::WriteMode::Append => {
                append::rejected_csv_part_allocator(target, cloud, resolver, entity)?
            }
        };
        let part_filename = part_allocator.allocate_next();
        io::storage::output::write_output(
            target,
            io::storage::output::OutputPlacement::Directory,
            &part_filename,
            temp_dir,
            cloud,
            resolver,
            entity,
            |path| {
                write_rejected_csv(df, path)?;
                Ok(())
            },
        )
    }
}

// Filename construction is shared via io::storage::paths helpers.
