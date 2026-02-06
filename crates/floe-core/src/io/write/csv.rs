use std::path::{Path, PathBuf};

use polars::prelude::{CsvWriter, DataFrame, SerWriter};

use crate::errors::IoError;
use crate::io::format::{RejectedSinkAdapter, RejectedWriteRequest};
use crate::{config, io, ConfigError, FloeResult};

use super::parts;

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
        let mut part_allocator = match target {
            io::storage::Target::Local { base_path, .. } => {
                let base_path = Path::new(base_path);
                if mode == config::WriteMode::Overwrite {
                    parts::clear_local_part_files(base_path, "csv")?;
                }
                parts::PartNameAllocator::from_local_path(base_path, "csv")?
            }
            io::storage::Target::S3 {
                storage, base_key, ..
            } => {
                let next_index = prepare_remote_part_index(
                    cloud, resolver, entity, storage, base_key, mode, "s3", "bucket",
                )?;
                parts::PartNameAllocator::from_next_index(next_index, "csv")
            }
            io::storage::Target::Gcs {
                storage, base_key, ..
            } => {
                let next_index = prepare_remote_part_index(
                    cloud, resolver, entity, storage, base_key, mode, "gcs", "bucket",
                )?;
                parts::PartNameAllocator::from_next_index(next_index, "csv")
            }
            io::storage::Target::Adls {
                storage, base_path, ..
            } => {
                let next_index = prepare_remote_part_index(
                    cloud,
                    resolver,
                    entity,
                    storage,
                    base_path,
                    mode,
                    "adls",
                    "container",
                )?;
                parts::PartNameAllocator::from_next_index(next_index, "csv")
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

fn prepare_remote_part_index(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    base_key: &str,
    mode: config::WriteMode,
    target_type: &str,
    root_label: &str,
) -> FloeResult<usize> {
    let prefix = base_key.trim_matches('/');
    if prefix.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} sink.rejected.path must not be {} root for {} outputs",
            entity.name, root_label, target_type
        ))));
    }
    let list_prefix = format!("{prefix}/");
    let client = cloud.client_for(resolver, storage, entity)?;
    let objects = client.list(&list_prefix)?;
    let mut next_index = 0usize;
    let mut part_uris = Vec::new();
    for object in objects {
        if !object.key.starts_with(&list_prefix) || object.key.is_empty() {
            continue;
        }
        let Some(index) = parse_part_index_from_key(&object.key, "csv") else {
            continue;
        };
        next_index = next_index.max(index.saturating_add(1));
        if mode == config::WriteMode::Overwrite {
            part_uris.push(object.uri);
        }
    }
    if mode == config::WriteMode::Overwrite {
        for uri in part_uris {
            client.delete_object(&uri)?;
        }
        Ok(0)
    } else {
        Ok(next_index)
    }
}

fn parse_part_index_from_key(key: &str, extension: &str) -> Option<usize> {
    let path = Path::new(key);
    if path.extension()?.to_str()? != extension {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let digits = stem.strip_prefix("part-")?;
    if digits.len() < 5 || !digits.bytes().all(|value| value.is_ascii_digit()) {
        return None;
    }
    digits.parse::<usize>().ok()
}

// Filename construction is shared via io::storage::paths helpers.
