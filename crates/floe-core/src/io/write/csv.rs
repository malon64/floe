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
        let mut part_allocator = prepare_part_allocator(target, mode, cloud, resolver, entity)?;
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

fn prepare_part_allocator(
    target: &io::storage::Target,
    mode: config::WriteMode,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<parts::PartNameAllocator> {
    match mode {
        config::WriteMode::Overwrite => {
            clear_output_parts(target, cloud, resolver, entity)?;
            Ok(parts::PartNameAllocator::from_next_index(0, "csv"))
        }
        config::WriteMode::Append => Ok(parts::PartNameAllocator::unique("csv")),
    }
}

fn clear_output_parts(
    target: &io::storage::Target,
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<()> {
    match target {
        io::storage::Target::Local { base_path, .. } => {
            let base_path = Path::new(base_path);
            let _ = parts::clear_local_part_files(base_path, "csv")?;
        }
        io::storage::Target::S3 {
            storage, base_key, ..
        } => {
            clear_remote_part_files(cloud, resolver, entity, storage, base_key, "s3", "bucket")?;
        }
        io::storage::Target::Gcs {
            storage, base_key, ..
        } => {
            clear_remote_part_files(cloud, resolver, entity, storage, base_key, "gcs", "bucket")?;
        }
        io::storage::Target::Adls {
            storage, base_path, ..
        } => {
            clear_remote_part_files(
                cloud,
                resolver,
                entity,
                storage,
                base_path,
                "adls",
                "container",
            )?;
        }
    }
    Ok(())
}

fn clear_remote_part_files(
    cloud: &mut io::storage::CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    storage: &str,
    base_key: &str,
    target_type: &str,
    root_label: &str,
) -> FloeResult<()> {
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
    for object in objects {
        if !object.key.starts_with(&list_prefix) || object.key.is_empty() {
            continue;
        }
        if !parts::is_part_key(&object.key, "csv") {
            continue;
        }
        client.delete_object(&object.uri)?;
    }
    Ok(())
}

// Filename construction is shared via io::storage::paths helpers.
