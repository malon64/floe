//! Storage-agnostic output routing.
//!
//! This module centralizes "where to write" decisions so format writers only
//! deal with serialization, and storage backends (local/S3/etc.) handle the
//! final placement.

use std::path::Path;

use crate::{config, errors::StorageError, FloeResult};

use super::{paths, CloudClient, Target};

/// Determines how an output file should be positioned relative to the target base.
#[derive(Debug, Clone, Copy)]
pub enum OutputPlacement {
    /// Write into the target path/key (file or directory).
    Output,
    /// Write into the target path/key treating it as a directory.
    Directory,
    /// Write alongside the target (sibling of a file target).
    Sibling,
}

/// Write an output file to a storage target.
///
/// The caller supplies a filename and a writer callback; this function resolves the
/// final local path (or S3 key), writes the file, and uploads if needed.
pub fn write_output<F>(
    target: &Target,
    placement: OutputPlacement,
    filename: &str,
    temp_dir: Option<&Path>,
    cloud: &mut CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    writer: F,
) -> FloeResult<String>
where
    F: FnOnce(&Path) -> FloeResult<()>,
{
    match target {
        Target::Local { base_path, .. } => {
            let output_path = match placement {
                OutputPlacement::Output => paths::resolve_output_path(base_path, filename),
                OutputPlacement::Directory => paths::resolve_output_dir_path(base_path, filename),
                OutputPlacement::Sibling => paths::resolve_sibling_path(base_path, filename),
            };
            if let Some(parent) = output_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            writer(&output_path)?;
            Ok(output_path.display().to_string())
        }
        Target::S3 {
            storage,
            bucket,
            base_key,
            ..
        } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(StorageError(format!(
                    "entity.name={} missing temp dir for s3 output",
                    entity.name
                )))
            })?;
            let temp_path = temp_dir.join(filename);
            writer(&temp_path)?;
            let key = match placement {
                OutputPlacement::Output => paths::resolve_output_key(base_key, filename),
                OutputPlacement::Directory => paths::resolve_output_dir_key(base_key, filename),
                OutputPlacement::Sibling => paths::resolve_sibling_key(base_key, filename),
            };
            let client = cloud.client_for(resolver, storage, entity)?;
            let uri = super::s3::format_s3_uri(bucket, &key);
            client.upload_from_path(&temp_path, &uri)?;
            Ok(uri)
        }
    }
}
