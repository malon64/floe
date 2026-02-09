//! Storage-agnostic output routing.
//!
//! This module centralizes "where to write" decisions so format writers only
//! deal with serialization, and storage backends (local/S3/etc.) handle the
//! final placement.

use std::path::Path;

use crate::{config, errors::StorageError, FloeResult};

use super::{paths, CloudClient, OutputPlacement, Target};

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
        Target::S3 { storage, .. } => {
            let temp_dir = require_temp_dir(temp_dir, entity, "s3")?;
            let temp_path = temp_dir.join(filename);
            writer(&temp_path)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let uri = target.resolve_output_uri(placement, filename);
            client.upload_from_path(&temp_path, &uri)?;
            Ok(uri)
        }
        Target::Gcs { storage, .. } => {
            let temp_dir = require_temp_dir(temp_dir, entity, "gcs")?;
            let temp_path = temp_dir.join(filename);
            writer(&temp_path)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let uri = target.resolve_output_uri(placement, filename);
            client.upload_from_path(&temp_path, &uri)?;
            Ok(uri)
        }
        Target::Adls { storage, .. } => {
            let temp_dir = require_temp_dir(temp_dir, entity, "adls")?;
            let temp_path = temp_dir.join(filename);
            writer(&temp_path)?;
            let client = cloud.client_for(resolver, storage, entity)?;
            let uri = target.resolve_output_uri(placement, filename);
            client.upload_from_path(&temp_path, &uri)?;
            Ok(uri)
        }
    }
}

fn require_temp_dir<'a>(
    temp_dir: Option<&'a Path>,
    entity: &config::EntityConfig,
    label: &str,
) -> FloeResult<&'a Path> {
    temp_dir.ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(StorageError(format!(
            "entity.name={} missing temp dir for {} output",
            entity.name, label
        )))
    })
}
