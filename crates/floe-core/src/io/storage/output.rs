use std::path::Path;

use crate::{config, ConfigError, FloeResult};

use super::{paths, CloudClient, Target};

#[derive(Debug, Clone, Copy)]
pub enum OutputPlacement {
    Output,
    Sibling,
}

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
                Box::new(ConfigError(format!(
                    "entity.name={} missing temp dir for s3 output",
                    entity.name
                )))
            })?;
            let temp_path = temp_dir.join(filename);
            writer(&temp_path)?;
            let key = match placement {
                OutputPlacement::Output => paths::resolve_output_key(base_key, filename),
                OutputPlacement::Sibling => paths::resolve_sibling_key(base_key, filename),
            };
            let client = cloud.client_for(resolver, storage, entity)?;
            client.upload(&key, &temp_path)?;
            Ok(super::s3::format_s3_uri(bucket, &key))
        }
    }
}
