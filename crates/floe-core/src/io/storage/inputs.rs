use std::path::{Path, PathBuf};

use crate::{config, io, report, FloeResult};

use super::{planner, Target};

#[derive(Debug, Clone)]
pub struct ResolvedInputs {
    pub files: Vec<io::format::InputFile>,
    pub mode: report::ResolvedInputMode,
}

pub fn resolve_inputs(
    config_dir: &Path,
    entity: &config::EntityConfig,
    adapter: &dyn io::format::InputAdapter,
    target: &Target,
    temp_dir: Option<&Path>,
    storage_client: Option<&dyn super::StorageClient>,
) -> FloeResult<ResolvedInputs> {
    // Storage-specific resolution: list + download for cloud, direct paths for local.
    match target {
        Target::S3 { storage, .. } => {
            let temp_dir = require_temp_dir(temp_dir, "s3")?;
            let client = require_storage_client(storage_client, "s3")?;
            let (bucket, key) = target.s3_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "s3 target missing bucket".to_string(),
                ))
            })?;
            let location = format!("bucket={}", bucket);
            let files =
                build_cloud_inputs(client, key, adapter, temp_dir, entity, storage, &location)?;
            Ok(ResolvedInputs {
                files,
                mode: report::ResolvedInputMode::Directory,
            })
        }
        Target::Gcs { storage, .. } => {
            let temp_dir = require_temp_dir(temp_dir, "gcs")?;
            let client = require_storage_client(storage_client, "gcs")?;
            let (bucket, key) = target.gcs_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "gcs target missing bucket".to_string(),
                ))
            })?;
            let location = format!("bucket={}", bucket);
            let files =
                build_cloud_inputs(client, key, adapter, temp_dir, entity, storage, &location)?;
            Ok(ResolvedInputs {
                files,
                mode: report::ResolvedInputMode::Directory,
            })
        }
        Target::Adls { storage, .. } => {
            let temp_dir = require_temp_dir(temp_dir, "adls")?;
            let client = require_storage_client(storage_client, "adls")?;
            let (container, account, base_path) = target.adls_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "adls target missing container".to_string(),
                ))
            })?;
            let location = format!("container={}, account={}", container, account);
            let files = build_cloud_inputs(
                client, base_path, adapter, temp_dir, entity, storage, &location,
            )?;
            Ok(ResolvedInputs {
                files,
                mode: report::ResolvedInputMode::Directory,
            })
        }
        Target::Local { storage, .. } => {
            let resolved =
                adapter.resolve_local_inputs(config_dir, &entity.name, &entity.source, storage)?;
            let files = build_local_inputs(&resolved.files, entity, storage_client);
            let mode = match resolved.mode {
                io::storage::local::LocalInputMode::File => report::ResolvedInputMode::File,
                io::storage::local::LocalInputMode::Directory => {
                    report::ResolvedInputMode::Directory
                }
            };
            Ok(ResolvedInputs { files, mode })
        }
    }
}

fn require_temp_dir<'a>(temp_dir: Option<&'a Path>, label: &str) -> FloeResult<&'a Path> {
    temp_dir.ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(crate::errors::RunError(format!(
            "{} tempdir missing",
            label
        )))
    })
}

fn require_storage_client<'a>(
    storage_client: Option<&'a dyn super::StorageClient>,
    label: &str,
) -> FloeResult<&'a dyn super::StorageClient> {
    storage_client.ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(crate::errors::RunError(format!(
            "{} storage client missing",
            label
        )))
    })
}

fn build_cloud_inputs(
    client: &dyn super::StorageClient,
    prefix: &str,
    adapter: &dyn io::format::InputAdapter,
    temp_dir: &Path,
    entity: &config::EntityConfig,
    storage: &str,
    location: &str,
) -> FloeResult<Vec<io::format::InputFile>> {
    let suffixes = adapter.suffixes()?;
    let list_refs = client.list(prefix)?;
    let filtered = planner::filter_by_suffixes(list_refs, &suffixes);
    let filtered = planner::stable_sort_refs(filtered);
    if filtered.is_empty() {
        return Err(Box::new(crate::errors::RunError(format!(
            "entity.name={} source.storage={} no input objects matched ({}, prefix={}, suffixes={})",
            entity.name,
            storage,
            location,
            prefix,
            suffixes.join(",")
        ))));
    }
    let mut inputs = Vec::with_capacity(filtered.len());
    for object in filtered {
        let local_path = client.download_to_temp(&object.uri, temp_dir)?;
        let source_name =
            planner::file_name_from_key(&object.key).unwrap_or_else(|| entity.name.clone());
        let source_stem =
            planner::file_stem_from_name(&source_name).unwrap_or_else(|| entity.name.clone());
        let source_uri = object.uri;
        inputs.push(io::format::InputFile {
            source_uri,
            source_local_path: local_path,
            source_name,
            source_stem,
        });
    }
    Ok(inputs)
}

fn build_local_inputs(
    files: &[PathBuf],
    entity: &config::EntityConfig,
    storage_client: Option<&dyn super::StorageClient>,
) -> Vec<io::format::InputFile> {
    files
        .iter()
        .map(|path| {
            let source_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(entity.name.as_str())
                .to_string();
            let source_stem = Path::new(&source_name)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or(entity.name.as_str())
                .to_string();
            let uri = storage_client
                .and_then(|client| client.resolve_uri(&path.display().to_string()).ok())
                .unwrap_or_else(|| path.display().to_string());
            io::format::InputFile {
                source_uri: uri,
                source_local_path: path.clone(),
                source_name,
                source_stem,
            }
        })
        .collect()
}
