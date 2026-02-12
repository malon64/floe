use std::path::{Path, PathBuf};

use crate::io::storage::{planner, Target};
use crate::{config, io, report, FloeResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveInputsMode {
    Download,
    ListOnly,
}

#[derive(Debug, Clone)]
pub struct ResolvedInputs {
    pub files: Vec<io::format::InputFile>,
    pub listed: Vec<String>,
    pub mode: report::ResolvedInputMode,
}

pub fn resolve_inputs(
    config_dir: &Path,
    entity: &config::EntityConfig,
    adapter: &dyn io::format::InputAdapter,
    target: &Target,
    resolution_mode: ResolveInputsMode,
    temp_dir: Option<&Path>,
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
) -> FloeResult<ResolvedInputs> {
    // Storage-specific resolution: list + download for cloud, direct paths for local.
    match target {
        Target::S3 { storage, .. } => {
            let client = require_storage_client(storage_client, "s3")?;
            let (bucket, key) = target.s3_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "s3 target missing bucket".to_string(),
                ))
            })?;
            let location = format!("bucket={}", bucket);
            resolve_cloud_inputs_for_prefix(
                client,
                key,
                adapter,
                entity,
                storage,
                &location,
                resolution_mode,
                temp_dir,
            )
        }
        Target::Gcs { storage, .. } => {
            let client = require_storage_client(storage_client, "gcs")?;
            let (bucket, key) = target.gcs_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "gcs target missing bucket".to_string(),
                ))
            })?;
            let location = format!("bucket={}", bucket);
            resolve_cloud_inputs_for_prefix(
                client,
                key,
                adapter,
                entity,
                storage,
                &location,
                resolution_mode,
                temp_dir,
            )
        }
        Target::Adls { storage, .. } => {
            let client = require_storage_client(storage_client, "adls")?;
            let (container, account, base_path) = target.adls_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "adls target missing container".to_string(),
                ))
            })?;
            let location = format!("container={}, account={}", container, account);
            resolve_cloud_inputs_for_prefix(
                client,
                base_path,
                adapter,
                entity,
                storage,
                &location,
                resolution_mode,
                temp_dir,
            )
        }
        Target::Local { storage, .. } => {
            let resolved =
                adapter.resolve_local_inputs(config_dir, &entity.name, &entity.source, storage)?;
            let listed = build_local_listing(&resolved.files, storage_client);
            let files = match resolution_mode {
                ResolveInputsMode::Download => {
                    build_local_inputs(&resolved.files, entity, storage_client)
                }
                ResolveInputsMode::ListOnly => Vec::new(),
            };
            let mode = match resolved.mode {
                io::storage::local::LocalInputMode::File => report::ResolvedInputMode::File,
                io::storage::local::LocalInputMode::Directory => {
                    report::ResolvedInputMode::Directory
                }
            };
            Ok(ResolvedInputs {
                files,
                listed,
                mode,
            })
        }
    }
}

fn resolve_cloud_inputs_for_prefix(
    client: &dyn crate::io::storage::StorageClient,
    prefix: &str,
    adapter: &dyn io::format::InputAdapter,
    entity: &config::EntityConfig,
    storage: &str,
    location: &str,
    resolution_mode: ResolveInputsMode,
    temp_dir: Option<&Path>,
) -> FloeResult<ResolvedInputs> {
    let objects = list_cloud_objects(client, prefix, adapter, entity, storage, location)?;
    let listed = objects.iter().map(|obj| obj.uri.clone()).collect();
    let files = match resolution_mode {
        ResolveInputsMode::Download => {
            let temp_dir = require_temp_dir(temp_dir, storage)?;
            build_cloud_inputs(client, &objects, temp_dir, entity)?
        }
        ResolveInputsMode::ListOnly => Vec::new(),
    };
    Ok(ResolvedInputs {
        files,
        listed,
        mode: report::ResolvedInputMode::Directory,
    })
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
    storage_client: Option<&'a dyn crate::io::storage::StorageClient>,
    label: &str,
) -> FloeResult<&'a dyn crate::io::storage::StorageClient> {
    storage_client.ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(crate::errors::RunError(format!(
            "{} storage client missing",
            label
        )))
    })
}

fn list_cloud_objects(
    client: &dyn crate::io::storage::StorageClient,
    prefix: &str,
    adapter: &dyn io::format::InputAdapter,
    entity: &config::EntityConfig,
    storage: &str,
    location: &str,
) -> FloeResult<Vec<io::storage::planner::ObjectRef>> {
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
    Ok(filtered)
}

fn build_cloud_inputs(
    client: &dyn crate::io::storage::StorageClient,
    objects: &[io::storage::planner::ObjectRef],
    temp_dir: &Path,
    entity: &config::EntityConfig,
) -> FloeResult<Vec<io::format::InputFile>> {
    let mut inputs = Vec::with_capacity(objects.len());
    for object in objects {
        let local_path = client.download_to_temp(&object.uri, temp_dir)?;
        let source_name =
            planner::file_name_from_key(&object.key).unwrap_or_else(|| entity.name.clone());
        let source_stem =
            planner::file_stem_from_name(&source_name).unwrap_or_else(|| entity.name.clone());
        let source_uri = object.uri.clone();
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
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
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

fn build_local_listing(
    files: &[PathBuf],
    storage_client: Option<&dyn crate::io::storage::StorageClient>,
) -> Vec<String> {
    files
        .iter()
        .map(|path| {
            storage_client
                .and_then(|client| client.resolve_uri(&path.display().to_string()).ok())
                .unwrap_or_else(|| path.display().to_string())
        })
        .collect()
}
