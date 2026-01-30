use std::path::{Path, PathBuf};

use crate::{config, io, report, FloeResult};

use super::Target;

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
    match target {
        Target::S3 { storage, .. } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(crate::errors::RunError("s3 tempdir missing".to_string()))
            })?;
            let client = storage_client.ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "s3 storage client missing".to_string(),
                ))
            })?;
            let (bucket, key) = target.s3_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "s3 target missing bucket".to_string(),
                ))
            })?;
            let files = io::storage::s3::build_input_files(
                client, bucket, key, adapter, temp_dir, entity, storage,
            )?;
            Ok(ResolvedInputs {
                files,
                mode: report::ResolvedInputMode::Directory,
            })
        }
        Target::Gcs { storage, .. } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(crate::errors::RunError("gcs tempdir missing".to_string()))
            })?;
            let client = storage_client.ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "gcs storage client missing".to_string(),
                ))
            })?;
            let (bucket, key) = target.gcs_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "gcs target missing bucket".to_string(),
                ))
            })?;
            let files = io::storage::gcs::build_input_files(
                client, bucket, key, adapter, temp_dir, entity, storage,
            )?;
            Ok(ResolvedInputs {
                files,
                mode: report::ResolvedInputMode::Directory,
            })
        }
        Target::Adls { storage, .. } => {
            let temp_dir = temp_dir.ok_or_else(|| {
                Box::new(crate::errors::RunError("adls tempdir missing".to_string()))
            })?;
            let client = storage_client.ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "adls storage client missing".to_string(),
                ))
            })?;
            let (container, account, base_path) = target.adls_parts().ok_or_else(|| {
                Box::new(crate::errors::RunError(
                    "adls target missing container".to_string(),
                ))
            })?;
            let files = io::storage::adls::build_input_files(
                client, container, account, base_path, adapter, temp_dir, entity, storage,
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
