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
        Target::Local { storage, .. } => {
            let resolved =
                adapter.resolve_local_inputs(config_dir, &entity.name, &entity.source, storage)?;
            let files = build_local_inputs(&resolved.files, entity);
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
            io::format::InputFile {
                source_uri: path.display().to_string(),
                source_local_path: path.clone(),
                source_name,
                source_stem,
            }
        })
        .collect()
}
