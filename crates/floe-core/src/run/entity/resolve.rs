use std::path::{Path, PathBuf};

use crate::{config, io, report, ConfigError, FloeResult};

use crate::run::RunContext;
use io::format::{InputAdapter, InputFile};
use io::storage::Target;

#[derive(Debug, Clone)]
pub(super) struct ResolvedEntityTargets {
    pub(super) source: Target,
    pub(super) accepted: Target,
    pub(super) rejected: Option<Target>,
}

pub(super) fn resolve_input_files(
    context: &RunContext,
    cloud: &mut io::storage::CloudClient,
    entity: &config::EntityConfig,
    input_adapter: &dyn InputAdapter,
    resolved_targets: &ResolvedEntityTargets,
    source_is_s3: bool,
    temp_dir: Option<&tempfile::TempDir>,
) -> FloeResult<(Vec<InputFile>, report::ResolvedInputMode)> {
    if source_is_s3 {
        let (bucket, key) = resolved_targets
            .source
            .s3_parts()
            .ok_or_else(|| Box::new(ConfigError("s3 target missing bucket".to_string())))?;
        let s3_client = cloud.client_for(
            &context.storage_resolver,
            resolved_targets.source.storage(),
            entity,
        )?;
        let temp_dir =
            temp_dir.ok_or_else(|| Box::new(ConfigError("s3 tempdir missing".to_string())))?;
        let inputs = build_s3_inputs(
            s3_client,
            bucket,
            key,
            input_adapter,
            temp_dir.path(),
            entity,
            resolved_targets.source.storage(),
        )?;
        return Ok((inputs, report::ResolvedInputMode::Directory));
    }

    let resolved_inputs = input_adapter.resolve_local_inputs(
        &context.config_dir,
        &entity.name,
        &entity.source,
        resolved_targets.source.storage(),
    )?;
    let inputs = build_local_inputs(&resolved_inputs.files, entity);
    let mode = match resolved_inputs.mode {
        io::storage::local::LocalInputMode::File => report::ResolvedInputMode::File,
        io::storage::local::LocalInputMode::Directory => report::ResolvedInputMode::Directory,
    };
    Ok((inputs, mode))
}

pub(super) fn resolve_entity_targets(
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<ResolvedEntityTargets> {
    let source = resolver.resolve_path(
        &entity.name,
        "source.storage",
        entity.source.storage.as_deref(),
        &entity.source.path,
    )?;
    let accepted = resolver.resolve_path(
        &entity.name,
        "sink.accepted.storage",
        entity.sink.accepted.storage.as_deref(),
        &entity.sink.accepted.path,
    )?;
    let rejected = entity
        .sink
        .rejected
        .as_ref()
        .map(|rejected| {
            resolver.resolve_path(
                &entity.name,
                "sink.rejected.storage",
                rejected.storage.as_deref(),
                &rejected.path,
            )
        })
        .transpose()?;
    let source = Target::from_resolved(&source)?;
    let accepted = Target::from_resolved(&accepted)?;
    let rejected = rejected.as_ref().map(Target::from_resolved).transpose()?;
    Ok(ResolvedEntityTargets {
        source,
        accepted,
        rejected,
    })
}

fn build_local_inputs(files: &[PathBuf], entity: &config::EntityConfig) -> Vec<InputFile> {
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
            InputFile {
                source_uri: path.display().to_string(),
                local_path: path.clone(),
                source_name,
                source_stem,
            }
        })
        .collect()
}

fn build_s3_inputs(
    client: &dyn io::storage::StorageClient,
    bucket: &str,
    prefix: &str,
    adapter: &dyn InputAdapter,
    temp_dir: &Path,
    entity: &config::EntityConfig,
    storage: &str,
) -> FloeResult<Vec<InputFile>> {
    let suffixes = adapter.suffixes()?;
    let keys = client.list(prefix)?;
    let keys = io::storage::s3::filter_keys_by_suffixes(keys, &suffixes);
    if keys.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "entity.name={} source.storage={} no input objects matched (bucket={}, prefix={}, suffixes={})",
            entity.name,
            storage,
            bucket,
            prefix,
            suffixes.join(",")
        ))));
    }
    let mut inputs = Vec::with_capacity(keys.len());
    for key in keys {
        let local_path = io::storage::s3::temp_path_for_key(temp_dir, &key);
        client.download(&key, &local_path)?;
        let source_name =
            io::storage::s3::file_name_from_key(&key).unwrap_or_else(|| entity.name.clone());
        let source_stem = io::storage::s3::file_stem_from_name(&source_name)
            .unwrap_or_else(|| entity.name.clone());
        let source_uri = io::storage::s3::format_s3_uri(bucket, &key);
        inputs.push(InputFile {
            source_uri,
            local_path,
            source_name,
            source_stem,
        });
    }
    Ok(inputs)
}
