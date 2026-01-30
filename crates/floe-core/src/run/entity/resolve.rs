use crate::{config, io, report, FloeResult};

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
    _source_is_s3: bool,
    temp_dir: Option<&tempfile::TempDir>,
) -> FloeResult<(Vec<InputFile>, report::ResolvedInputMode)> {
    let storage_client = Some(cloud.client_for(
        &context.storage_resolver,
        resolved_targets.source.storage(),
        entity,
    )? as &dyn io::storage::StorageClient);
    let temp_dir = temp_dir.map(|dir| dir.path());
    let resolved = io::storage::inputs::resolve_inputs(
        &context.config_dir,
        entity,
        input_adapter,
        &resolved_targets.source,
        temp_dir,
        storage_client,
    )?;
    Ok((resolved.files, resolved.mode))
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
