use crate::{config, FloeResult};

use crate::io::storage::Target;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedEntityTargets {
    pub(crate) source: Target,
    pub(crate) accepted: Target,
    pub(crate) rejected: Option<Target>,
}

pub(crate) fn resolve_entity_targets(
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
