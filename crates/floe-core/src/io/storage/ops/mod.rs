use std::path::Path;

pub mod archive;
pub mod inputs;
pub mod output;

use crate::io::storage::{CloudClient, StorageClient, Target};
use crate::{config, io, FloeResult};

pub fn resolve_inputs(
    config_dir: &Path,
    entity: &config::EntityConfig,
    adapter: &dyn io::format::InputAdapter,
    target: &Target,
    resolution_mode: inputs::ResolveInputsMode,
    temp_dir: Option<&Path>,
    storage_client: Option<&dyn StorageClient>,
) -> FloeResult<inputs::ResolvedInputs> {
    inputs::resolve_inputs(
        config_dir,
        entity,
        adapter,
        target,
        resolution_mode,
        temp_dir,
        storage_client,
    )
}

pub fn archive_input(
    cloud: &mut CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    archive_target: Option<&Target>,
    input_file: &io::format::InputFile,
) -> FloeResult<Option<String>> {
    archive::archive_input_file(cloud, resolver, entity, archive_target, input_file)
}
