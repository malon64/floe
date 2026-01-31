use crate::errors::RunError;
use crate::{config, io, FloeResult};

use super::{paths, CloudClient, Target};

pub fn archive_input_file(
    cloud: &mut CloudClient,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    archive_target: Option<&Target>,
    input_file: &io::format::InputFile,
) -> FloeResult<Option<String>> {
    let target = match archive_target {
        Some(target) => target,
        None => return Ok(None),
    };
    let relative = paths::archive_relative_path(&entity.name, input_file.source_name.as_str());
    let dest_uri = target.join_relative(&relative);
    let client = cloud.client_for(resolver, target.storage(), entity)?;
    client.copy_object(&input_file.source_uri, &dest_uri)?;
    if let Err(err) = client.delete_object(&input_file.source_uri) {
        return Err(Box::new(RunError(format!(
            "entity.name={} archive delete failed for {}: {err}",
            entity.name, input_file.source_uri
        ))));
    }
    Ok(Some(dest_uri))
}
