use crate::errors::RunError;
use crate::{config, io, FloeResult};

use crate::io::storage::{paths, CloudClient, Target};

pub fn archive_input_file(
    cloud: &mut CloudClient,
    resolver: &config::StorageResolver,
    run_id: &str,
    entity: &config::EntityConfig,
    archive_target: Option<&Target>,
    input_file: &io::format::InputFile,
) -> FloeResult<Option<String>> {
    let target = match archive_target {
        Some(target) => target,
        None => return Ok(None),
    };
    let relative = paths::archive_relative_path_for_run(
        &entity.name,
        input_file.source_name.as_str(),
        run_id,
        &input_file.source_uri,
    );
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
