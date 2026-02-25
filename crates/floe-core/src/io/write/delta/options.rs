use crate::errors::RunError;
use crate::io::write::metrics;
use crate::{config, FloeResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaWriteRuntimeOptions {
    pub partition_by: Option<Vec<String>>,
    pub target_file_size_bytes: Option<usize>,
    pub small_file_threshold_bytes: u64,
}

pub fn delta_write_runtime_options(
    entity: &config::EntityConfig,
) -> FloeResult<DeltaWriteRuntimeOptions> {
    let target_file_size_bytes_u64 = entity
        .sink
        .accepted
        .options
        .as_ref()
        .and_then(|options| options.max_size_per_file);
    let target_file_size_bytes = match target_file_size_bytes_u64 {
        Some(value) => Some(usize::try_from(value).map_err(|_| {
            Box::new(RunError(format!(
                "delta sink max_size_per_file is too large for this platform: {value}"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?),
        None => None,
    };
    Ok(DeltaWriteRuntimeOptions {
        partition_by: entity.sink.accepted.partition_by.clone(),
        target_file_size_bytes,
        small_file_threshold_bytes: metrics::default_small_file_threshold_bytes(
            target_file_size_bytes_u64,
        ),
    })
}
