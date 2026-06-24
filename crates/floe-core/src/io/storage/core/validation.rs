use crate::errors::FloeError;
use crate::{config, FloeResult};

pub fn require_field(
    definition: &config::StorageDefinition,
    value: Option<&String>,
    field: &str,
    kind: &str,
) -> FloeResult<String> {
    value
        .cloned()
        .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
            FloeError::config(format!(
                "storage {} requires {} for type {}",
                definition.name, field, kind
            ))
            .into()
        })
}
