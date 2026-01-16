use std::path::Path;

use polars::prelude::{DataFrame, Schema, SerReader};

use crate::{config, ConfigError, FloeResult};

pub fn read_csv(
    input_path: &Path,
    source_options: &config::SourceOptions,
    schema: Schema,
) -> FloeResult<DataFrame> {
    let read_options = source_options
        .to_csv_read_options(input_path)?
        .with_schema(Some(std::sync::Arc::new(schema)));
    let reader = read_options
        .try_into_reader_with_file_path(None)
        .map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to open csv at {}: {err}",
                input_path.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    let df = reader
        .finish()
        .map_err(|err| {
            Box::new(ConfigError(format!("csv read failed: {err}")))
                as Box<dyn std::error::Error + Send + Sync>
        })?;
    Ok(df)
}
