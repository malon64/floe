use std::path::Path;

use polars::prelude::{DataFrame, Schema, SerReader};

use crate::{config, ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub struct CsvReadPlan {
    pub schema: Schema,
    pub ignore_errors: bool,
}

impl CsvReadPlan {
    pub fn strict(schema: Schema) -> Self {
        Self {
            schema,
            ignore_errors: false,
        }
    }

    pub fn permissive(schema: Schema) -> Self {
        Self {
            schema,
            ignore_errors: true,
        }
    }
}

pub fn read_csv_file(
    input_path: &Path,
    source_options: &config::SourceOptions,
    plan: &CsvReadPlan,
) -> FloeResult<DataFrame> {
    let read_options = source_options
        .to_csv_read_options(input_path)?
        .with_schema(Some(std::sync::Arc::new(plan.schema.clone())))
        .with_ignore_errors(plan.ignore_errors);
    let reader = read_options
        .try_into_reader_with_file_path(None)
        .map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to open csv at {}: {err}",
                input_path.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    let df = reader.finish().map_err(|err| {
        Box::new(ConfigError(format!("csv read failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    Ok(df)
}

pub fn read_csv_header(
    input_path: &Path,
    source_options: &config::SourceOptions,
) -> FloeResult<Vec<String>> {
    let read_options = source_options
        .to_csv_read_options(input_path)?
        .with_n_rows(Some(1));
    let reader = read_options
        .try_into_reader_with_file_path(None)
        .map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to open csv at {}: {err}",
                input_path.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    let df = reader.finish().map_err(|err| {
        Box::new(ConfigError(format!("csv header read failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    Ok(df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect())
}
