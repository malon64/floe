use std::path::Path;

use polars::prelude::{DataFrame, DataType, Schema, SerReader};

use crate::io::format::{self, InputAdapter, InputFile, ReadInput};
use crate::{config, ConfigError, FloeResult};

struct CsvInputAdapter;

static CSV_INPUT_ADAPTER: CsvInputAdapter = CsvInputAdapter;

pub(crate) fn csv_input_adapter() -> &'static dyn InputAdapter {
    &CSV_INPUT_ADAPTER
}

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
    n_rows: Option<usize>,
) -> FloeResult<Vec<String>> {
    let read_options = source_options
        .to_csv_read_options(input_path)?
        .with_n_rows(n_rows);
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

impl InputAdapter for CsvInputAdapter {
    fn format(&self) -> &'static str {
        "csv"
    }

    fn read_inputs(
        &self,
        entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
    ) -> FloeResult<Vec<ReadInput>> {
        let default_options = config::SourceOptions::default();
        let source_options = entity.source.options.as_ref().unwrap_or(&default_options);
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.local_path;
            let input_columns = resolve_input_columns(path, source_options, columns)?;
            let raw_schema = build_raw_schema(&input_columns);
            let typed_schema =
                format::build_typed_schema(&input_columns, columns, normalize_strategy)?;
            let raw_plan = CsvReadPlan::strict(raw_schema);
            let typed_plan = CsvReadPlan::permissive(typed_schema);
            let raw_df = read_csv_file(path, source_options, &raw_plan)?;
            let typed_df = read_csv_file(path, source_options, &typed_plan)?;
            let input =
                format::finalize_read_input(input_file, raw_df, typed_df, normalize_strategy)?;
            inputs.push(input);
        }
        Ok(inputs)
    }
}

fn resolve_input_columns(
    path: &Path,
    source_options: &config::SourceOptions,
    declared_columns: &[config::ColumnConfig],
) -> FloeResult<Vec<String>> {
    let header = source_options.header.unwrap_or(true);
    if header {
        return read_csv_header(path, source_options, Some(0));
    }

    let input_columns = read_csv_header(path, source_options, Some(1))?;
    let declared_names = declared_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    Ok(headless_columns(&declared_names, input_columns.len()))
}

fn headless_columns(declared_names: &[String], input_count: usize) -> Vec<String> {
    let mut names = declared_names
        .iter()
        .take(input_count)
        .cloned()
        .collect::<Vec<_>>();
    if input_count > declared_names.len() {
        for index in declared_names.len()..input_count {
            names.push(format!("extra_column_{}", index + 1));
        }
    }
    names
}

fn build_raw_schema(columns: &[String]) -> Schema {
    let mut schema = Schema::with_capacity(columns.len());
    for name in columns {
        schema.insert(name.as_str().into(), DataType::String);
    }
    schema
}
