use std::path::Path;
use std::sync::Arc;

use polars::prelude::{
    col, DataFrame, DataType, LazyCsvReader, LazyFileListReader, PlPath, Schema, SerReader,
};

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
}

pub fn read_csv_file(
    input_path: &Path,
    source_options: &config::SourceOptions,
    plan: &CsvReadPlan,
) -> FloeResult<DataFrame> {
    read_csv_lazy(
        input_path,
        source_options,
        &plan.schema,
        plan.ignore_errors,
        None,
    )
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
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>> {
        let default_options = config::SourceOptions::default();
        let source_options = entity.source.options.as_ref().unwrap_or(&default_options);
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.local_path;
            let input_columns = resolve_input_columns(path, source_options, columns)?;
            let typed_projection = if collect_raw {
                projected_columns(&input_columns, columns)
            } else {
                None
            };
            let typed_schema =
                format::build_typed_schema(input_columns.as_slice(), columns, normalize_strategy)?;
            let input = if collect_raw {
                let raw_schema = build_raw_schema(&input_columns);
                let raw_plan = CsvReadPlan::strict(raw_schema);
                let raw_df = read_csv_file(path, source_options, &raw_plan)?;
                let mut typed_df = format::cast_df_to_schema(&raw_df, &typed_schema)?;
                if let Some(projection) = typed_projection.as_ref() {
                    typed_df = typed_df.select(projection).map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to project typed columns: {err}"
                        )))
                    })?;
                }
                format::finalize_read_input(
                    input_file,
                    input_columns.clone(),
                    Some(raw_df),
                    typed_df,
                    normalize_strategy,
                )?
            } else {
                let typed_plan = CsvReadPlan {
                    schema: typed_schema,
                    ignore_errors: true,
                };
                let typed_df = read_csv_lazy(path, source_options, &typed_plan.schema, true, None)?;
                format::finalize_read_input(
                    input_file,
                    input_columns.clone(),
                    None,
                    typed_df,
                    normalize_strategy,
                )?
            };
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

fn read_csv_lazy(
    input_path: &Path,
    source_options: &config::SourceOptions,
    schema: &Schema,
    ignore_errors: bool,
    projection: Option<&[String]>,
) -> FloeResult<DataFrame> {
    let header = source_options.header.unwrap_or(true);
    let parse_options = source_options.to_csv_parse_options()?;
    let path_str = input_path.to_string_lossy();
    let mut reader = LazyCsvReader::new(PlPath::new(path_str.as_ref()))
        .with_has_header(header)
        .with_schema(Some(Arc::new(schema.clone())))
        .with_ignore_errors(ignore_errors)
        .map_parse_options(|_| parse_options.clone());

    if let Some(chunk_size) = csv_chunk_size_for_path(input_path) {
        reader = reader.with_chunk_size(chunk_size);
    }

    let mut lf = reader.finish().map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to scan csv at {}: {err}",
            input_path.display()
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;

    if let Some(columns) = projection {
        let exprs = columns.iter().map(col).collect::<Vec<_>>();
        lf = lf.select(exprs);
    }

    lf.collect().map_err(|err| {
        Box::new(ConfigError(format!("csv read failed: {err}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

fn projected_columns(
    input_columns: &[String],
    declared_columns: &[config::ColumnConfig],
) -> Option<Vec<String>> {
    let declared = declared_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    let projected = input_columns
        .iter()
        .filter(|name| declared.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if projected.is_empty() || projected.len() == input_columns.len() {
        None
    } else {
        Some(projected)
    }
}

fn csv_chunk_size_for_path(path: &Path) -> Option<usize> {
    let metadata = std::fs::metadata(path).ok()?;
    let size = metadata.len();
    if size >= 64 * 1024 * 1024 {
        Some(50_000)
    } else {
        None
    }
}
