use std::collections::HashMap;
use std::path::Path;

use polars::prelude::{DataFrame, DataType, Schema};

use crate::{check, config, io, ConfigError, FloeResult};

use super::entity::InputFile;
use super::normalize::{normalize_dataframe_columns, normalize_name};

pub(super) type ValidationCollect = (Vec<bool>, Vec<Option<String>>, Vec<Vec<check::RowError>>);

pub(super) fn required_columns(columns: &[config::ColumnConfig]) -> Vec<String> {
    columns
        .iter()
        .filter(|col| col.nullable == Some(false))
        .map(|col| col.name.clone())
        .collect()
}

pub(super) fn read_inputs(
    entity: &config::EntityConfig,
    files: &[InputFile],
    columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
) -> FloeResult<Vec<(InputFile, DataFrame, DataFrame)>> {
    let input = &entity.source;
    match input.format.as_str() {
        "csv" => {
            let default_options = config::SourceOptions::default();
            let source_options = input.options.as_ref().unwrap_or(&default_options);
            let mut inputs = Vec::with_capacity(files.len());
            for input_file in files {
                let path = &input_file.local_path;
                let input_columns = resolve_input_columns(path, source_options, columns)?;
                let raw_schema = build_raw_schema(&input_columns);
                let typed_schema = build_typed_schema(&input_columns, columns, normalize_strategy)?;
                let raw_plan = io::read::CsvReadPlan::strict(raw_schema);
                let typed_plan = io::read::CsvReadPlan::permissive(typed_schema);
                let mut raw_df = io::read::read_csv_file(path, source_options, &raw_plan)?;
                let mut typed_df = io::read::read_csv_file(path, source_options, &typed_plan)?;
                if let Some(strategy) = normalize_strategy {
                    normalize_dataframe_columns(&mut raw_df, strategy)?;
                    normalize_dataframe_columns(&mut typed_df, strategy)?;
                }
                inputs.push((input_file.clone(), raw_df, typed_df));
            }
            Ok(inputs)
        }
        format => Err(Box::new(ConfigError(format!(
            "unsupported source format for now: {format}"
        )))),
    }
}

fn resolve_input_columns(
    path: &Path,
    source_options: &config::SourceOptions,
    declared_columns: &[config::ColumnConfig],
) -> FloeResult<Vec<String>> {
    let header = source_options.header.unwrap_or(true);
    let input_columns = io::read::read_csv_header(path, source_options)?;
    if header {
        return Ok(input_columns);
    }

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

fn build_typed_schema(
    input_columns: &[String],
    declared_columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
) -> FloeResult<Schema> {
    let mut declared_types = HashMap::new();
    for column in declared_columns {
        declared_types.insert(
            column.name.as_str(),
            config::parse_data_type(&column.column_type)?,
        );
    }

    let mut schema = Schema::with_capacity(input_columns.len());
    for name in input_columns {
        let normalized = if let Some(strategy) = normalize_strategy {
            normalize_name(name, strategy)
        } else {
            name.to_string()
        };
        let dtype = declared_types
            .get(normalized.as_str())
            .cloned()
            .unwrap_or(DataType::String);
        schema.insert(name.as_str().into(), dtype);
    }
    Ok(schema)
}

pub(super) fn collect_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    required_cols: &[String],
    columns: &[config::ColumnConfig],
    track_cast_errors: bool,
) -> FloeResult<ValidationCollect> {
    let mut error_lists = check::not_null_errors(typed_df, required_cols)?;
    if track_cast_errors {
        let cast_errors = check::cast_mismatch_errors(raw_df, typed_df, columns)?;
        for (errors, cast) in error_lists.iter_mut().zip(cast_errors) {
            errors.extend(cast);
        }
    }
    let unique_errors = check::unique_errors(typed_df, columns)?;
    for (errors, unique) in error_lists.iter_mut().zip(unique_errors) {
        errors.extend(unique);
    }
    let (accept_rows, errors_json) = check::build_error_state(&error_lists);
    Ok((accept_rows, errors_json, error_lists))
}
