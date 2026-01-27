use std::collections::HashMap;
use std::path::{Path, PathBuf};

use polars::chunked_array::cast::CastOptions;
use polars::prelude::{Column, DataFrame, DataType, NamedFrom, Schema, Series};

use crate::{check, config, io, ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub struct InputFile {
    pub source_uri: String,
    pub local_path: PathBuf,
    pub source_name: String,
    pub source_stem: String,
}

#[derive(Debug, Clone)]
pub struct FileReadError {
    pub rule: String,
    pub message: String,
}

pub enum ReadInput {
    Data {
        input_file: InputFile,
        input_columns: Vec<String>,
        raw_df: Option<DataFrame>,
        typed_df: DataFrame,
    },
    FileError {
        input_file: InputFile,
        error: FileReadError,
    },
}

pub enum StorageTarget {
    Local {
        base_path: String,
    },
    S3 {
        storage: String,
        bucket: String,
        base_key: String,
    },
}

pub type ValidationCollect = (Vec<bool>, Vec<Option<String>>, Vec<Vec<check::RowError>>);

pub trait InputAdapter: Send + Sync {
    fn format(&self) -> &'static str;

    fn default_globs(&self) -> FloeResult<Vec<String>> {
        io::fs::extensions::glob_patterns_for_format(self.format())
    }

    fn suffixes(&self) -> FloeResult<Vec<String>> {
        io::fs::extensions::suffixes_for_format(self.format())
    }

    fn resolve_local_inputs(
        &self,
        config_dir: &Path,
        entity_name: &str,
        source: &config::SourceConfig,
        storage: &str,
    ) -> FloeResult<io::fs::local::ResolvedLocalInputs> {
        let default_globs = self.default_globs()?;
        io::fs::local::resolve_local_inputs(
            config_dir,
            entity_name,
            source,
            storage,
            &default_globs,
        )
    }

    fn read_inputs(
        &self,
        entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>>;
}

pub trait AcceptedSinkAdapter: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn write_accepted(
        &self,
        target: &StorageTarget,
        df: &mut DataFrame,
        source_stem: &str,
        temp_dir: Option<&Path>,
        s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
        resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String>;
}

pub trait RejectedSinkAdapter: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn write_rejected(
        &self,
        target: &StorageTarget,
        df: &mut DataFrame,
        source_stem: &str,
        temp_dir: Option<&Path>,
        s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
        resolver: &config::StorageResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String>;
}

#[derive(Debug, Clone, Copy)]
pub enum FormatKind {
    Source,
    SinkAccepted,
    SinkRejected,
}

impl FormatKind {
    fn field_path(self) -> &'static str {
        match self {
            FormatKind::Source => "source.format",
            FormatKind::SinkAccepted => "sink.accepted.format",
            FormatKind::SinkRejected => "sink.rejected.format",
        }
    }

    fn description(self) -> &'static str {
        match self {
            FormatKind::Source => "source format",
            FormatKind::SinkAccepted => "accepted sink format",
            FormatKind::SinkRejected => "rejected sink format",
        }
    }
}

fn unsupported_format_error(
    kind: FormatKind,
    format: &str,
    entity_name: Option<&str>,
) -> ConfigError {
    if let Some(entity_name) = entity_name {
        return ConfigError(format!(
            "entity.name={} {}={} is unsupported",
            entity_name,
            kind.field_path(),
            format
        ));
    }
    ConfigError(format!("unsupported {}: {format}", kind.description()))
}

pub fn ensure_input_format(entity_name: &str, format: &str) -> FloeResult<()> {
    if input_adapter(format).is_err() {
        return Err(Box::new(unsupported_format_error(
            FormatKind::Source,
            format,
            Some(entity_name),
        )));
    }
    Ok(())
}

pub fn ensure_accepted_sink_format(entity_name: &str, format: &str) -> FloeResult<()> {
    if accepted_sink_adapter(format).is_err() {
        return Err(Box::new(unsupported_format_error(
            FormatKind::SinkAccepted,
            format,
            Some(entity_name),
        )));
    }
    Ok(())
}

pub fn ensure_rejected_sink_format(entity_name: &str, format: &str) -> FloeResult<()> {
    if rejected_sink_adapter(format).is_err() {
        return Err(Box::new(unsupported_format_error(
            FormatKind::SinkRejected,
            format,
            Some(entity_name),
        )));
    }
    Ok(())
}

pub fn input_adapter(format: &str) -> FloeResult<&'static dyn InputAdapter> {
    match format {
        "csv" => Ok(io::read::csv::csv_input_adapter()),
        "parquet" => Ok(io::read::parquet::parquet_input_adapter()),
        "json" => Ok(io::read::json::json_input_adapter()),
        _ => Err(Box::new(unsupported_format_error(
            FormatKind::Source,
            format,
            None,
        ))),
    }
}

pub fn accepted_sink_adapter(format: &str) -> FloeResult<&'static dyn AcceptedSinkAdapter> {
    match format {
        "parquet" => Ok(io::write::parquet::parquet_accepted_adapter()),
        "delta" => Ok(io::write::delta::delta_accepted_adapter()),
        "iceberg" => Ok(io::write::iceberg::iceberg_accepted_adapter()),
        _ => Err(Box::new(unsupported_format_error(
            FormatKind::SinkAccepted,
            format,
            None,
        ))),
    }
}

pub fn rejected_sink_adapter(format: &str) -> FloeResult<&'static dyn RejectedSinkAdapter> {
    match format {
        "csv" => Ok(io::write::csv::csv_rejected_adapter()),
        _ => Err(Box::new(unsupported_format_error(
            FormatKind::SinkRejected,
            format,
            None,
        ))),
    }
}

pub(crate) fn read_input_from_df(
    input_file: &InputFile,
    df: &DataFrame,
    columns: &[config::ColumnConfig],
    normalize_strategy: Option<&str>,
    collect_raw: bool,
) -> FloeResult<ReadInput> {
    let input_columns = df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let typed_schema = build_typed_schema(&input_columns, columns, normalize_strategy)?;
    let raw_df = if collect_raw {
        Some(cast_df_to_string(df)?)
    } else {
        None
    };
    let typed_df = cast_df_to_schema(df, &typed_schema)?;
    finalize_read_input(
        input_file,
        input_columns,
        raw_df,
        typed_df,
        normalize_strategy,
    )
}

pub(crate) fn finalize_read_input(
    input_file: &InputFile,
    input_columns: Vec<String>,
    mut raw_df: Option<DataFrame>,
    mut typed_df: DataFrame,
    normalize_strategy: Option<&str>,
) -> FloeResult<ReadInput> {
    if let Some(strategy) = normalize_strategy {
        if let Some(raw_df) = raw_df.as_mut() {
            crate::run::normalize::normalize_dataframe_columns(raw_df, strategy)?;
        }
        crate::run::normalize::normalize_dataframe_columns(&mut typed_df, strategy)?;
    }
    Ok(ReadInput::Data {
        input_file: input_file.clone(),
        input_columns,
        raw_df,
        typed_df,
    })
}

pub(crate) fn build_typed_schema(
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
            crate::run::normalize::normalize_name(name, strategy)
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

pub(crate) fn cast_df_to_string(df: &DataFrame) -> FloeResult<DataFrame> {
    cast_df_with_type(df, &DataType::String)
}

pub(crate) fn cast_df_to_schema(df: &DataFrame, schema: &Schema) -> FloeResult<DataFrame> {
    let mut columns = Vec::with_capacity(schema.len());
    for (name, dtype) in schema.iter() {
        let series = df.column(name.as_str()).map_err(|err| {
            Box::new(ConfigError(format!(
                "input column {} not found: {err}",
                name.as_str()
            )))
        })?;
        let casted =
            if matches!(dtype, DataType::Boolean) && matches!(series.dtype(), DataType::String) {
                cast_string_to_bool(name.as_str(), series)?
            } else {
                series
                    .cast_with_options(dtype, CastOptions::NonStrict)
                    .map_err(|err| {
                        Box::new(ConfigError(format!(
                            "failed to cast input column {}: {err}",
                            name.as_str()
                        )))
                    })?
            };
        columns.push(casted);
    }
    DataFrame::new(columns).map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to build typed dataframe: {err}"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

fn cast_string_to_bool(name: &str, series: &Column) -> FloeResult<Column> {
    let string_values = series.as_materialized_series().str().map_err(|err| {
        Box::new(ConfigError(format!(
            "failed to read boolean column {} as string: {err}",
            name
        )))
    })?;
    let mut values = Vec::with_capacity(series.len());
    for value in string_values {
        let parsed = value.and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        });
        values.push(parsed);
    }
    Ok(Series::new(name.into(), values).into())
}

fn cast_df_with_type(df: &DataFrame, dtype: &DataType) -> FloeResult<DataFrame> {
    let mut out = df.clone();
    let names = out
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    for name in names {
        let series = out.column(&name).map_err(|err| {
            Box::new(ConfigError(format!(
                "input column {} not found: {err}",
                name
            )))
        })?;
        let casted = series
            .cast_with_options(dtype, CastOptions::NonStrict)
            .map_err(|err| {
                Box::new(ConfigError(format!(
                    "failed to cast input column {}: {err}",
                    name
                )))
            })?;
        let idx = out.get_column_index(&name).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "input column {} not found for update",
                name
            )))
        })?;
        out.replace_column(idx, casted).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to update input column {}: {err}",
                name
            )))
        })?;
    }
    Ok(out)
}

pub fn collect_errors(
    raw_df: &DataFrame,
    typed_df: &DataFrame,
    required_cols: &[String],
    columns: &[config::ColumnConfig],
    track_cast_errors: bool,
    raw_indices: &check::ColumnIndex,
    typed_indices: &check::ColumnIndex,
) -> FloeResult<ValidationCollect> {
    let mut error_lists = check::not_null_errors(typed_df, required_cols, typed_indices)?;
    if track_cast_errors {
        let cast_errors =
            check::cast_mismatch_errors(raw_df, typed_df, columns, raw_indices, typed_indices)?;
        for (errors, cast) in error_lists.iter_mut().zip(cast_errors) {
            errors.extend(cast);
        }
    }
    let unique_errors = check::unique_errors(typed_df, columns, typed_indices)?;
    for (errors, unique) in error_lists.iter_mut().zip(unique_errors) {
        errors.extend(unique);
    }
    let accept_rows = check::build_accept_rows(&error_lists);
    let errors_json = check::build_errors_json(&error_lists, &accept_rows);
    Ok((accept_rows, errors_json, error_lists))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_registry_returns_csv_adapter() {
        let adapter = input_adapter("csv").expect("adapter");
        assert_eq!(adapter.format(), "csv");
    }
}
