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
        raw_df: DataFrame,
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
        filesystem: String,
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
        filesystem: &str,
    ) -> FloeResult<io::fs::local::ResolvedLocalInputs> {
        let default_globs = self.default_globs()?;
        io::fs::local::resolve_local_inputs(
            config_dir,
            entity_name,
            source,
            filesystem,
            &default_globs,
        )
    }

    fn read_inputs(
        &self,
        entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
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
        resolver: &config::FilesystemResolver,
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
        resolver: &config::FilesystemResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String>;
}

pub fn input_adapter(format: &str) -> FloeResult<&'static dyn InputAdapter> {
    match format {
        "csv" => Ok(&CSV_INPUT_ADAPTER),
        "parquet" => Ok(&PARQUET_INPUT_ADAPTER),
        "json" => Ok(&JSON_INPUT_ADAPTER),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported source format: {format}"
        )))),
    }
}

pub fn accepted_sink_adapter(format: &str) -> FloeResult<&'static dyn AcceptedSinkAdapter> {
    match format {
        "parquet" => Ok(&PARQUET_ACCEPTED_ADAPTER),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported accepted sink format: {format}"
        )))),
    }
}

pub fn rejected_sink_adapter(format: &str) -> FloeResult<&'static dyn RejectedSinkAdapter> {
    match format {
        "csv" => Ok(&CSV_REJECTED_ADAPTER),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported rejected sink format: {format}"
        )))),
    }
}

struct CsvInputAdapter;
struct ParquetInputAdapter;
struct JsonInputAdapter;

struct ParquetAcceptedAdapter;
struct CsvRejectedAdapter;

static CSV_INPUT_ADAPTER: CsvInputAdapter = CsvInputAdapter;
static PARQUET_INPUT_ADAPTER: ParquetInputAdapter = ParquetInputAdapter;
static JSON_INPUT_ADAPTER: JsonInputAdapter = JsonInputAdapter;

static PARQUET_ACCEPTED_ADAPTER: ParquetAcceptedAdapter = ParquetAcceptedAdapter;
static CSV_REJECTED_ADAPTER: CsvRejectedAdapter = CsvRejectedAdapter;

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
            let typed_schema = build_typed_schema(&input_columns, columns, normalize_strategy)?;
            let raw_plan = io::read::CsvReadPlan::strict(raw_schema);
            let typed_plan = io::read::CsvReadPlan::permissive(typed_schema);
            let mut raw_df = io::read::read_csv_file(path, source_options, &raw_plan)?;
            let mut typed_df = io::read::read_csv_file(path, source_options, &typed_plan)?;
            if let Some(strategy) = normalize_strategy {
                crate::run::normalize::normalize_dataframe_columns(&mut raw_df, strategy)?;
                crate::run::normalize::normalize_dataframe_columns(&mut typed_df, strategy)?;
            }
            inputs.push(ReadInput::Data {
                input_file: input_file.clone(),
                raw_df,
                typed_df,
            });
        }
        Ok(inputs)
    }
}

impl InputAdapter for ParquetInputAdapter {
    fn format(&self) -> &'static str {
        "parquet"
    }

    fn read_inputs(
        &self,
        _entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
    ) -> FloeResult<Vec<ReadInput>> {
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.local_path;
            let df = io::read::read_parquet_file(path)?;
            let input_columns = df
                .get_column_names()
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>();
            let typed_schema = build_typed_schema(&input_columns, columns, normalize_strategy)?;
            let mut raw_df = cast_df_to_string(&df)?;
            let mut typed_df = cast_df_to_schema(&df, &typed_schema)?;
            if let Some(strategy) = normalize_strategy {
                crate::run::normalize::normalize_dataframe_columns(&mut raw_df, strategy)?;
                crate::run::normalize::normalize_dataframe_columns(&mut typed_df, strategy)?;
            }
            inputs.push(ReadInput::Data {
                input_file: input_file.clone(),
                raw_df,
                typed_df,
            });
        }
        Ok(inputs)
    }
}

impl InputAdapter for JsonInputAdapter {
    fn format(&self) -> &'static str {
        "json"
    }

    fn read_inputs(
        &self,
        _entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
    ) -> FloeResult<Vec<ReadInput>> {
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.local_path;
            match io::read::read_ndjson_file(path) {
                Ok(df) => {
                    let input_columns = df
                        .get_column_names()
                        .iter()
                        .map(|name| name.to_string())
                        .collect::<Vec<_>>();
                    let typed_schema =
                        build_typed_schema(&input_columns, columns, normalize_strategy)?;
                    let mut raw_df = cast_df_to_string(&df)?;
                    let mut typed_df = cast_df_to_schema(&df, &typed_schema)?;
                    if let Some(strategy) = normalize_strategy {
                        crate::run::normalize::normalize_dataframe_columns(&mut raw_df, strategy)?;
                        crate::run::normalize::normalize_dataframe_columns(
                            &mut typed_df,
                            strategy,
                        )?;
                    }
                    inputs.push(ReadInput::Data {
                        input_file: input_file.clone(),
                        raw_df,
                        typed_df,
                    });
                }
                Err(err) => {
                    inputs.push(ReadInput::FileError {
                        input_file: input_file.clone(),
                        error: FileReadError {
                            rule: err.rule,
                            message: err.message,
                        },
                    });
                }
            }
        }
        Ok(inputs)
    }
}

impl AcceptedSinkAdapter for ParquetAcceptedAdapter {
    fn write_accepted(
        &self,
        target: &StorageTarget,
        df: &mut DataFrame,
        source_stem: &str,
        temp_dir: Option<&Path>,
        s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
        resolver: &config::FilesystemResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String> {
        match target {
            StorageTarget::Local { base_path } => {
                let output_path = io::write::write_parquet(df, base_path, source_stem)?;
                Ok(output_path.display().to_string())
            }
            StorageTarget::S3 {
                filesystem,
                bucket,
                base_key,
            } => {
                let temp_dir = temp_dir.ok_or_else(|| {
                    Box::new(ConfigError(format!(
                        "entity.name={} missing temp dir for s3 output",
                        entity.name
                    )))
                })?;
                let temp_base = temp_dir.display().to_string();
                let local_path = io::write::write_parquet(df, &temp_base, source_stem)?;
                let key = io::fs::s3::build_parquet_key(base_key, source_stem);
                let client =
                    crate::run::entity::s3_client_for(s3_clients, resolver, filesystem, entity)?;
                client.upload_file(bucket, &key, &local_path)?;
                Ok(io::fs::s3::format_s3_uri(bucket, &key))
            }
        }
    }
}

impl RejectedSinkAdapter for CsvRejectedAdapter {
    fn write_rejected(
        &self,
        target: &StorageTarget,
        df: &mut DataFrame,
        source_stem: &str,
        temp_dir: Option<&Path>,
        s3_clients: &mut HashMap<String, io::fs::s3::S3Client>,
        resolver: &config::FilesystemResolver,
        entity: &config::EntityConfig,
    ) -> FloeResult<String> {
        match target {
            StorageTarget::Local { base_path } => {
                let output_path = io::write::write_rejected_csv(df, base_path, source_stem)?;
                Ok(output_path.display().to_string())
            }
            StorageTarget::S3 {
                filesystem,
                bucket,
                base_key,
            } => {
                let temp_dir = temp_dir.ok_or_else(|| {
                    Box::new(ConfigError(format!(
                        "entity.name={} missing temp dir for s3 output",
                        entity.name
                    )))
                })?;
                let temp_base = temp_dir.display().to_string();
                let local_path = io::write::write_rejected_csv(df, &temp_base, source_stem)?;
                let key = io::fs::s3::build_rejected_csv_key(base_key, source_stem);
                let client =
                    crate::run::entity::s3_client_for(s3_clients, resolver, filesystem, entity)?;
                client.upload_file(bucket, &key, &local_path)?;
                Ok(io::fs::s3::format_s3_uri(bucket, &key))
            }
        }
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

fn cast_df_to_string(df: &DataFrame) -> FloeResult<DataFrame> {
    cast_df_with_type(df, &DataType::String)
}

fn cast_df_to_schema(df: &DataFrame, schema: &Schema) -> FloeResult<DataFrame> {
    let mut out = df.clone();
    for (name, dtype) in schema.iter() {
        let series = out.column(name.as_str()).map_err(|err| {
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
        let idx = out.get_column_index(name.as_str()).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "input column {} not found for update",
                name.as_str()
            )))
        })?;
        out.replace_column(idx, casted).map_err(|err| {
            Box::new(ConfigError(format!(
                "failed to update input column {}: {err}",
                name.as_str()
            )))
        })?;
    }
    Ok(out)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_registry_returns_csv_adapter() {
        let adapter = input_adapter("csv").expect("adapter");
        assert_eq!(adapter.format(), "csv");
    }
}
