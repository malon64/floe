use std::path::Path;

use polars::polars_utils::pl_str::PlSmallStr;
use polars::prelude::{
    CsvEncoding, CsvParseOptions, CsvReadOptions, DataType, NullValues, Schema, TimeUnit,
};

use crate::{ConfigError, FloeResult};

#[derive(Debug)]
pub struct RootConfig {
    pub version: String,
    pub metadata: Option<ProjectMetadata>,
    pub storages: Option<StoragesConfig>,
    pub report: Option<ReportConfig>,
    pub entities: Vec<EntityConfig>,
}

#[derive(Debug)]
pub struct ProjectMetadata {
    pub project: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct EntityConfig {
    pub name: String,
    pub metadata: Option<EntityMetadata>,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub policy: PolicyConfig,
    pub schema: SchemaConfig,
}

#[derive(Debug)]
pub struct EntityMetadata {
    pub data_product: Option<String>,
    pub domain: Option<String>,
    pub owner: Option<String>,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct SourceConfig {
    pub format: String,
    pub path: String,
    pub storage: Option<String>,
    pub options: Option<SourceOptions>,
    pub cast_mode: Option<String>,
}

#[derive(Debug)]
pub struct SourceOptions {
    pub header: Option<bool>,
    pub separator: Option<String>,
    pub encoding: Option<String>,
    pub null_values: Option<Vec<String>>,
    pub recursive: Option<bool>,
    pub glob: Option<String>,
    pub ndjson: Option<bool>,
    pub array: Option<bool>,
}

impl Default for SourceOptions {
    fn default() -> Self {
        Self {
            header: Some(true),
            separator: Some(";".to_string()),
            encoding: Some("UTF8".to_string()),
            null_values: Some(Vec::new()),
            recursive: Some(false),
            glob: None,
            ndjson: Some(false),
            array: Some(false),
        }
    }
}

impl SourceOptions {
    pub fn to_csv_parse_options(&self) -> FloeResult<CsvParseOptions> {
        let separator = parse_separator(self.separator.as_deref().unwrap_or(";"))?;
        let encoding = parse_encoding(self.encoding.as_deref())?;
        let null_values = build_null_values(self.null_values.as_ref());
        Ok(CsvParseOptions::default()
            .with_separator(separator)
            .with_encoding(encoding)
            .with_null_values(null_values))
    }

    pub fn to_csv_read_options(&self, input_path: &Path) -> FloeResult<CsvReadOptions> {
        let header = self.header.unwrap_or(true);
        let parse_options = self.to_csv_parse_options()?;

        Ok(CsvReadOptions::default()
            .with_path(Some(input_path))
            .with_has_header(header)
            .with_parse_options(parse_options))
    }
}

fn parse_separator(value: &str) -> FloeResult<u8> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(Box::new(ConfigError(format!(
            "separator must be a single byte, got {value:?}"
        ))));
    }
    Ok(bytes[0])
}

fn parse_encoding(value: Option<&str>) -> FloeResult<CsvEncoding> {
    let normalized = value
        .unwrap_or("utf8")
        .to_ascii_lowercase()
        .replace(['-', '_'], "");
    match normalized.as_str() {
        "utf8" => Ok(CsvEncoding::Utf8),
        "lossyutf8" => Ok(CsvEncoding::LossyUtf8),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported encoding: {}",
            value.unwrap_or("utf8")
        )))),
    }
}

fn build_null_values(values: Option<&Vec<String>>) -> Option<NullValues> {
    let values = values?;
    if values.is_empty() {
        return None;
    }
    if values.len() == 1 {
        return Some(NullValues::AllColumnsSingle(PlSmallStr::from(
            values[0].as_str(),
        )));
    }
    Some(NullValues::AllColumns(
        values.iter().map(|value| value.as_str().into()).collect(),
    ))
}

#[derive(Debug)]
pub struct SinkConfig {
    pub accepted: SinkTarget,
    pub rejected: Option<SinkTarget>,
    pub archive: Option<ArchiveTarget>,
}

#[derive(Debug)]
pub struct SinkTarget {
    pub format: String,
    pub path: String,
    pub storage: Option<String>,
    pub options: Option<SinkOptions>,
}

#[derive(Debug)]
pub struct SinkOptions {
    pub compression: Option<String>,
    pub row_group_size: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StoragesConfig {
    pub default: Option<String>,
    pub definitions: Vec<StorageDefinition>,
}

#[derive(Debug, Clone)]
pub struct StorageDefinition {
    pub name: String,
    pub fs_type: String,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub prefix: Option<String>,
}

#[derive(Debug)]
pub struct ReportConfig {
    pub path: String,
    pub formatter: Option<String>,
}

#[derive(Debug)]
pub struct ArchiveTarget {
    pub path: String,
}

#[derive(Debug)]
pub struct PolicyConfig {
    pub severity: String,
}

#[derive(Debug)]
pub struct SchemaConfig {
    pub normalize_columns: Option<NormalizeColumnsConfig>,
    pub mismatch: Option<SchemaMismatchConfig>,
    pub columns: Vec<ColumnConfig>,
}

impl SchemaConfig {
    pub fn to_polars_schema(&self) -> FloeResult<Schema> {
        let mut schema = Schema::with_capacity(self.columns.len());
        for column in &self.columns {
            let dtype = parse_data_type(&column.column_type)?;
            if schema.insert(column.name.as_str().into(), dtype).is_some() {
                return Err(Box::new(ConfigError(format!(
                    "duplicate column name in schema: {}",
                    column.name
                ))));
            }
        }
        Ok(schema)
    }

    pub fn to_polars_string_schema(&self) -> FloeResult<Schema> {
        let mut schema = Schema::with_capacity(self.columns.len());
        for column in &self.columns {
            if schema
                .insert(column.name.as_str().into(), DataType::String)
                .is_some()
            {
                return Err(Box::new(ConfigError(format!(
                    "duplicate column name in schema: {}",
                    column.name
                ))));
            }
        }
        Ok(schema)
    }
}

#[derive(Debug)]
pub struct NormalizeColumnsConfig {
    pub enabled: Option<bool>,
    pub strategy: Option<String>,
}

#[derive(Debug)]
pub struct SchemaMismatchConfig {
    pub missing_columns: Option<String>,
    pub extra_columns: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnConfig {
    pub name: String,
    pub column_type: String,
    pub nullable: Option<bool>,
    pub unique: Option<bool>,
}

pub(crate) fn parse_data_type(value: &str) -> FloeResult<DataType> {
    let normalized = value.to_ascii_lowercase().replace(['-', '_'], "");
    match normalized.as_str() {
        "string" | "str" | "text" => Ok(DataType::String),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" | "int" | "integer" | "long" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" => Ok(DataType::Float32),
        "float64" | "float" | "double" | "number" | "decimal" => Ok(DataType::Float64),
        "date" => Ok(DataType::Date),
        "datetime" | "timestamp" => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
        "time" => Ok(DataType::Time),
        _ => Err(Box::new(ConfigError(format!(
            "unsupported column type: {value}"
        )))),
    }
}
