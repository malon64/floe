use std::collections::{HashMap, HashSet};
use std::path::Path;

use apache_avro::types::Value;
use apache_avro::{Reader, Schema};
use polars::prelude::{DataFrame, NamedFrom, Series};

use crate::checks::normalize::normalize_name;
use crate::io::format::{self, FileReadError, InputAdapter, InputFile, ReadInput};
use crate::{config, FloeResult};

struct AvroInputAdapter;

static AVRO_INPUT_ADAPTER: AvroInputAdapter = AvroInputAdapter;

pub(crate) fn avro_input_adapter() -> &'static dyn InputAdapter {
    &AVRO_INPUT_ADAPTER
}

#[derive(Debug, Clone)]
pub struct AvroReadError {
    pub rule: String,
    pub message: String,
}

impl std::fmt::Display for AvroReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.rule, self.message)
    }
}

impl std::error::Error for AvroReadError {}

fn read_avro_schema_fields(input_path: &Path) -> Result<Vec<String>, AvroReadError> {
    let file = std::fs::File::open(input_path).map_err(|err| AvroReadError {
        rule: "avro_read_error".to_string(),
        message: format!("failed to open avro at {}: {err}", input_path.display()),
    })?;
    let reader = Reader::new(file).map_err(|err| AvroReadError {
        rule: "avro_read_error".to_string(),
        message: format!(
            "failed to read avro header at {}: {err}",
            input_path.display()
        ),
    })?;
    schema_field_names(reader.writer_schema())
}

fn schema_field_names(schema: &Schema) -> Result<Vec<String>, AvroReadError> {
    match schema {
        Schema::Record(record) => Ok(record
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect()),
        Schema::Union(union) => {
            let mut record_fields = None;
            for variant in union.variants() {
                match variant {
                    Schema::Null => continue,
                    Schema::Record(record) => {
                        if record_fields.is_some() {
                            return Err(AvroReadError {
                                rule: "avro_schema_error".to_string(),
                                message: "avro schema has multiple record variants at root"
                                    .to_string(),
                            });
                        }
                        record_fields = Some(
                            record
                                .fields
                                .iter()
                                .map(|field| field.name.clone())
                                .collect(),
                        );
                    }
                    _ => {}
                }
            }
            record_fields.ok_or_else(|| AvroReadError {
                rule: "avro_schema_error".to_string(),
                message: "expected avro record schema at root".to_string(),
            })
        }
        _ => Err(AvroReadError {
            rule: "avro_schema_error".to_string(),
            message: "expected avro record schema at root".to_string(),
        }),
    }
}

fn read_avro_file(
    input_path: &Path,
    cast_mode: &str,
    declared_columns: &HashSet<String>,
    normalize_strategy: Option<&str>,
) -> Result<DataFrame, AvroReadError> {
    let file = std::fs::File::open(input_path).map_err(|err| AvroReadError {
        rule: "avro_read_error".to_string(),
        message: format!("failed to open avro at {}: {err}", input_path.display()),
    })?;
    let mut reader = Reader::new(file).map_err(|err| AvroReadError {
        rule: "avro_read_error".to_string(),
        message: format!(
            "failed to read avro header at {}: {err}",
            input_path.display()
        ),
    })?;
    let columns = schema_field_names(reader.writer_schema())?;
    if columns.is_empty() {
        return Err(AvroReadError {
            rule: "avro_schema_error".to_string(),
            message: format!("avro schema has no fields at {}", input_path.display()),
        });
    }

    let mut indices = HashMap::with_capacity(columns.len());
    for (idx, name) in columns.iter().enumerate() {
        indices.insert(name.clone(), idx);
    }

    let mut values = vec![Vec::new(); columns.len()];
    while let Some(item) = reader.next() {
        let value = item.map_err(|err| AvroReadError {
            rule: "avro_read_error".to_string(),
            message: format!(
                "failed to read avro record at {}: {err}",
                input_path.display()
            ),
        })?;
        let mut row = vec![None; columns.len()];
        if let Some(fields) = record_fields_from_root_value(value, input_path)? {
            for (name, value) in fields {
                if !is_declared_field(&name, declared_columns, normalize_strategy) {
                    // Extra Avro fields are handled by mismatch policy; skip conversion here.
                    continue;
                }
                if let Some(index) = indices.get(&name) {
                    row[*index] = value_to_string(&value, cast_mode)?;
                }
            }
        }
        for (index, cell) in row.into_iter().enumerate() {
            values[index].push(cell);
        }
    }

    let mut series = Vec::with_capacity(columns.len());
    for (idx, name) in columns.iter().enumerate() {
        series.push(Series::new(name.as_str().into(), std::mem::take(&mut values[idx])).into());
    }
    DataFrame::new(series).map_err(|err| AvroReadError {
        rule: "avro_read_error".to_string(),
        message: format!("failed to build dataframe: {err}"),
    })
}

fn record_fields_from_root_value(
    value: Value,
    input_path: &Path,
) -> Result<Option<Vec<(String, Value)>>, AvroReadError> {
    match value {
        Value::Record(fields) => Ok(Some(fields)),
        Value::Null => Ok(None),
        Value::Union(_, boxed) => match *boxed {
            Value::Record(fields) => Ok(Some(fields)),
            Value::Null => Ok(None),
            other => Err(AvroReadError {
                rule: "avro_schema_error".to_string(),
                message: format!(
                    "expected avro record value at {}, got {:?}",
                    input_path.display(),
                    other
                ),
            }),
        },
        other => Err(AvroReadError {
            rule: "avro_schema_error".to_string(),
            message: format!(
                "expected avro record value at {}, got {:?}",
                input_path.display(),
                other
            ),
        }),
    }
}

fn is_declared_field(
    field_name: &str,
    declared_columns: &HashSet<String>,
    normalize_strategy: Option<&str>,
) -> bool {
    if declared_columns.contains(field_name) {
        return true;
    }
    normalize_strategy
        .is_some_and(|strategy| declared_columns.contains(&normalize_name(field_name, strategy)))
}

fn value_to_string(value: &Value, cast_mode: &str) -> Result<Option<String>, AvroReadError> {
    match value {
        Value::Null => Ok(None),
        Value::Boolean(value) => Ok(Some(value.to_string())),
        Value::Int(value) => Ok(Some(value.to_string())),
        Value::Long(value) => Ok(Some(value.to_string())),
        Value::Float(value) => Ok(Some(value.to_string())),
        Value::Double(value) => Ok(Some(value.to_string())),
        Value::String(value) => Ok(Some(value.clone())),
        Value::Enum(_, value) => Ok(Some(value.clone())),
        Value::Bytes(value) => Ok(Some(String::from_utf8_lossy(value).to_string())),
        Value::Fixed(_, value) => Ok(Some(String::from_utf8_lossy(value).to_string())),
        Value::Date(value) => Ok(Some(value.to_string())),
        Value::TimeMillis(value) => Ok(Some(value.to_string())),
        Value::TimeMicros(value) => Ok(Some(value.to_string())),
        Value::TimestampMillis(value) => Ok(Some(value.to_string())),
        Value::TimestampMicros(value) => Ok(Some(value.to_string())),
        Value::LocalTimestampMillis(value) => Ok(Some(value.to_string())),
        Value::LocalTimestampMicros(value) => Ok(Some(value.to_string())),
        Value::Uuid(value) => Ok(Some(value.to_string())),
        Value::Union(_, value) => value_to_string(value, cast_mode),
        other => {
            if cast_mode == "coerce" {
                Ok(Some(format!("{other:?}")))
            } else {
                Err(AvroReadError {
                    rule: "avro_cast_error".to_string(),
                    message: format!("unsupported avro value: {other:?}"),
                })
            }
        }
    }
}

impl InputAdapter for AvroInputAdapter {
    fn format(&self) -> &'static str {
        "avro"
    }

    fn read_input_columns(
        &self,
        _entity: &config::EntityConfig,
        input_file: &InputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError> {
        read_avro_schema_fields(&input_file.source_local_path).map_err(|err| FileReadError {
            rule: err.rule,
            message: err.message,
        })
    }

    fn read_inputs(
        &self,
        entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>> {
        let cast_mode = entity.source.cast_mode.as_deref().unwrap_or("strict");
        let declared_columns = columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<HashSet<_>>();
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            match read_avro_file(
                &input_file.source_local_path,
                cast_mode,
                &declared_columns,
                normalize_strategy,
            ) {
                Ok(df) => {
                    let input = format::read_input_from_df(
                        input_file,
                        &df,
                        columns,
                        normalize_strategy,
                        collect_raw,
                    )?;
                    inputs.push(input);
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
