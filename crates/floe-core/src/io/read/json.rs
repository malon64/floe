use std::collections::{BTreeMap, HashSet};
use std::io::BufRead;
use std::path::Path;

use polars::prelude::{DataFrame, NamedFrom, Series};
use serde_json::Value;

use crate::io::format::{self, FileReadError, InputAdapter, InputFile, ReadInput};
use crate::{config, FloeResult};

struct JsonInputAdapter;

static JSON_INPUT_ADAPTER: JsonInputAdapter = JsonInputAdapter;

pub(crate) fn json_input_adapter() -> &'static dyn InputAdapter {
    &JSON_INPUT_ADAPTER
}

#[derive(Debug, Clone)]
pub struct JsonReadError {
    pub rule: String,
    pub message: String,
}

impl std::fmt::Display for JsonReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.rule, self.message)
    }
}

impl std::error::Error for JsonReadError {}

pub fn read_ndjson_file(input_path: &Path) -> Result<DataFrame, JsonReadError> {
    let content = std::fs::read_to_string(input_path).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("failed to read json at {}: {err}", input_path.display()),
    })?;

    let mut rows: Vec<BTreeMap<String, Option<String>>> = Vec::new();
    let mut keys = HashSet::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line).map_err(|err| JsonReadError {
            rule: "json_parse_error".to_string(),
            message: format!("json parse error at line {}: {err}", idx + 1),
        })?;
        let object = value.as_object().ok_or_else(|| JsonReadError {
            rule: "json_parse_error".to_string(),
            message: format!("expected json object at line {}", idx + 1),
        })?;

        let mut row = BTreeMap::new();
        for (key, value) in object {
            if value.is_object() || value.is_array() {
                return Err(JsonReadError {
                    rule: "json_unsupported_value".to_string(),
                    message: format!(
                        "nested json values are not supported (line {}, key {})",
                        idx + 1,
                        key
                    ),
                });
            }
            let cell = match value {
                Value::Null => None,
                Value::String(value) => Some(value.clone()),
                Value::Bool(value) => Some(value.to_string()),
                Value::Number(value) => Some(value.to_string()),
                _ => None,
            };
            row.insert(key.clone(), cell);
            keys.insert(key.clone());
        }
        rows.push(row);
    }

    let mut columns = keys.into_iter().collect::<Vec<_>>();
    columns.sort();

    let mut series = Vec::with_capacity(columns.len());
    for name in &columns {
        let mut values = Vec::with_capacity(rows.len());
        for row in &rows {
            values.push(row.get(name).cloned().unwrap_or(None));
        }
        series.push(Series::new(name.as_str().into(), values).into());
    }

    DataFrame::new(series).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("failed to build dataframe: {err}"),
    })
}

pub fn read_ndjson_columns(input_path: &Path) -> Result<Vec<String>, JsonReadError> {
    let file = std::fs::File::open(input_path).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("failed to read json at {}: {err}", input_path.display()),
    })?;
    let reader = std::io::BufReader::new(file);
    let mut first_error: Option<JsonReadError> = None;

    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|err| JsonReadError {
            rule: "json_parse_error".to_string(),
            message: format!("failed to read json at {}: {err}", input_path.display()),
        })?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(value) => {
                let object = value.as_object().ok_or_else(|| JsonReadError {
                    rule: "json_parse_error".to_string(),
                    message: format!("expected json object at line {}", idx + 1),
                })?;
                let mut keys = Vec::with_capacity(object.len());
                for (key, value) in object {
                    if value.is_object() || value.is_array() {
                        return Err(JsonReadError {
                            rule: "json_unsupported_value".to_string(),
                            message: format!(
                                "nested json values are not supported (line {}, key {})",
                                idx + 1,
                                key
                            ),
                        });
                    }
                    keys.push(key.clone());
                }
                keys.sort();
                return Ok(keys);
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(JsonReadError {
                        rule: "json_parse_error".to_string(),
                        message: format!("json parse error at line {}: {err}", idx + 1),
                    });
                }
                continue;
            }
        }
    }

    Err(first_error.unwrap_or_else(|| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("no json objects found in {}", input_path.display()),
    }))
}

impl InputAdapter for JsonInputAdapter {
    fn format(&self) -> &'static str {
        "json"
    }

    fn read_input_columns(
        &self,
        _entity: &config::EntityConfig,
        input_file: &InputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError> {
        read_ndjson_columns(&input_file.source_local_path).map_err(|err| FileReadError {
            rule: err.rule,
            message: err.message,
        })
    }

    fn read_inputs(
        &self,
        _entity: &config::EntityConfig,
        files: &[InputFile],
        columns: &[config::ColumnConfig],
        normalize_strategy: Option<&str>,
        collect_raw: bool,
    ) -> FloeResult<Vec<ReadInput>> {
        let mut inputs = Vec::with_capacity(files.len());
        for input_file in files {
            let path = &input_file.source_local_path;
            match read_ndjson_file(path) {
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
