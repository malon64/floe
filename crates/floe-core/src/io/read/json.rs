use std::collections::HashSet;
use std::io::BufRead;
use std::path::Path;

use polars::prelude::{DataFrame, NamedFrom, Series};
use serde_json::Value;

use crate::io::format::{self, FileReadError, InputAdapter, InputFile, ReadInput};
use crate::io::read::json_selector::{
    compact_json, evaluate_selector, parse_selector, SelectorToken, SelectorValue,
};
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

struct SelectorPlan {
    source: String,
    tokens: Vec<SelectorToken>,
    top_level_key: Option<String>,
}

fn build_selector_plan(
    columns: &[config::ColumnConfig],
) -> Result<Vec<SelectorPlan>, JsonReadError> {
    let mut plans = Vec::with_capacity(columns.len());
    let mut seen = HashSet::new();
    for column in columns {
        let source = column.source_or_name().to_string();
        if !seen.insert(source.clone()) {
            return Err(JsonReadError {
                rule: "json_selector_invalid".to_string(),
                message: format!("duplicate json selector source: {}", source),
            });
        }
        let tokens = parse_selector(&source).map_err(|err| JsonReadError {
            rule: "json_selector_invalid".to_string(),
            message: format!("invalid selector {}: {}", source, err.message),
        })?;
        let top_level_key = match tokens.as_slice() {
            [SelectorToken::Field(name)] => Some(name.clone()),
            _ => None,
        };
        plans.push(SelectorPlan {
            source,
            tokens,
            top_level_key,
        });
    }
    Ok(plans)
}

fn evaluate_selector_plan(
    value: &Value,
    plan: &SelectorPlan,
) -> Result<SelectorValue, JsonReadError> {
    if let Some(key) = plan.top_level_key.as_deref() {
        let Some(object) = value.as_object() else {
            return Ok(SelectorValue::Null);
        };
        let Some(current) = object.get(key) else {
            return Ok(SelectorValue::Null);
        };
        if current.is_null() {
            return Ok(SelectorValue::Null);
        }
        if current.is_object() || current.is_array() {
            return Ok(SelectorValue::NonScalar(current.clone()));
        }
        return match current {
            Value::String(value) => Ok(SelectorValue::Scalar(value.clone())),
            Value::Bool(value) => Ok(SelectorValue::Scalar(value.to_string())),
            Value::Number(value) => Ok(SelectorValue::Scalar(value.to_string())),
            Value::Null => Ok(SelectorValue::Null),
            Value::Object(_) | Value::Array(_) => Ok(SelectorValue::NonScalar(current.clone())),
        };
    }

    evaluate_selector(value, &plan.tokens).map_err(|err| JsonReadError {
        rule: "json_selector_invalid".to_string(),
        message: format!("invalid selector {}: {}", plan.source, err.message),
    })
}

fn append_extracted_row_into(
    value: &Value,
    plans: &[SelectorPlan],
    cast_mode: &str,
    location_kind: &'static str,
    location_index: usize,
    output_columns: &mut [Vec<Option<String>>],
) -> Result<(), JsonReadError> {
    for (output, plan) in output_columns.iter_mut().zip(plans) {
        let selected = evaluate_selector_plan(value, plan)?;
        let cell = match selected {
            SelectorValue::Null => None,
            SelectorValue::Scalar(value) => Some(value),
            SelectorValue::NonScalar(value) => {
                let location = format!("{location_kind} {location_index}");
                if cast_mode == "coerce" {
                    Some(compact_json(&value).map_err(|err| JsonReadError {
                        rule: "json_selector_non_scalar".to_string(),
                        message: format!(
                            "failed to stringify selector {} at {}: {}",
                            plan.source, location, err.message
                        ),
                    })?)
                } else {
                    return Err(JsonReadError {
                        rule: "json_selector_non_scalar".to_string(),
                        message: format!(
                            "non-scalar value for selector {} at {}",
                            plan.source, location
                        ),
                    });
                }
            }
        };
        output.push(cell);
    }
    Ok(())
}

fn read_ndjson_file(
    input_path: &Path,
    columns: &[config::ColumnConfig],
    cast_mode: &str,
) -> Result<DataFrame, JsonReadError> {
    let content = std::fs::read_to_string(input_path).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("failed to read json at {}: {err}", input_path.display()),
    })?;

    let plans = build_selector_plan(columns)?;
    let mut column_values: Vec<Vec<Option<String>>> = vec![Vec::new(); plans.len()];
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line).map_err(|err| JsonReadError {
            rule: "json_parse_error".to_string(),
            message: format!("json parse error at line {}: {err}", idx + 1),
        })?;
        if !value.is_object() {
            return Err(JsonReadError {
                rule: "json_parse_error".to_string(),
                message: format!("expected json object at line {}", idx + 1),
            });
        }

        append_extracted_row_into(
            &value,
            &plans,
            cast_mode,
            "line",
            idx + 1,
            &mut column_values,
        )?;
    }

    build_dataframe(&plans, column_values)
}

fn read_ndjson_columns(input_path: &Path) -> Result<Vec<String>, JsonReadError> {
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
                let mut keys = object.keys().cloned().collect::<Vec<_>>();
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

fn read_json_array_file(
    input_path: &Path,
    columns: &[config::ColumnConfig],
    cast_mode: &str,
) -> Result<DataFrame, JsonReadError> {
    let content = std::fs::read_to_string(input_path).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("failed to read json at {}: {err}", input_path.display()),
    })?;

    let value: Value = serde_json::from_str(&content).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("json parse error: {err}"),
    })?;
    let array = value.as_array().ok_or_else(|| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: "expected json array at root".to_string(),
    })?;

    let plans = build_selector_plan(columns)?;
    let mut column_values: Vec<Vec<Option<String>>> = (0..plans.len())
        .map(|_| Vec::with_capacity(array.len()))
        .collect();

    for (idx, value) in array.iter().enumerate() {
        if !value.is_object() {
            return Err(JsonReadError {
                rule: "json_parse_error".to_string(),
                message: format!("expected json object at index {}", idx),
            });
        }
        append_extracted_row_into(value, &plans, cast_mode, "index", idx, &mut column_values)?;
    }

    build_dataframe(&plans, column_values)
}

fn read_json_array_columns(input_path: &Path) -> Result<Vec<String>, JsonReadError> {
    let content = std::fs::read_to_string(input_path).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("failed to read json at {}: {err}", input_path.display()),
    })?;
    let value: Value = serde_json::from_str(&content).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("json parse error: {err}"),
    })?;
    let array = value.as_array().ok_or_else(|| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: "expected json array at root".to_string(),
    })?;
    if array.is_empty() {
        return Err(JsonReadError {
            rule: "json_parse_error".to_string(),
            message: "json array is empty".to_string(),
        });
    }
    let object = array[0].as_object().ok_or_else(|| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: "expected json object at index 0".to_string(),
    })?;
    let mut keys = object.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

fn build_dataframe(
    plans: &[SelectorPlan],
    column_values: Vec<Vec<Option<String>>>,
) -> Result<DataFrame, JsonReadError> {
    let mut series = Vec::with_capacity(plans.len());
    for (plan, values) in plans.iter().zip(column_values) {
        series.push(Series::new(plan.source.as_str().into(), values).into());
    }

    DataFrame::new(series).map_err(|err| JsonReadError {
        rule: "json_parse_error".to_string(),
        message: format!("failed to build dataframe: {err}"),
    })
}

impl InputAdapter for JsonInputAdapter {
    fn format(&self) -> &'static str {
        "json"
    }

    fn read_input_columns(
        &self,
        entity: &config::EntityConfig,
        input_file: &InputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, FileReadError> {
        let json_mode = entity
            .source
            .options
            .as_ref()
            .and_then(|options| options.json_mode.as_deref())
            .unwrap_or("array");
        let path = &input_file.source_local_path;
        let read_result = match json_mode {
            "ndjson" => read_ndjson_columns(path),
            "array" => read_json_array_columns(path),
            other => Err(JsonReadError {
                rule: "json_parse_error".to_string(),
                message: format!(
                    "unsupported source.options.json_mode={} (allowed: array, ndjson)",
                    other
                ),
            }),
        };
        read_result.map_err(|err| FileReadError {
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
        let mut inputs = Vec::with_capacity(files.len());
        let json_mode = entity
            .source
            .options
            .as_ref()
            .and_then(|options| options.json_mode.as_deref())
            .unwrap_or("array");
        let cast_mode = entity.source.cast_mode.as_deref().unwrap_or("strict");
        for input_file in files {
            let path = &input_file.source_local_path;
            let read_result = match json_mode {
                "ndjson" => read_ndjson_file(path, columns, cast_mode),
                "array" => read_json_array_file(path, columns, cast_mode),
                other => Err(JsonReadError {
                    rule: "json_parse_error".to_string(),
                    message: format!(
                        "unsupported source.options.json_mode={} (allowed: array, ndjson)",
                        other
                    ),
                }),
            };
            match read_result {
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
