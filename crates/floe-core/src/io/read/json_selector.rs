use serde_json::Value;

use crate::ConfigError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorToken {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectorValue {
    Scalar(String),
    Null,
    NonScalar(Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorError {
    pub message: String,
}

impl std::fmt::Display for SelectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SelectorError {}

pub fn parse_selector(selector: &str) -> Result<Vec<SelectorToken>, SelectorError> {
    let trimmed = selector.trim();
    if trimmed.is_empty() {
        return Err(SelectorError {
            message: "selector is empty".to_string(),
        });
    }
    let mut tokens = Vec::new();
    let mut buf = String::new();
    let mut chars = trimmed.chars().peekable();
    let mut last_was_index = false;
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                if buf.is_empty() {
                    if last_was_index {
                        last_was_index = false;
                        continue;
                    }
                    return Err(SelectorError {
                        message: "selector contains empty field".to_string(),
                    });
                }
                tokens.push(SelectorToken::Field(buf.clone()));
                buf.clear();
                last_was_index = false;
            }
            '[' => {
                if !buf.is_empty() {
                    tokens.push(SelectorToken::Field(buf.clone()));
                    buf.clear();
                }
                let mut index_buf = String::new();
                for next in chars.by_ref() {
                    if next == ']' {
                        break;
                    }
                    index_buf.push(next);
                }
                if index_buf.is_empty() {
                    return Err(SelectorError {
                        message: "selector index is empty".to_string(),
                    });
                }
                if !index_buf.chars().all(|c| c.is_ascii_digit()) {
                    return Err(SelectorError {
                        message: format!("selector index is not numeric: {}", index_buf),
                    });
                }
                let index = index_buf.parse::<usize>().map_err(|_| SelectorError {
                    message: format!("selector index is invalid: {}", index_buf),
                })?;
                tokens.push(SelectorToken::Index(index));
                last_was_index = true;
            }
            ']' => {
                return Err(SelectorError {
                    message: "selector contains unexpected ']'".to_string(),
                });
            }
            _ => {
                buf.push(ch);
                last_was_index = false;
            }
        }
    }
    if !buf.is_empty() {
        tokens.push(SelectorToken::Field(buf));
    }
    Ok(tokens)
}

pub fn evaluate_selector(
    value: &Value,
    tokens: &[SelectorToken],
) -> Result<SelectorValue, SelectorError> {
    if tokens.is_empty() {
        return Err(SelectorError {
            message: "selector has no tokens".to_string(),
        });
    }
    let mut current = value;
    for token in tokens {
        match token {
            SelectorToken::Field(name) => {
                let object = match current.as_object() {
                    Some(object) => object,
                    None => return Ok(SelectorValue::Null),
                };
                match object.get(name) {
                    Some(next) => current = next,
                    None => return Ok(SelectorValue::Null),
                }
            }
            SelectorToken::Index(index) => {
                let array = match current.as_array() {
                    Some(array) => array,
                    None => return Ok(SelectorValue::Null),
                };
                if let Some(next) = array.get(*index) {
                    current = next;
                } else {
                    return Ok(SelectorValue::Null);
                }
            }
        }
    }
    if current.is_null() {
        return Ok(SelectorValue::Null);
    }
    if current.is_object() || current.is_array() {
        return Ok(SelectorValue::NonScalar(current.clone()));
    }
    match current {
        Value::String(value) => Ok(SelectorValue::Scalar(value.clone())),
        Value::Bool(value) => Ok(SelectorValue::Scalar(value.to_string())),
        Value::Number(value) => Ok(SelectorValue::Scalar(value.to_string())),
        Value::Null => Ok(SelectorValue::Null),
        Value::Object(_) | Value::Array(_) => Ok(SelectorValue::NonScalar(current.clone())),
    }
}

pub fn selector_is_top_level(selector: &str) -> bool {
    !selector.contains('.') && !selector.contains('[')
}

pub fn compact_json(value: &Value) -> Result<String, SelectorError> {
    serde_json::to_string(value).map_err(|err| SelectorError {
        message: format!("failed to serialize json value: {err}"),
    })
}

pub fn selector_tokens_or_error(selector: &str) -> Result<Vec<SelectorToken>, ConfigError> {
    parse_selector(selector).map_err(|err| ConfigError(err.message))
}
