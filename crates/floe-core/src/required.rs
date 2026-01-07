use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

use crate::{ConfigError, FloeResult};
use crate::yaml_decode::{hash_get, yaml_array, yaml_number, yaml_string};

pub(crate) fn required_value<'a>(hash: &'a Hash, key: &str, ctx: &str) -> FloeResult<&'a Yaml> {
    hash_get(hash, key).ok_or_else(|| {
        Box::new(ConfigError(format!("missing required field {ctx}.{key}")))
            as Box<dyn std::error::Error + Send + Sync>
    })
}

pub(crate) fn required_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<String> {
    let value = required_value(hash, key, ctx)?;
    yaml_string(value, &format!("{ctx}.{key}"))
}

pub(crate) fn required_array<'a>(hash: &'a Hash, key: &str, ctx: &str) -> FloeResult<&'a Vec<Yaml>> {
    let value = required_value(hash, key, ctx)?;
    let list = yaml_array(value, &format!("{ctx}.{key}"))?;
    if list.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "{ctx}.{key} must contain at least one item"
        ))));
    }
    Ok(list)
}

pub(crate) fn optional_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<String>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => Ok(Some(yaml_string(value, &format!("{ctx}.{key}"))?)),
    }
}

pub(crate) fn optional_vec_string(
    hash: &Hash,
    key: &str,
    ctx: &str,
) -> FloeResult<Option<Vec<String>>> {
    let value = match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => return Ok(None),
        Some(value) => value,
    };
    let list = yaml_array(value, &format!("{ctx}.{key}"))?;
    let mut values = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let item_ctx = format!("{ctx}.{key}[{index}]");
        values.push(yaml_string(item, &item_ctx)?);
    }
    Ok(Some(values))
}

pub(crate) fn optional_bool(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<bool>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => match value {
            Yaml::Boolean(value) => Ok(Some(*value)),
            _ => Err(Box::new(ConfigError(format!(
                "expected boolean at {ctx}.{key}"
            )))),
        },
    }
}

pub(crate) fn optional_f64(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<f64>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => yaml_number(value)
            .map(Some)
            .map_err(|_| Box::new(ConfigError(format!("expected number at {ctx}.{key}"))) as _),
    }
}

pub(crate) fn optional_u64(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<u64>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => match value {
            Yaml::Integer(raw) if *raw >= 0 => Ok(Some(*raw as u64)),
            _ => Err(Box::new(ConfigError(format!(
                "expected positive integer at {ctx}.{key}"
            )))),
        },
    }
}
