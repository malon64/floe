use std::path::Path;

use yaml_rust2::yaml::Hash;
use yaml_rust2::{Yaml, YamlLoader};

use crate::{ConfigError, FloeResult};

pub(crate) fn load_yaml(path: &Path) -> FloeResult<Vec<Yaml>> {
    let contents = std::fs::read_to_string(path)?;
    let docs = YamlLoader::load_from_str(&contents)?;
    Ok(docs)
}

pub(crate) fn yaml_hash<'a>(value: &'a Yaml, ctx: &str) -> FloeResult<&'a Hash> {
    match value {
        Yaml::Hash(hash) => Ok(hash),
        _ => Err(Box::new(ConfigError(format!(
            "expected map at {ctx}"
        )))),
    }
}

pub(crate) fn yaml_array<'a>(value: &'a Yaml, ctx: &str) -> FloeResult<&'a Vec<Yaml>> {
    match value {
        Yaml::Array(values) => Ok(values),
        _ => Err(Box::new(ConfigError(format!(
            "expected array at {ctx}"
        )))),
    }
}

pub(crate) fn yaml_string(value: &Yaml, ctx: &str) -> FloeResult<String> {
    match value {
        Yaml::String(value) => Ok(value.clone()),
        _ => Err(Box::new(ConfigError(format!(
            "expected string at {ctx}"
        )))),
    }
}

pub(crate) fn yaml_number(value: &Yaml) -> Result<f64, ()> {
    match value {
        Yaml::Integer(raw) => Ok(*raw as f64),
        Yaml::Real(raw) => raw.parse::<f64>().map_err(|_| ()),
        _ => Err(()),
    }
}

pub(crate) fn hash_get<'a>(hash: &'a Hash, key: &str) -> Option<&'a Yaml> {
    hash.get(&Yaml::String(key.to_string()))
}
