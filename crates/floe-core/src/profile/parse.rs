use std::collections::HashMap;
use std::path::Path;

use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

use crate::config::yaml_decode::{
    hash_get, load_yaml, validate_known_keys, yaml_array, yaml_hash, yaml_string,
};
use crate::profile::types::{
    ProfileConfig, ProfileExecution, ProfileMetadata, ProfileRunner, ProfileValidation,
    PROFILE_API_VERSION, PROFILE_KIND,
};
use crate::{ConfigError, FloeResult};

/// Parse a profile YAML file from disk.
pub fn parse_profile(path: &Path) -> FloeResult<ProfileConfig> {
    let docs = load_yaml(path)?;
    if docs.is_empty() {
        return Err(Box::new(ConfigError("profile YAML is empty".to_string())));
    }
    if docs.len() > 1 {
        return Err(Box::new(ConfigError(
            "profile YAML contains multiple documents; expected one".to_string(),
        )));
    }
    parse_profile_doc(&docs[0])
}

/// Parse a profile from a YAML string (used in tests).
pub fn parse_profile_from_str(contents: &str) -> FloeResult<ProfileConfig> {
    use yaml_rust2::YamlLoader;
    let docs = YamlLoader::load_from_str(contents)
        .map_err(|e| Box::new(ConfigError(format!("YAML parse error: {e}"))))?;
    if docs.is_empty() {
        return Err(Box::new(ConfigError("profile YAML is empty".to_string())));
    }
    if docs.len() > 1 {
        return Err(Box::new(ConfigError(
            "profile YAML contains multiple documents; expected one".to_string(),
        )));
    }
    parse_profile_doc(&docs[0])
}

fn parse_profile_doc(doc: &Yaml) -> FloeResult<ProfileConfig> {
    let root = yaml_hash(doc, "profile")?;
    validate_known_keys(
        root,
        "profile",
        &[
            "apiVersion",
            "kind",
            "metadata",
            "execution",
            "variables",
            "validation",
        ],
    )?;

    let api_version = get_required_string(root, "apiVersion", "profile")?;
    if api_version != PROFILE_API_VERSION {
        return Err(Box::new(ConfigError(format!(
            "profile.apiVersion: expected \"{PROFILE_API_VERSION}\", got \"{api_version}\""
        ))));
    }

    let kind = get_required_string(root, "kind", "profile")?;
    if kind != PROFILE_KIND {
        return Err(Box::new(ConfigError(format!(
            "profile.kind: expected \"{PROFILE_KIND}\", got \"{kind}\""
        ))));
    }

    let metadata_yaml = hash_get(root, "metadata").ok_or_else(|| {
        Box::new(ConfigError("profile.metadata is required".to_string()))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    let metadata = parse_metadata(metadata_yaml)?;

    let execution = match hash_get(root, "execution") {
        Some(value) => Some(parse_execution(value)?),
        None => None,
    };

    let variables = match hash_get(root, "variables") {
        Some(value) => parse_variables(value)?,
        None => HashMap::new(),
    };

    let validation = match hash_get(root, "validation") {
        Some(value) => Some(parse_validation(value)?),
        None => None,
    };

    Ok(ProfileConfig {
        api_version,
        kind,
        metadata,
        execution,
        variables,
        validation,
    })
}

fn parse_metadata(value: &Yaml) -> FloeResult<ProfileMetadata> {
    let hash = yaml_hash(value, "profile.metadata")?;
    validate_known_keys(
        hash,
        "profile.metadata",
        &["name", "description", "env", "tags"],
    )?;

    let name = get_required_string(hash, "name", "profile.metadata")?;
    let description = get_optional_string(hash, "description", "profile.metadata")?;
    let env = get_optional_string(hash, "env", "profile.metadata")?;

    let tags = match hash_get(hash, "tags") {
        Some(value) => {
            let arr = yaml_array(value, "profile.metadata.tags")?;
            let mut tags = Vec::with_capacity(arr.len());
            for item in arr {
                tags.push(yaml_string(item, "profile.metadata.tags[]")?);
            }
            Some(tags)
        }
        None => None,
    };

    Ok(ProfileMetadata {
        name,
        description,
        env,
        tags,
    })
}

fn parse_execution(value: &Yaml) -> FloeResult<ProfileExecution> {
    let hash = yaml_hash(value, "profile.execution")?;
    validate_known_keys(hash, "profile.execution", &["runner"])?;

    let runner_yaml = hash_get(hash, "runner").ok_or_else(|| {
        Box::new(ConfigError(
            "profile.execution.runner is required".to_string(),
        )) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let runner = parse_runner(runner_yaml)?;

    Ok(ProfileExecution { runner })
}

fn parse_runner(value: &Yaml) -> FloeResult<ProfileRunner> {
    let hash = yaml_hash(value, "profile.execution.runner")?;
    validate_known_keys(hash, "profile.execution.runner", &["type"])?;

    let runner_type = get_required_string(hash, "type", "profile.execution.runner")?;

    Ok(ProfileRunner { runner_type })
}

fn parse_variables(value: &Yaml) -> FloeResult<HashMap<String, String>> {
    let hash = yaml_hash(value, "profile.variables")?;
    extract_string_map(hash, "profile.variables")
}

fn parse_validation(value: &Yaml) -> FloeResult<ProfileValidation> {
    let hash = yaml_hash(value, "profile.validation")?;
    validate_known_keys(hash, "profile.validation", &["strict"])?;

    let strict = match hash_get(hash, "strict") {
        Some(Yaml::Boolean(b)) => Some(*b),
        Some(_) => {
            return Err(Box::new(ConfigError(
                "profile.validation.strict must be a boolean".to_string(),
            )))
        }
        None => None,
    };

    Ok(ProfileValidation { strict })
}

fn get_required_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<String> {
    let value = hash_get(hash, key).ok_or_else(|| {
        Box::new(ConfigError(format!("{ctx}.{key} is required")))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    yaml_string(value, &format!("{ctx}.{key}"))
}

fn get_optional_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<String>> {
    match hash_get(hash, key) {
        None => Ok(None),
        Some(value) => yaml_string(value, &format!("{ctx}.{key}")).map(Some),
    }
}

fn extract_string_map(hash: &Hash, context: &str) -> FloeResult<HashMap<String, String>> {
    let mut map = HashMap::new();
    for (key, value) in hash {
        let key_str = yaml_string(key, context)?;
        let value_str = yaml_string(value, context)?;
        map.insert(key_str, value_str);
    }
    Ok(map)
}
