use std::collections::HashMap;
use std::path::Path;

use yaml_rust2::yaml::Hash;

use crate::config::storage::resolve_local_path;
use crate::config::yaml_decode::{load_yaml, yaml_hash, yaml_string};
use crate::config::{EnvConfig, RootConfig};
use crate::{ConfigError, FloeResult};

pub fn apply_templates(config: &mut RootConfig, config_dir: &Path) -> FloeResult<()> {
    let vars = build_env_vars(config_dir, config.env.as_ref())?;
    let mut domain_lookup = HashMap::new();
    for domain in config.domains.iter_mut() {
        let resolved =
            replace_placeholders(&domain.incoming_dir, &vars, "domains.incoming_dir", None)?;
        domain.resolved_incoming_dir = Some(resolved.clone());
        if domain_lookup
            .insert(domain.name.clone(), resolved)
            .is_some()
        {
            return Err(Box::new(ConfigError(format!(
                "duplicate domain name {}",
                domain.name
            ))));
        }
    }

    if let Some(report) = config.report.as_mut() {
        report.path = replace_placeholders(&report.path, &vars, "report.path", None)?;
    }

    if let Some(storages) = config.storages.as_mut() {
        for definition in storages.definitions.iter_mut() {
            if let Some(prefix) = definition.prefix.as_mut() {
                let field = format!("storages.definitions.{}.prefix", definition.name);
                *prefix = replace_placeholders(prefix, &vars, &field, None)?;
            }
        }
    }

    for entity in config.entities.iter_mut() {
        let mut context_vars = vars.clone();
        if let Some(domain_name) = entity.domain.as_ref() {
            let incoming_dir = domain_lookup.get(domain_name).ok_or_else(|| {
                ConfigError(format!(
                    "entity.name={} references unknown domain {}",
                    entity.name, domain_name
                ))
            })?;
            context_vars.insert("domain.incoming_dir".to_string(), incoming_dir.clone());
        }

        entity.source.path = replace_placeholders(
            &entity.source.path,
            &context_vars,
            "entities.source.path",
            Some(&entity.name),
        )?;
        entity.sink.accepted.path = replace_placeholders(
            &entity.sink.accepted.path,
            &context_vars,
            "entities.sink.accepted.path",
            Some(&entity.name),
        )?;
        if let Some(rejected) = entity.sink.rejected.as_mut() {
            rejected.path = replace_placeholders(
                &rejected.path,
                &context_vars,
                "entities.sink.rejected.path",
                Some(&entity.name),
            )?;
        }
        if let Some(archived) = entity.sink.archive.as_mut() {
            archived.path = replace_placeholders(
                &archived.path,
                &context_vars,
                "entities.sink.archive.path",
                Some(&entity.name),
            )?;
        }
    }

    Ok(())
}

fn build_env_vars(
    config_dir: &Path,
    env: Option<&EnvConfig>,
) -> FloeResult<HashMap<String, String>> {
    let mut vars = HashMap::new();
    let env = match env {
        Some(env) => env,
        None => return Ok(vars),
    };
    if let Some(file) = env.file.as_ref() {
        let path = resolve_local_path(config_dir, file);
        let file_vars = load_env_file(&path)?;
        vars.extend(file_vars);
    }
    vars.extend(env.vars.clone());
    Ok(vars)
}

fn load_env_file(path: &Path) -> FloeResult<HashMap<String, String>> {
    let docs = load_yaml(path)?;
    if docs.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "env file {} is empty",
            path.display()
        ))));
    }
    let hash = yaml_hash(&docs[0], "env.file")?;
    extract_string_map(hash, "env.file")
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

fn replace_placeholders(
    value: &str,
    vars: &HashMap<String, String>,
    field: &str,
    entity: Option<&str>,
) -> FloeResult<String> {
    let mut result = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("{{") {
        result.push_str(&rest[..start]);
        rest = &rest[start + 2..];
        let end = rest.find("}}").ok_or_else(|| {
            Box::new(ConfigError(format!(
                "{}{} missing closing '}}'",
                entity_prefix(entity),
                field
            )))
        })?;
        let key = rest[..end].trim();
        if key.is_empty() {
            return Err(Box::new(ConfigError(format!(
                "{}{} empty placeholder",
                entity_prefix(entity),
                field
            ))));
        }
        let replacement = vars.get(key).ok_or_else(|| {
            Box::new(ConfigError(format!(
                "{}{} references unknown variable {}",
                entity_prefix(entity),
                field,
                key
            )))
        })?;
        result.push_str(replacement);
        rest = &rest[end + 2..];
    }
    result.push_str(rest);
    Ok(result)
}

fn entity_prefix(entity: Option<&str>) -> String {
    match entity {
        Some(name) => format!("entity.name={} ", name),
        None => String::new(),
    }
}
