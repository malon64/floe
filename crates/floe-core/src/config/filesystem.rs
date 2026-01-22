use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::config::{FilesystemDefinition, RootConfig};
use crate::{ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub struct ResolvedPath {
    pub filesystem: String,
    pub uri: String,
    pub local_path: Option<PathBuf>,
}

pub struct FilesystemResolver {
    config_dir: PathBuf,
    default_name: String,
    definitions: HashMap<String, FilesystemDefinition>,
    has_config: bool,
}

impl FilesystemResolver {
    pub fn new(config: &RootConfig, config_path: &Path) -> FloeResult<Self> {
        let config_dir = config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        if let Some(filesystems) = &config.filesystems {
            let mut definitions = HashMap::new();
            for definition in &filesystems.definitions {
                if definitions
                    .insert(definition.name.clone(), definition.clone())
                    .is_some()
                {
                    return Err(Box::new(ConfigError(format!(
                        "filesystems.definitions name={} is duplicated",
                        definition.name
                    ))));
                }
            }
            let default_name = filesystems.default.clone().ok_or_else(|| {
                Box::new(ConfigError("filesystems.default is required".to_string()))
            })?;
            if !definitions.contains_key(&default_name) {
                return Err(Box::new(ConfigError(format!(
                    "filesystems.default={} does not match any definition",
                    default_name
                ))));
            }
            Ok(Self {
                config_dir,
                default_name,
                definitions,
                has_config: true,
            })
        } else {
            Ok(Self {
                config_dir,
                default_name: "local".to_string(),
                definitions: HashMap::new(),
                has_config: false,
            })
        }
    }

    pub fn resolve_path(
        &self,
        entity_name: &str,
        field: &str,
        filesystem_name: Option<&str>,
        raw_path: &str,
    ) -> FloeResult<ResolvedPath> {
        let name = filesystem_name.unwrap_or(self.default_name.as_str());
        if !self.has_config && name != "local" {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} {field} references unknown filesystem {} (no filesystems block)",
                entity_name, name
            ))));
        }

        let definition = if self.has_config {
            self.definitions.get(name).cloned().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} {field} references unknown filesystem {}",
                    entity_name, name
                )))
            })?
        } else {
            FilesystemDefinition {
                name: "local".to_string(),
                fs_type: "local".to_string(),
                bucket: None,
                region: None,
                prefix: None,
            }
        };

        match definition.fs_type.as_str() {
            "local" => {
                let resolved = resolve_local_path(&self.config_dir, raw_path);
                Ok(ResolvedPath {
                    filesystem: name.to_string(),
                    uri: local_uri(&resolved),
                    local_path: Some(resolved),
                })
            }
            "s3" => {
                let uri = resolve_s3_uri(&definition, raw_path)?;
                Ok(ResolvedPath {
                    filesystem: name.to_string(),
                    uri,
                    local_path: None,
                })
            }
            _ => Err(Box::new(ConfigError(format!(
                "filesystem type {} is unsupported",
                definition.fs_type
            )))),
        }
    }

    pub fn definition(&self, name: &str) -> Option<FilesystemDefinition> {
        if self.has_config {
            self.definitions.get(name).cloned()
        } else if name == "local" {
            Some(FilesystemDefinition {
                name: "local".to_string(),
                fs_type: "local".to_string(),
                bucket: None,
                region: None,
                prefix: None,
            })
        } else {
            None
        }
    }
}

pub fn resolve_local_path(config_dir: &Path, raw_path: &str) -> PathBuf {
    let path = Path::new(raw_path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    }
}

fn local_uri(path: &Path) -> String {
    format!("local://{}", path.display())
}

fn resolve_s3_uri(definition: &FilesystemDefinition, raw_path: &str) -> FloeResult<String> {
    let bucket = definition.bucket.as_ref().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "filesystem {} requires bucket for type s3",
            definition.name
        )))
    })?;
    if let Some((bucket_in_path, key)) = parse_s3_uri(raw_path) {
        if bucket_in_path != *bucket {
            return Err(Box::new(ConfigError(format!(
                "filesystem {} bucket mismatch: {}",
                definition.name, bucket_in_path
            ))));
        }
        return Ok(format_s3_uri(bucket, &key));
    }

    let key = join_s3_key(definition.prefix.as_deref().unwrap_or(""), raw_path);
    Ok(format_s3_uri(bucket, &key))
}

fn parse_s3_uri(value: &str) -> Option<(String, String)> {
    let stripped = value.strip_prefix("s3://")?;
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next()?.to_string();
    if bucket.is_empty() {
        return None;
    }
    let key = parts.next().unwrap_or("").to_string();
    Some((bucket, key))
}

fn join_s3_key(prefix: &str, raw_path: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let trimmed = raw_path.trim_start_matches('/');
    match (prefix.is_empty(), trimmed.is_empty()) {
        (true, true) => String::new(),
        (true, false) => trimmed.to_string(),
        (false, true) => prefix.to_string(),
        (false, false) => format!("{}/{}", prefix, trimmed),
    }
}

fn format_s3_uri(bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("s3://{}", bucket)
    } else {
        format!("s3://{}/{}", bucket, key)
    }
}
