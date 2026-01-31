use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::config::{RootConfig, StorageDefinition};
use crate::{ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub struct ConfigBase {
    local_dir: PathBuf,
    remote_base: Option<RemoteConfigBase>,
}

impl ConfigBase {
    pub fn local_from_path(path: &Path) -> Self {
        let local_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Self {
            local_dir,
            remote_base: None,
        }
    }

    pub fn remote_from_uri(local_dir: PathBuf, uri: &str) -> FloeResult<Self> {
        Ok(Self {
            local_dir,
            remote_base: Some(RemoteConfigBase::from_uri(uri)?),
        })
    }

    pub fn local_dir(&self) -> &Path {
        &self.local_dir
    }

    pub fn remote_base(&self) -> Option<&RemoteConfigBase> {
        self.remote_base.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteScheme {
    S3,
    Gcs,
    Adls,
}

#[derive(Debug, Clone)]
pub struct RemoteConfigBase {
    scheme: RemoteScheme,
    bucket: String,
    account: Option<String>,
    container: Option<String>,
    prefix: String,
}

impl RemoteConfigBase {
    fn from_uri(uri: &str) -> FloeResult<Self> {
        if let Some((bucket, key)) = parse_s3_uri(uri) {
            let prefix = parent_prefix(&key);
            return Ok(Self {
                scheme: RemoteScheme::S3,
                bucket,
                account: None,
                container: None,
                prefix,
            });
        }
        if let Some((bucket, key)) = parse_gcs_uri(uri) {
            let prefix = parent_prefix(&key);
            return Ok(Self {
                scheme: RemoteScheme::Gcs,
                bucket,
                account: None,
                container: None,
                prefix,
            });
        }
        if let Some((container, account, path)) = parse_adls_uri(uri) {
            let prefix = parent_prefix(&path);
            return Ok(Self {
                scheme: RemoteScheme::Adls,
                bucket: String::new(),
                account: Some(account),
                container: Some(container),
                prefix,
            });
        }
        Err(Box::new(ConfigError(format!(
            "unsupported config uri: {}",
            uri
        ))))
    }

    fn matches_storage(&self, storage_type: &str) -> bool {
        matches!(
            (self.scheme, storage_type),
            (RemoteScheme::S3, "s3") | (RemoteScheme::Gcs, "gcs") | (RemoteScheme::Adls, "adls")
        )
    }

    fn join(&self, relative: &str) -> String {
        match self.scheme {
            RemoteScheme::S3 => {
                let key = join_s3_key(&self.prefix, relative);
                format_s3_uri(&self.bucket, &key)
            }
            RemoteScheme::Gcs => {
                let key = join_s3_key(&self.prefix, relative);
                format_gcs_uri(&self.bucket, &key)
            }
            RemoteScheme::Adls => {
                let combined = join_adls_path(&self.prefix, relative);
                let container = self.container.as_ref().expect("container");
                let account = self.account.as_ref().expect("account");
                format_adls_uri(container, account, &combined)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedPath {
    pub storage: String,
    pub uri: String,
    pub local_path: Option<PathBuf>,
}

pub struct StorageResolver {
    config_base: ConfigBase,
    default_name: String,
    definitions: HashMap<String, StorageDefinition>,
    has_config: bool,
}

impl StorageResolver {
    pub fn new(config: &RootConfig, config_base: ConfigBase) -> FloeResult<Self> {
        if let Some(storages) = &config.storages {
            let mut definitions = HashMap::new();
            for definition in &storages.definitions {
                if definitions
                    .insert(definition.name.clone(), definition.clone())
                    .is_some()
                {
                    return Err(Box::new(ConfigError(format!(
                        "storages.definitions name={} is duplicated",
                        definition.name
                    ))));
                }
            }
            let default_name = storages
                .default
                .clone()
                .ok_or_else(|| Box::new(ConfigError("storages.default is required".to_string())))?;
            if !definitions.contains_key(&default_name) {
                return Err(Box::new(ConfigError(format!(
                    "storages.default={} does not match any definition",
                    default_name
                ))));
            }
            Ok(Self {
                config_base,
                default_name,
                definitions,
                has_config: true,
            })
        } else {
            Ok(Self {
                config_base,
                default_name: "local".to_string(),
                definitions: HashMap::new(),
                has_config: false,
            })
        }
    }

    pub fn from_path(config: &RootConfig, config_path: &Path) -> FloeResult<Self> {
        Self::new(config, ConfigBase::local_from_path(config_path))
    }

    pub fn resolve_path(
        &self,
        entity_name: &str,
        field: &str,
        storage_name: Option<&str>,
        raw_path: &str,
    ) -> FloeResult<ResolvedPath> {
        let name = storage_name.unwrap_or(self.default_name.as_str());
        if !self.has_config && name != "local" {
            return Err(Box::new(ConfigError(format!(
                "entity.name={} {field} references unknown storage {} (no storages block)",
                entity_name, name
            ))));
        }

        let definition = if self.has_config {
            self.definitions.get(name).cloned().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "entity.name={} {field} references unknown storage {}",
                    entity_name, name
                )))
            })?
        } else {
            StorageDefinition {
                name: "local".to_string(),
                fs_type: "local".to_string(),
                bucket: None,
                region: None,
                account: None,
                container: None,
                prefix: None,
            }
        };

        let resolved_remote = self
            .resolve_remote_relative(&definition, raw_path)
            .unwrap_or_else(|| raw_path.to_string());
        let raw_path = resolved_remote.as_str();

        match definition.fs_type.as_str() {
            "local" => {
                if is_remote_uri(raw_path) {
                    return Err(Box::new(ConfigError(format!(
                        "entity.name={} {field} must be a local path (got {})",
                        entity_name, raw_path
                    ))));
                }
                if self.config_base.remote_base().is_some() && Path::new(raw_path).is_relative() {
                    return Err(Box::new(ConfigError(format!(
                        "entity.name={} {field} must be absolute when config is remote",
                        entity_name
                    ))));
                }
                let resolved = resolve_local_path(self.config_base.local_dir(), raw_path);
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri: local_uri(&resolved),
                    local_path: Some(resolved),
                })
            }
            "s3" => {
                let uri = resolve_s3_uri(&definition, raw_path)?;
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri,
                    local_path: None,
                })
            }
            "adls" => {
                let uri = resolve_adls_uri(&definition, raw_path)?;
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri,
                    local_path: None,
                })
            }
            "gcs" => {
                let uri = resolve_gcs_uri(&definition, raw_path)?;
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri,
                    local_path: None,
                })
            }
            _ => Err(Box::new(ConfigError(format!(
                "storage type {} is unsupported",
                definition.fs_type
            )))),
        }
    }

    pub fn resolve_report_path(
        &self,
        storage_name: Option<&str>,
        raw_path: &str,
    ) -> FloeResult<ResolvedPath> {
        let name = storage_name.unwrap_or(self.default_name.as_str());
        if !self.has_config && name != "local" {
            return Err(Box::new(ConfigError(format!(
                "report.storage references unknown storage {} (no storages block)",
                name
            ))));
        }

        let definition = if self.has_config {
            self.definitions.get(name).cloned().ok_or_else(|| {
                Box::new(ConfigError(format!(
                    "report.storage references unknown storage {}",
                    name
                )))
            })?
        } else {
            StorageDefinition {
                name: "local".to_string(),
                fs_type: "local".to_string(),
                bucket: None,
                region: None,
                account: None,
                container: None,
                prefix: None,
            }
        };

        let resolved_remote = self
            .resolve_remote_relative(&definition, raw_path)
            .unwrap_or_else(|| raw_path.to_string());
        let raw_path = resolved_remote.as_str();

        match definition.fs_type.as_str() {
            "local" => {
                if is_remote_uri(raw_path) {
                    return Err(Box::new(ConfigError(format!(
                        "report.path must be a local path (got {})",
                        raw_path
                    ))));
                }
                if self.config_base.remote_base().is_some() && Path::new(raw_path).is_relative() {
                    return Err(Box::new(ConfigError(
                        "report.path must be absolute when config is remote".to_string(),
                    )));
                }
                let resolved = resolve_local_path(self.config_base.local_dir(), raw_path);
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri: local_uri(&resolved),
                    local_path: Some(resolved),
                })
            }
            "s3" => {
                let uri = resolve_s3_uri(&definition, raw_path)?;
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri,
                    local_path: None,
                })
            }
            "adls" => {
                let uri = resolve_adls_uri(&definition, raw_path)?;
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri,
                    local_path: None,
                })
            }
            "gcs" => {
                let uri = resolve_gcs_uri(&definition, raw_path)?;
                Ok(ResolvedPath {
                    storage: name.to_string(),
                    uri,
                    local_path: None,
                })
            }
            _ => Err(Box::new(ConfigError(format!(
                "storage type {} is unsupported",
                definition.fs_type
            )))),
        }
    }

    pub fn definition(&self, name: &str) -> Option<StorageDefinition> {
        if self.has_config {
            self.definitions.get(name).cloned()
        } else if name == "local" {
            Some(StorageDefinition {
                name: "local".to_string(),
                fs_type: "local".to_string(),
                bucket: None,
                region: None,
                account: None,
                container: None,
                prefix: None,
            })
        } else {
            None
        }
    }

    pub fn default_storage_name(&self) -> &str {
        self.default_name.as_str()
    }

    pub fn config_dir(&self) -> &Path {
        self.config_base.local_dir()
    }

    fn resolve_remote_relative(
        &self,
        definition: &StorageDefinition,
        raw_path: &str,
    ) -> Option<String> {
        if !is_relative_path(raw_path) {
            return None;
        }
        if definition.prefix.is_some() {
            return None;
        }
        let remote = self.config_base.remote_base()?;
        if !remote.matches_storage(definition.fs_type.as_str()) {
            return None;
        }
        Some(remote.join(raw_path))
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

fn resolve_s3_uri(definition: &StorageDefinition, raw_path: &str) -> FloeResult<String> {
    let bucket = definition.bucket.as_ref().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "storage {} requires bucket for type s3",
            definition.name
        )))
    })?;
    if let Some((bucket_in_path, key)) = parse_s3_uri(raw_path) {
        if bucket_in_path != *bucket {
            return Err(Box::new(ConfigError(format!(
                "storage {} bucket mismatch: {}",
                definition.name, bucket_in_path
            ))));
        }
        return Ok(format_s3_uri(bucket, &key));
    }

    let key = join_s3_key(definition.prefix.as_deref().unwrap_or(""), raw_path);
    Ok(format_s3_uri(bucket, &key))
}

fn resolve_adls_uri(definition: &StorageDefinition, raw_path: &str) -> FloeResult<String> {
    let account = definition.account.as_ref().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "storage {} requires account for type adls",
            definition.name
        )))
    })?;
    let container = definition.container.as_ref().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "storage {} requires container for type adls",
            definition.name
        )))
    })?;
    if let Some((container_in_path, account_in_path, path)) = parse_adls_uri(raw_path) {
        if container_in_path != *container || account_in_path != *account {
            return Err(Box::new(ConfigError(format!(
                "storage {} adls account/container mismatch",
                definition.name
            ))));
        }
        return Ok(format_adls_uri(container, account, &path));
    }
    let prefix = definition.prefix.as_deref().unwrap_or("");
    let combined = join_adls_path(prefix, raw_path);
    Ok(format_adls_uri(container, account, &combined))
}

fn join_adls_path(prefix: &str, raw_path: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let trimmed = raw_path.trim_start_matches('/');
    match (prefix.is_empty(), trimmed.is_empty()) {
        (true, true) => String::new(),
        (true, false) => trimmed.to_string(),
        (false, true) => prefix.to_string(),
        (false, false) => format!("{}/{}", prefix, trimmed),
    }
}

fn format_adls_uri(container: &str, account: &str, path: &str) -> String {
    if path.is_empty() {
        format!("abfs://{}@{}.dfs.core.windows.net", container, account)
    } else {
        format!(
            "abfs://{}@{}.dfs.core.windows.net/{}",
            container, account, path
        )
    }
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

fn resolve_gcs_uri(definition: &StorageDefinition, raw_path: &str) -> FloeResult<String> {
    let bucket = definition.bucket.as_ref().ok_or_else(|| {
        Box::new(ConfigError(format!(
            "storage {} requires bucket for type gcs",
            definition.name
        )))
    })?;
    if let Some((bucket_in_path, key)) = parse_gcs_uri(raw_path) {
        if bucket_in_path != *bucket {
            return Err(Box::new(ConfigError(format!(
                "storage {} bucket mismatch: {}",
                definition.name, bucket_in_path
            ))));
        }
        return Ok(format_gcs_uri(bucket, &key));
    }

    let key = join_gcs_key(definition.prefix.as_deref().unwrap_or(""), raw_path);
    Ok(format_gcs_uri(bucket, &key))
}

fn parse_gcs_uri(value: &str) -> Option<(String, String)> {
    let stripped = value.strip_prefix("gs://")?;
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next()?.to_string();
    if bucket.is_empty() {
        return None;
    }
    let key = parts.next().unwrap_or("").to_string();
    Some((bucket, key))
}

fn join_gcs_key(prefix: &str, raw_path: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let trimmed = raw_path.trim_start_matches('/');
    match (prefix.is_empty(), trimmed.is_empty()) {
        (true, true) => String::new(),
        (true, false) => trimmed.to_string(),
        (false, true) => prefix.to_string(),
        (false, false) => format!("{}/{}", prefix, trimmed),
    }
}

fn format_gcs_uri(bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("gs://{}", bucket)
    } else {
        format!("gs://{}/{}", bucket, key)
    }
}

fn parse_adls_uri(value: &str) -> Option<(String, String, String)> {
    let stripped = value.strip_prefix("abfs://")?;
    let (container, rest) = stripped.split_once('@')?;
    let (account, path) = rest.split_once(".dfs.core.windows.net")?;
    let path = path.trim_start_matches('/').to_string();
    if container.is_empty() || account.is_empty() {
        return None;
    }
    Some((container.to_string(), account.to_string(), path))
}

fn parent_prefix(key: &str) -> String {
    match key.rsplit_once('/') {
        Some((parent, _)) => parent.to_string(),
        None => String::new(),
    }
}

fn is_remote_uri(value: &str) -> bool {
    value.starts_with("s3://") || value.starts_with("gs://") || value.starts_with("abfs://")
}

fn is_relative_path(value: &str) -> bool {
    !is_remote_uri(value) && Path::new(value).is_relative()
}
