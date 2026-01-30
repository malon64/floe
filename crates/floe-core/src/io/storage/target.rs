use crate::{config, io, ConfigError, FloeResult};

#[derive(Debug, Clone)]
pub enum Target {
    Local {
        storage: String,
        uri: String,
        base_path: String,
    },
    S3 {
        storage: String,
        uri: String,
        bucket: String,
        base_key: String,
    },
    Adls {
        storage: String,
        uri: String,
        account: String,
        container: String,
        base_path: String,
    },
    Gcs {
        storage: String,
        uri: String,
        bucket: String,
        base_key: String,
    },
}

impl Target {
    pub fn from_resolved(resolved: &config::ResolvedPath) -> FloeResult<Self> {
        if let Some(path) = &resolved.local_path {
            return Ok(Target::Local {
                storage: resolved.storage.clone(),
                uri: resolved.uri.clone(),
                base_path: path.display().to_string(),
            });
        }
        if resolved.uri.starts_with("s3://") {
            let location = io::storage::s3::parse_s3_uri(&resolved.uri)?;
            return Ok(Target::S3 {
                storage: resolved.storage.clone(),
                uri: resolved.uri.clone(),
                bucket: location.bucket,
                base_key: location.key,
            });
        }
        if resolved.uri.starts_with("abfs://") {
            let location = io::storage::adls::parse_adls_uri(&resolved.uri)?;
            return Ok(Target::Adls {
                storage: resolved.storage.clone(),
                uri: resolved.uri.clone(),
                account: location.account,
                container: location.container,
                base_path: location.path,
            });
        }
        if resolved.uri.starts_with("gs://") {
            let location = io::storage::gcs::parse_gcs_uri(&resolved.uri)?;
            return Ok(Target::Gcs {
                storage: resolved.storage.clone(),
                uri: resolved.uri.clone(),
                bucket: location.bucket,
                base_key: location.key,
            });
        }
        Err(Box::new(ConfigError(format!(
            "unsupported storage uri: {}",
            resolved.uri
        ))))
    }

    pub fn storage(&self) -> &str {
        match self {
            Target::Local { storage, .. }
            | Target::S3 { storage, .. }
            | Target::Adls { storage, .. }
            | Target::Gcs { storage, .. } => storage.as_str(),
        }
    }

    pub fn target_uri(&self) -> &str {
        match self {
            Target::Local { uri, .. }
            | Target::S3 { uri, .. }
            | Target::Adls { uri, .. }
            | Target::Gcs { uri, .. } => uri.as_str(),
        }
    }

    pub fn s3_parts(&self) -> Option<(&str, &str)> {
        match self {
            Target::S3 {
                bucket, base_key, ..
            } => Some((bucket.as_str(), base_key.as_str())),
            Target::Local { .. } => None,
            Target::Adls { .. } | Target::Gcs { .. } => None,
        }
    }

    pub fn gcs_parts(&self) -> Option<(&str, &str)> {
        match self {
            Target::Gcs {
                bucket, base_key, ..
            } => Some((bucket.as_str(), base_key.as_str())),
            Target::Local { .. } | Target::S3 { .. } | Target::Adls { .. } => None,
        }
    }

    pub fn adls_parts(&self) -> Option<(&str, &str, &str)> {
        match self {
            Target::Adls {
                container,
                account,
                base_path,
                ..
            } => Some((container.as_str(), account.as_str(), base_path.as_str())),
            Target::Local { .. } | Target::S3 { .. } | Target::Gcs { .. } => None,
        }
    }
}
