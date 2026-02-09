use crate::{ConfigError, FloeResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketLocation {
    pub bucket: String,
    pub key: String,
}

pub fn parse_bucket_uri(scheme: &str, uri: &str) -> FloeResult<BucketLocation> {
    let expected = format!("{scheme}://");
    let stripped = uri.strip_prefix(&expected).ok_or_else(|| {
        Box::new(ConfigError(format!("expected {} uri, got {}", scheme, uri)))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    if bucket.is_empty() {
        return Err(Box::new(ConfigError(format!(
            "missing bucket in {} uri: {}",
            scheme, uri
        ))));
    }
    let key = parts.next().unwrap_or("").to_string();
    Ok(BucketLocation { bucket, key })
}

pub fn format_bucket_uri(scheme: &str, bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("{}://{}", scheme, bucket)
    } else {
        format!("{}://{}/{}", scheme, bucket, key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdlsLocation {
    pub account: String,
    pub container: String,
    pub path: String,
}

pub fn parse_abfs_uri(uri: &str) -> FloeResult<AdlsLocation> {
    let stripped = uri.strip_prefix("abfs://").ok_or_else(|| {
        Box::new(ConfigError(format!("expected abfs uri, got {}", uri)))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    let (container, rest) = stripped.split_once('@').ok_or_else(|| {
        Box::new(ConfigError(format!(
            "missing container in abfs uri: {}",
            uri
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let (account, path) = rest.split_once(".dfs.core.windows.net").ok_or_else(|| {
        Box::new(ConfigError(format!("missing account in abfs uri: {}", uri)))
            as Box<dyn std::error::Error + Send + Sync>
    })?;
    let path = path.trim_start_matches('/');
    Ok(AdlsLocation {
        account: account.to_string(),
        container: container.to_string(),
        path: path.to_string(),
    })
}

pub fn format_abfs_uri(container: &str, account: &str, path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        format!("abfs://{}@{}.dfs.core.windows.net", container, account)
    } else {
        format!(
            "abfs://{}@{}.dfs.core.windows.net/{}",
            container, account, trimmed
        )
    }
}
