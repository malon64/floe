use crate::errors::FloeError;
use std::borrow::Cow;

use crate::FloeResult;

/// Remote storage URI schemes recognized across config resolution and runtime.
/// `abfss://` is the secure spelling Azure surfaces in the portal/Databricks; it is
/// accepted wherever `abfs://` is and folded to it by [`normalize_remote_uri`].
const REMOTE_URI_SCHEMES: &[&str] = &["s3://", "gs://", "gcs://", "abfs://", "abfss://", "az://"];

/// Single source of truth for "is this a remote storage URI rather than a local path".
pub fn is_remote_uri(value: &str) -> bool {
    REMOTE_URI_SCHEMES
        .iter()
        .any(|scheme| value.starts_with(scheme))
}

/// Fold the secure `abfss://` scheme to the canonical `abfs://` that Floe stores and
/// parses internally (the reverse of the `abfs://` -> `abfss://` rewrite Unity Catalog
/// registration applies). Any other value is returned borrowed and unchanged.
pub fn normalize_remote_uri(value: &str) -> Cow<'_, str> {
    match value.strip_prefix("abfss://") {
        Some(rest) => Cow::Owned(format!("abfs://{rest}")),
        None => Cow::Borrowed(value),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketLocation {
    pub bucket: String,
    pub key: String,
}

pub fn parse_bucket_uri(scheme: &str, uri: &str) -> FloeResult<BucketLocation> {
    let expected = format!("{scheme}://");
    let stripped = uri
        .strip_prefix(&expected)
        .ok_or_else(|| FloeError::config(format!("expected {} uri, got {}", scheme, uri)))?;
    let mut parts = stripped.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    if bucket.is_empty() {
        return Err(FloeError::config(format!("missing bucket in {} uri: {}", scheme, uri)).into());
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
    let normalized = normalize_remote_uri(uri);
    let stripped = normalized
        .strip_prefix("abfs://")
        .ok_or_else(|| FloeError::config(format!("expected abfs uri, got {}", uri)))?;
    let (container, rest) = stripped
        .split_once('@')
        .ok_or_else(|| FloeError::config(format!("missing container in abfs uri: {}", uri)))?;
    let (account, path) = rest
        .split_once(".dfs.core.windows.net")
        .ok_or_else(|| FloeError::config(format!("missing account in abfs uri: {}", uri)))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_remote_uri_recognizes_every_scheme() {
        for uri in [
            "s3://b/k",
            "gs://b/k",
            "gcs://b/k",
            "abfs://c@a.dfs.core.windows.net/p",
            "abfss://c@a.dfs.core.windows.net/p",
            "az://c/p",
        ] {
            assert!(is_remote_uri(uri), "{uri} should be remote");
        }
        for value in ["data/file.csv", "/abs/path", "local://x", "./rel"] {
            assert!(!is_remote_uri(value), "{value} should be local");
        }
    }

    #[test]
    fn normalize_folds_abfss_only() {
        assert_eq!(
            normalize_remote_uri("abfss://c@a.dfs.core.windows.net/p"),
            "abfs://c@a.dfs.core.windows.net/p"
        );
        for unchanged in ["abfs://c@a.dfs.core.windows.net/p", "s3://b/k", "data/x"] {
            assert!(matches!(normalize_remote_uri(unchanged), Cow::Borrowed(v) if v == unchanged));
        }
    }

    #[test]
    fn parse_abfs_uri_accepts_both_spellings() {
        let secure = parse_abfs_uri("abfss://cont@acct.dfs.core.windows.net/data/x.csv").unwrap();
        let plain = parse_abfs_uri("abfs://cont@acct.dfs.core.windows.net/data/x.csv").unwrap();
        assert_eq!(secure, plain);
        assert_eq!(secure.account, "acct");
        assert_eq!(secure.container, "cont");
        assert_eq!(secure.path, "data/x.csv");
    }
}
