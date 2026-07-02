use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::config::{ConfigBase, StorageDefinition};
use crate::io::storage::{self, StorageClient};
use crate::FloeResult;

pub struct ConfigLocation {
    pub path: PathBuf,
    pub base: ConfigBase,
    pub display: String,
    /// URI recorded as `config_uri` / `profile_uri` in the manifest:
    /// `local://<path-as-typed>` for local configs (normalized, not canonicalized,
    /// so a relative `-c` stays relative and the manifest does not depend on where
    /// the file physically lives), or the scheme-normalized remote URI otherwise.
    pub uri: String,
    _temp_dir: Option<TempDir>,
}

pub fn resolve_config_location(input: &str) -> FloeResult<ConfigLocation> {
    if is_remote_uri(input) {
        let temp_dir = TempDir::new()?;
        let local_path = download_remote_config(input, temp_dir.path())?;
        let base = ConfigBase::remote_from_uri(temp_dir.path().to_path_buf(), input)?;
        // Remote URIs are already environment-independent; keep them absolute.
        let uri = storage::uri::normalize_remote_uri(input).into_owned();
        Ok(ConfigLocation {
            path: local_path,
            base,
            display: input.to_string(),
            uri,
            _temp_dir: Some(temp_dir),
        })
    } else {
        let path = PathBuf::from(input);
        let absolute = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };
        let canonical = std::fs::canonicalize(&absolute)?;
        let base = ConfigBase::local_from_path(&canonical);
        Ok(ConfigLocation {
            path: canonical.clone(),
            base,
            display: canonical.display().to_string(),
            uri: format!("local://{}", normalize_input_path(input)),
            _temp_dir: None,
        })
    }
}

/// Lexically normalize a local path for `config_uri` / `profile_uri` without
/// touching the filesystem, so a relative `-c` stays relative and the same path is
/// produced on any machine. Uses `/` as the separator.
fn normalize_input_path(input: &str) -> String {
    use std::path::Component;

    let mut parts: Vec<String> = Vec::new();
    let mut has_prefix = false;
    for comp in Path::new(input).components() {
        match comp {
            Component::CurDir => {}
            Component::Prefix(prefix) => {
                has_prefix = true;
                parts.push(prefix.as_os_str().to_string_lossy().into_owned());
            }
            // A Windows drive/UNC prefix already carries the root; skip the trailing
            // RootDir so we emit `C:/...`, not `C://...` (which looks like a scheme).
            Component::RootDir => {
                if !has_prefix {
                    parts.push(String::new());
                }
            }
            other => parts.push(other.as_os_str().to_string_lossy().into_owned()),
        }
    }

    let joined = parts.join("/");
    if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}

fn download_remote_config(uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
    let normalized = storage::uri::normalize_remote_uri(uri);
    let uri = normalized.as_ref();
    if uri.starts_with("s3://") {
        let location = storage::s3::parse_s3_uri(uri)?;
        let client = storage::s3::S3Client::new(location.bucket, None, None, None)?;
        return client.download_to_temp(uri, temp_dir);
    }
    if uri.starts_with("gs://") {
        let location = storage::gcs::parse_gcs_uri(uri)?;
        let client = storage::gcs::GcsClient::new(location.bucket)?;
        return client.download_to_temp(uri, temp_dir);
    }
    if uri.starts_with("abfs://") {
        let location = storage::adls::parse_adls_uri(uri)?;
        let definition = StorageDefinition {
            name: "config".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some(location.account),
            container: Some(location.container),
            prefix: None,
            endpoint: None,
            path_style_access: None,
        };
        let client = storage::adls::AdlsClient::new(&definition)?;
        return client.download_to_temp(uri, temp_dir);
    }
    Err(format!("unsupported config uri: {}", uri).into())
}

/// Write `bytes` to a remote URI by staging them in a temp file then uploading.
pub fn write_bytes_to_remote_uri(bytes: &[u8], uri: &str) -> FloeResult<()> {
    let temp_dir = TempDir::new()?;
    let local_path = temp_dir.path().join("upload");
    std::fs::write(&local_path, bytes)?;
    upload_to_remote_uri(&local_path, uri)
}

pub fn upload_to_remote_uri(local_path: &Path, uri: &str) -> FloeResult<()> {
    let normalized = storage::uri::normalize_remote_uri(uri);
    let uri = normalized.as_ref();
    if uri.starts_with("s3://") {
        let location = storage::s3::parse_s3_uri(uri)?;
        let client = storage::s3::S3Client::new(location.bucket, None, None, None)?;
        return client.upload_from_path(local_path, uri);
    }
    if uri.starts_with("gs://") {
        let location = storage::gcs::parse_gcs_uri(uri)?;
        let client = storage::gcs::GcsClient::new(location.bucket)?;
        return client.upload_from_path(local_path, uri);
    }
    if uri.starts_with("abfs://") {
        let location = storage::adls::parse_adls_uri(uri)?;
        let definition = StorageDefinition {
            name: "manifest".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some(location.account),
            container: Some(location.container),
            prefix: None,
            endpoint: None,
            path_style_access: None,
        };
        let client = storage::adls::AdlsClient::new(&definition)?;
        return client.upload_from_path(local_path, uri);
    }
    Err(format!("unsupported manifest output uri: {uri}").into())
}

pub(crate) fn is_remote_uri(value: &str) -> bool {
    crate::io::storage::uri::is_remote_uri(value)
}
