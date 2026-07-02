use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::config::{ConfigBase, StorageDefinition};
use crate::io::storage::{self, StorageClient};
use crate::FloeResult;

pub struct ConfigLocation {
    pub path: PathBuf,
    pub base: ConfigBase,
    pub display: String,
    /// Environment-independent path used to build `config_uri` / `profile_uri`
    /// in the manifest. For local configs this is the user-provided path,
    /// lexically normalized but NOT canonicalized — so a relative `-c` argument
    /// stays relative and the manifest is reproducible across machines and
    /// containers (issue #438). For remote configs (s3://, gs://, abfs://, ...) it
    /// is the full, resolved (scheme-normalized) URI — remote locations are already
    /// environment-independent, so they are kept absolute, never relativized.
    pub uri_path: String,
    _temp_dir: Option<TempDir>,
}

pub fn resolve_config_location(input: &str) -> FloeResult<ConfigLocation> {
    if is_remote_uri(input) {
        let temp_dir = TempDir::new()?;
        let local_path = download_remote_config(input, temp_dir.path())?;
        let base = ConfigBase::remote_from_uri(temp_dir.path().to_path_buf(), input)?;
        // Remote configs keep the full, absolute URI (scheme-normalized so e.g.
        // `abfss://` folds to the canonical `abfs://` Floe stores internally). Only
        // local filesystem paths are relativized for reproducibility (issue #438);
        // a remote URI is the same across every environment, so it stays as-is.
        let uri_path = storage::uri::normalize_remote_uri(input).into_owned();
        Ok(ConfigLocation {
            path: local_path,
            base,
            display: input.to_string(),
            uri_path,
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
            uri_path: normalize_input_path(input),
            _temp_dir: None,
        })
    }
}

/// Lexically normalize a user-provided local config/profile path for use in the
/// manifest's `config_uri` / `profile_uri`, WITHOUT touching the filesystem.
///
/// Canonicalizing here (as `path`/`display` do, for IO and hints) would bake the
/// host-absolute path into the manifest and make `manifest_id` / `manifest_revision`
/// depend on where the file physically lives — so the same config/profile produced
/// different IDs under Docker (`/work/...`) and a native checkout (`/home/...`),
/// issue #438. Preserving the path as typed keeps a relative `-c domains/x.yml`
/// relative, so both environments hash the same URI. `/` is used as the separator
/// for cross-platform stability (a Windows `\` path would otherwise differ from the
/// same POSIX path). Absolute inputs stay absolute — reproducible only when the
/// caller passes the same absolute path, which is the documented expectation.
fn normalize_input_path(input: &str) -> String {
    use std::path::Component;

    let mut parts: Vec<String> = Vec::new();
    for comp in Path::new(input).components() {
        match comp {
            // Drop redundant "." segments; "//" collapses naturally since the
            // components iterator yields no empty segments.
            Component::CurDir => {}
            // Leading root ("/") becomes an empty leading part so the join below
            // re-emits the leading slash.
            Component::RootDir => parts.push(String::new()),
            // Keep everything else verbatim (normal segments, ".." and any
            // Windows path prefix such as "C:").
            other => parts.push(other.as_os_str().to_string_lossy().into_owned()),
        }
    }

    let joined = parts.join("/");
    if joined.is_empty() {
        // Input was "." or empty — keep an explicit relative marker.
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
