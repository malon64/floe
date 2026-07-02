use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::config::{ConfigBase, StorageDefinition};
use crate::io::storage::{self, StorageClient};
use crate::FloeResult;

pub struct ConfigLocation {
    pub path: PathBuf,
    pub base: ConfigBase,
    pub display: String,
    /// Fully-formed, environment-independent URI recorded as `config_uri` /
    /// `profile_uri` in the manifest. The scheme is decided here — where we
    /// authoritatively know local vs remote — rather than re-derived later from a
    /// string check, so a downstream consumer never has to guess (issue #438).
    ///
    /// - Local configs: `local://<path-as-typed>`, lexically normalized but NOT
    ///   canonicalized, so a relative `-c domains/x.yml` stays relative and the same
    ///   config hashes identically under Docker (`/work/...`) and a native checkout.
    /// - Remote configs (s3://, gs://, abfs://, ...): the full, scheme-normalized
    ///   URI (e.g. `abfss://` folds to the canonical `abfs://`) — already
    ///   environment-independent, so kept absolute and never relativized.
    pub uri: String,
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
    let mut has_prefix = false;
    for comp in Path::new(input).components() {
        match comp {
            // Drop redundant "." segments; "//" collapses naturally since the
            // components iterator yields no empty segments.
            Component::CurDir => {}
            // A Windows path prefix (e.g. `C:`, or a UNC/verbatim prefix) already
            // carries the root, so record it and let the following `RootDir` be a
            // no-op; otherwise `C:` + a leading empty part joins to `C://...`, which
            // would collide with the URI scheme separator.
            Component::Prefix(prefix) => {
                has_prefix = true;
                parts.push(prefix.as_os_str().to_string_lossy().into_owned());
            }
            // A POSIX leading root ("/") becomes an empty leading part so the join
            // below re-emits the leading slash. After a Windows drive prefix it must
            // NOT add that empty part (see above).
            Component::RootDir => {
                if !has_prefix {
                    parts.push(String::new());
                }
            }
            // Keep everything else verbatim (normal segments and "..").
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

#[cfg(test)]
mod tests {
    use super::normalize_input_path;

    #[test]
    fn relative_paths_are_preserved_and_slash_separated() {
        assert_eq!(normalize_input_path("domains/x.yml"), "domains/x.yml");
        // "./" and redundant separators collapse; ".." is kept (lexical, not resolved).
        assert_eq!(normalize_input_path("./a/../b/x.yml"), "a/../b/x.yml");
        assert_eq!(normalize_input_path("."), ".");
    }

    #[test]
    fn posix_absolute_paths_keep_their_leading_slash() {
        assert_eq!(
            normalize_input_path("/work/domains/x.yml"),
            "/work/domains/x.yml"
        );
    }

    // Windows path parsing (drive prefixes) only happens on Windows targets, so this
    // regression for the `C://...` scheme collision (PR #440 review) runs there.
    #[cfg(windows)]
    #[test]
    fn windows_drive_paths_do_not_produce_a_double_slash() {
        // No `C://` (which `format!("local://{}")` would turn into an invalid,
        // scheme-colliding URI); a single slash after the drive.
        assert_eq!(
            normalize_input_path(r"C:\repo\config.yml"),
            "C:/repo/config.yml"
        );
        assert_eq!(normalize_input_path(r"repo\config.yml"), "repo/config.yml");
    }
}
