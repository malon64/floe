use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::config::{ConfigBase, StorageDefinition};
use crate::io::storage::{self, StorageClient};
use crate::FloeResult;

pub struct ConfigLocation {
    pub path: PathBuf,
    pub base: ConfigBase,
    pub display: String,
    _temp_dir: Option<TempDir>,
}

pub fn resolve_config_location(input: &str) -> FloeResult<ConfigLocation> {
    if is_remote_uri(input) {
        let temp_dir = TempDir::new()?;
        let local_path = download_remote_config(input, temp_dir.path())?;
        let base = ConfigBase::remote_from_uri(temp_dir.path().to_path_buf(), input)?;
        Ok(ConfigLocation {
            path: local_path,
            base,
            display: input.to_string(),
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
            _temp_dir: None,
        })
    }
}

fn download_remote_config(uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
    if uri.starts_with("s3://") {
        let location = storage::s3::parse_s3_uri(uri)?;
        let client = storage::s3::S3Client::new(location.bucket, None)?;
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
        };
        let client = storage::adls::AdlsClient::new(&definition)?;
        return client.download_to_temp(uri, temp_dir);
    }
    Err(format!("unsupported config uri: {}", uri).into())
}

fn is_remote_uri(value: &str) -> bool {
    value.starts_with("s3://") || value.starts_with("gs://") || value.starts_with("abfs://")
}
