use std::path::{Path, PathBuf};

use crate::{ConfigError, FloeResult};

use super::StorageClient;

pub struct LocalClient;

impl LocalClient {
    pub fn new() -> Self {
        Self
    }
}

impl StorageClient for LocalClient {
    fn list(&self, prefix: &str) -> FloeResult<Vec<String>> {
        let path = Path::new(prefix);
        if path.is_file() {
            return Ok(vec![path.display().to_string()]);
        }
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                files.push(path.display().to_string());
            }
        }
        files.sort();
        Ok(files)
    }

    fn download(&self, key: &str, dest: &Path) -> FloeResult<()> {
        let src = PathBuf::from(key);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&src, dest).map_err(|err| {
            Box::new(ConfigError(format!(
                "local download failed from {}: {err}",
                src.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        Ok(())
    }

    fn upload(&self, key: &str, path: &Path) -> FloeResult<()> {
        let dest = PathBuf::from(key);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(path, &dest).map_err(|err| {
            Box::new(ConfigError(format!(
                "local upload failed to {}: {err}",
                dest.display()
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        Ok(())
    }
}
